//! Explicit pin management around Moka without changing Moka's entry key.
//!
//! The first pin removes one entry from Moka. The final unpin inserts the same
//! generation again. A pin alias can be reserved before its entry is inserted.

use moka::notification::RemovalCause;
use moka::ops::compute::{CompResult, Op};
use moka::sync::Cache;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, Weak};

type EvictionListener<Key, Value> = dyn Fn(Arc<Key>, Value, RemovalCause) + Send + Sync + 'static;
type Weigher<Key, Value> = dyn Fn(&Key, &Value) -> u32 + Send + Sync + 'static;

/// Builder for [`PinAwareMoka`].
pub struct PinAwareMokaBuilder<Key, PinAlias, Value> {
    max_capacity: u64,
    weigher: Arc<Weigher<Key, Value>>,
    eviction_listener: Option<Arc<EvictionListener<Key, Value>>>,
    _pin_alias: std::marker::PhantomData<PinAlias>,
}

impl<Key, PinAlias, Value> PinAwareMokaBuilder<Key, PinAlias, Value> {
    fn new(max_capacity: u64) -> Self {
        Self {
            max_capacity,
            weigher: Arc::new(|_, _| 1),
            eviction_listener: None,
            _pin_alias: std::marker::PhantomData,
        }
    }

    pub fn weigher(
        mut self,
        weigher: impl Fn(&Key, &Value) -> u32 + Send + Sync + 'static,
    ) -> Self {
        self.weigher = Arc::new(weigher);
        self
    }

    /// Set the listener for Moka removals.
    ///
    /// Pin/unpin removals are internal and are not sent to this listener.
    pub fn eviction_listener(
        mut self,
        listener: impl Fn(Arc<Key>, Value, RemovalCause) + Send + Sync + 'static,
    ) -> Self {
        self.eviction_listener = Some(Arc::new(listener));
        self
    }
}

impl<Key, PinAlias, Value> PinAwareMokaBuilder<Key, PinAlias, Value>
where
    Key: Clone + Eq + Hash + Send + Sync + 'static,
    PinAlias: Clone + Eq + Hash + Send + Sync + 'static,
    Value: Clone + Send + Sync + 'static,
{
    pub fn build(self) -> PinAwareMoka<Key, PinAlias, Value> {
        assert!(
            self.max_capacity > 0,
            "pin-aware Moka capacity must be positive"
        );
        let weigher = self.weigher;
        let eviction_listener = self.eviction_listener;
        let inner = Arc::new_cyclic(|weak: &Weak<Inner<Key, PinAlias, Value>>| {
            let listener_inner = weak.clone();
            let cache = Cache::builder()
                .max_capacity(self.max_capacity)
                .weigher(move |_key: &Key, entry: &MokaEntry<PinAlias, Value>| entry.weight)
                .eviction_listener(move |key, entry, cause| {
                    if let Some(inner) = listener_inner.upgrade() {
                        inner.handle_moka_removal(key, entry, cause);
                    }
                })
                .build();
            Inner {
                cache,
                state: Mutex::new(State::default()),
                mutation: Mutex::new(()),
                weigher,
                eviction_listener,
            }
        });
        PinAwareMoka { inner }
    }
}

/// A key-preserving Moka wrapper with explicit pin aliases.
pub struct PinAwareMoka<Key, PinAlias, Value> {
    inner: Arc<Inner<Key, PinAlias, Value>>,
}

impl<Key, PinAlias, Value> Clone for PinAwareMoka<Key, PinAlias, Value> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Clone)]
struct MokaEntry<PinAlias, Value> {
    generation: u64,
    value: Value,
    weight: u32,
    _pin_alias: std::marker::PhantomData<PinAlias>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Residency {
    InMoka,
    Pinned,
    Selected,
}

struct EntryState<PinAlias, Value> {
    generation: u64,
    pin_aliases: Arc<[PinAlias]>,
    value: Value,
    weight: u32,
    pin_count: usize,
    residency: Residency,
}

enum PinLease<Key, PinAlias> {
    Pending(PinAlias),
    Assigned { key: Key, generation: u64 },
}

struct State<Key, PinAlias, Value> {
    next_generation: u64,
    next_pin_lease: u64,
    entries: HashMap<Key, EntryState<PinAlias, Value>>,
    aliases: HashMap<PinAlias, (Key, u64)>,
    pin_leases: HashMap<u64, PinLease<Key, PinAlias>>,
    pending_pins: HashMap<PinAlias, HashSet<u64>>,
}

impl<Key, PinAlias, Value> Default for State<Key, PinAlias, Value> {
    fn default() -> Self {
        Self {
            next_generation: 1,
            next_pin_lease: 1,
            entries: HashMap::new(),
            aliases: HashMap::new(),
            pin_leases: HashMap::new(),
            pending_pins: HashMap::new(),
        }
    }
}

struct Inner<Key, PinAlias, Value> {
    cache: Cache<Key, MokaEntry<PinAlias, Value>>,
    state: Mutex<State<Key, PinAlias, Value>>,
    // State transitions and Moka mutations are serialized, but the state lock
    // is always released before calling Moka and its synchronous listener.
    mutation: Mutex<()>,
    weigher: Arc<Weigher<Key, Value>>,
    eviction_listener: Option<Arc<EvictionListener<Key, Value>>>,
}

impl<Key, PinAlias, Value> Inner<Key, PinAlias, Value>
where
    Key: Clone + Eq + Hash + Send + Sync + 'static,
    PinAlias: Clone + Eq + Hash + Send + Sync + 'static,
    Value: Clone + Send + Sync + 'static,
{
    fn remove_moka_generation(&self, key: &Key, generation: u64) -> bool {
        matches!(
            self.cache.entry(key.clone()).and_compute_with(|current| {
                if current
                    .as_ref()
                    .is_some_and(|entry| entry.value().generation == generation)
                {
                    Op::Remove
                } else {
                    Op::Nop
                }
            }),
            CompResult::Removed(_)
        )
    }

    fn handle_moka_removal(
        &self,
        key: Arc<Key>,
        entry: MokaEntry<PinAlias, Value>,
        cause: RemovalCause,
    ) {
        if cause != RemovalCause::Size {
            return;
        }
        let selected = {
            let mut state = self.state.lock();
            let Some(current) = state.entries.get_mut(key.as_ref()) else {
                return;
            };
            if current.generation != entry.generation || current.residency != Residency::InMoka {
                return;
            }
            debug_assert_eq!(current.pin_count, 0);
            current.residency = Residency::Selected;
            true
        };
        if selected && let Some(listener) = self.eviction_listener.as_ref() {
            listener(key, entry.value, cause);
        }
    }

    fn release_pin(self: &Arc<Self>, lease_id: u64) {
        let _mutation = self.mutation.lock();
        let entry_to_admit = {
            let mut state = self.state.lock();
            let Some(lease) = state.pin_leases.remove(&lease_id) else {
                return;
            };
            match lease {
                PinLease::Pending(alias) => {
                    if let Some(leases) = state.pending_pins.get_mut(&alias) {
                        leases.remove(&lease_id);
                        if leases.is_empty() {
                            state.pending_pins.remove(&alias);
                        }
                    }
                    None
                }
                PinLease::Assigned { key, generation } => {
                    let Some(entry) = state.entries.get_mut(&key) else {
                        return;
                    };
                    if entry.generation != generation {
                        return;
                    }
                    entry.pin_count = entry
                        .pin_count
                        .checked_sub(1)
                        .expect("pin-aware Moka pin count underflow");
                    if entry.pin_count == 0 && entry.residency == Residency::Pinned {
                        entry.residency = Residency::InMoka;
                        Some((
                            key,
                            MokaEntry {
                                generation,
                                value: entry.value.clone(),
                                weight: entry.weight,
                                _pin_alias: std::marker::PhantomData,
                            },
                        ))
                    } else {
                        None
                    }
                }
            }
        };
        if let Some((key, entry)) = entry_to_admit {
            self.cache.insert(key, entry);
        }
    }
}

impl<Key, PinAlias, Value> PinAwareMoka<Key, PinAlias, Value>
where
    Key: Clone + Eq + Hash + Send + Sync + 'static,
    PinAlias: Clone + Eq + Hash + Send + Sync + 'static,
    Value: Clone + Send + Sync + 'static,
{
    pub fn builder(max_capacity: u64) -> PinAwareMokaBuilder<Key, PinAlias, Value> {
        PinAwareMokaBuilder::new(max_capacity)
    }

    /// Insert one normal Moka entry with one or more explicit pin identities.
    pub fn insert(
        &self,
        key: Key,
        pin_aliases: impl IntoIterator<Item = PinAlias>,
        value: Value,
    ) -> u64 {
        let pin_aliases = pin_aliases.into_iter().collect::<Vec<_>>();
        assert!(
            !pin_aliases.is_empty(),
            "a pin-aware Moka entry must have at least one pin alias"
        );
        assert_eq!(
            pin_aliases.iter().collect::<HashSet<_>>().len(),
            pin_aliases.len(),
            "a pin-aware Moka entry cannot contain duplicate pin aliases"
        );
        let weight = (self.inner.weigher)(&key, &value);
        let pin_aliases: Arc<[PinAlias]> = pin_aliases.into();
        let _mutation = self.inner.mutation.lock();
        let (generation, admit, displaced) = {
            let mut state = self.inner.state.lock();
            let generation = state.next_generation;
            state.next_generation = state
                .next_generation
                .checked_add(1)
                .expect("pin-aware Moka generation space exhausted");

            let mut displaced_keys = HashSet::new();
            if state.entries.contains_key(&key) {
                displaced_keys.insert(key.clone());
            }
            for alias in pin_aliases.iter() {
                if let Some((old_key, _)) = state.aliases.get(alias) {
                    displaced_keys.insert(old_key.clone());
                }
            }
            let mut displaced = Vec::new();
            for old_key in displaced_keys {
                if let Some(old) = state.entries.remove(&old_key) {
                    for alias in old.pin_aliases.iter() {
                        if state.aliases.get(alias) == Some(&(old_key.clone(), old.generation)) {
                            state.aliases.remove(alias);
                        }
                    }
                    displaced.push((old_key, old.generation));
                }
            }

            let mut pin_count = 0usize;
            for alias in pin_aliases.iter() {
                let pending = state.pending_pins.remove(alias).unwrap_or_default();
                pin_count = pin_count
                    .checked_add(pending.len())
                    .expect("pin-aware Moka pin count overflow");
                for lease_id in pending {
                    *state
                        .pin_leases
                        .get_mut(&lease_id)
                        .expect("pending pin lease must exist") = PinLease::Assigned {
                        key: key.clone(),
                        generation,
                    };
                }
                state
                    .aliases
                    .insert(alias.clone(), (key.clone(), generation));
            }
            let residency = if pin_count == 0 {
                Residency::InMoka
            } else {
                Residency::Pinned
            };
            state.entries.insert(
                key.clone(),
                EntryState {
                    generation,
                    pin_aliases: pin_aliases.clone(),
                    value: value.clone(),
                    weight,
                    pin_count,
                    residency,
                },
            );
            let admit = (residency == Residency::InMoka).then(|| MokaEntry {
                generation,
                value,
                weight,
                _pin_alias: std::marker::PhantomData,
            });
            (generation, admit, displaced)
        };
        for (old_key, old_generation) in displaced {
            self.inner.remove_moka_generation(&old_key, old_generation);
        }
        if let Some(entry) = admit {
            self.inner.cache.insert(key, entry);
        }
        generation
    }

    /// Pin the entry identified by `alias`, or reserve a pin before insertion.
    ///
    /// Returns `None` when Moka selection already owns that entry.
    pub fn try_pin_alias(&self, alias: PinAlias) -> Option<PinGuard> {
        self.try_pin_alias_if(alias, |_| true)
    }

    /// Pin only if an existing value satisfies `predicate`.
    pub fn try_pin_alias_if(
        &self,
        alias: PinAlias,
        predicate: impl FnOnce(&Value) -> bool,
    ) -> Option<PinGuard> {
        let _mutation = self.inner.mutation.lock();
        let mut state = self.inner.state.lock();
        let lease_id = state.next_pin_lease;
        state.next_pin_lease = state
            .next_pin_lease
            .checked_add(1)
            .expect("pin-aware Moka pin lease space exhausted");
        let mut remove = None;
        if let Some((key, generation)) = state.aliases.get(&alias).cloned() {
            let entry = state
                .entries
                .get_mut(&key)
                .expect("pin alias must reference a live entry");
            assert_eq!(entry.generation, generation);
            if !predicate(&entry.value) || entry.residency == Residency::Selected {
                return None;
            }
            entry.pin_count = entry
                .pin_count
                .checked_add(1)
                .expect("pin-aware Moka pin count overflow");
            if entry.residency == Residency::InMoka {
                entry.residency = Residency::Pinned;
                remove = Some((key.clone(), generation));
            }
            state
                .pin_leases
                .insert(lease_id, PinLease::Assigned { key, generation });
        } else {
            state
                .pending_pins
                .entry(alias.clone())
                .or_default()
                .insert(lease_id);
            state.pin_leases.insert(lease_id, PinLease::Pending(alias));
        }
        drop(state);
        if let Some((key, generation)) = remove {
            self.inner.remove_moka_generation(&key, generation);
        }
        Some(PinGuard {
            _lease: Arc::new(PinGuardLease {
                release: Box::new({
                    let inner = Arc::downgrade(&self.inner);
                    move || {
                        if let Some(inner) = inner.upgrade() {
                            inner.release_pin(lease_id);
                        }
                    }
                }),
            }),
        })
    }

    /// Read and touch an unselected entry. Pinned entries remain readable here.
    pub fn get(&self, key: &Key) -> Option<Value> {
        let _mutation = self.inner.mutation.lock();
        let (generation, value, in_moka) = {
            let state = self.inner.state.lock();
            let entry = state.entries.get(key)?;
            if entry.residency == Residency::Selected {
                return None;
            }
            (
                entry.generation,
                entry.value.clone(),
                entry.residency == Residency::InMoka,
            )
        };
        if in_moka {
            let _ = self
                .inner
                .cache
                .get(key)
                .filter(|entry| entry.generation == generation);
        }
        Some(value)
    }

    pub fn contains_key(&self, key: &Key) -> bool {
        self.inner
            .state
            .lock()
            .entries
            .get(key)
            .is_some_and(|entry| entry.residency != Residency::Selected)
    }

    /// Remove an entry generation from both the wrapper and Moka.
    pub fn invalidate(&self, key: &Key) -> bool {
        self.invalidate_if(key, |_| true)
    }

    pub fn invalidate_if(&self, key: &Key, predicate: impl FnOnce(&Value) -> bool) -> bool {
        self.take_if(key, predicate).is_some()
    }

    pub fn take_if(&self, key: &Key, predicate: impl FnOnce(&Value) -> bool) -> Option<Value> {
        let _mutation = self.inner.mutation.lock();
        let removed = {
            let mut state = self.inner.state.lock();
            let Some(entry) = state.entries.get(key) else {
                return None;
            };
            if !predicate(&entry.value) {
                return None;
            }
            let entry = state
                .entries
                .remove(key)
                .expect("validated pin-aware entry disappeared under lock");
            for alias in entry.pin_aliases.iter() {
                if state.aliases.get(alias) == Some(&(key.clone(), entry.generation)) {
                    state.aliases.remove(alias);
                }
            }
            entry
        };
        self.inner.remove_moka_generation(key, removed.generation);
        Some(removed.value)
    }

    /// Pop LRU entries until at least `weight_to_evict` weight is selected.
    pub fn evict_some(&self, weight_to_evict: u64) -> u64 {
        let _mutation = self.inner.mutation.lock();
        self.inner.cache.evict_some(weight_to_evict)
    }

    pub fn run_pending_tasks(&self) {
        let _mutation = self.inner.mutation.lock();
        self.inner.cache.run_pending_tasks();
    }

    pub fn set_max_capacity(&self, capacity: u64) -> Result<(), moka::CapacityError> {
        self.inner.cache.set_max_capacity(capacity)
    }

    pub fn max_capacity(&self) -> Option<u64> {
        self.inner.cache.policy().max_capacity()
    }

    pub fn weighted_size(&self) -> u64 {
        self.inner.cache.weighted_size()
    }

    pub fn entry_count(&self) -> u64 {
        self.inner.cache.entry_count()
    }
}

/// A cloneable pin. The final clone performs the unpin transition.
#[derive(Clone)]
pub struct PinGuard {
    _lease: Arc<PinGuardLease>,
}

struct PinGuardLease {
    release: Box<dyn Fn() + Send + Sync + 'static>,
}

impl Drop for PinGuardLease {
    fn drop(&mut self) {
        (self.release)();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    type TestCache = PinAwareMoka<String, String, u32>;

    fn cache(capacity: u64, evicted: Arc<Mutex<Vec<String>>>) -> TestCache {
        TestCache::builder(capacity)
            .weigher(|_, weight| *weight)
            .eviction_listener(move |key, _, cause| {
                assert_eq!(cause, RemovalCause::Size);
                evicted.lock().push((*key).clone());
            })
            .build()
    }

    fn insert(cache: &TestCache, key: &str, alias: &str, weight: u32) {
        cache.insert(key.to_string(), [alias.to_string()], weight);
        cache.run_pending_tasks();
    }

    #[test]
    fn first_pin_removes_and_final_unpin_readmits_one_key() {
        let cache = cache(100, Arc::new(Mutex::new(Vec::new())));
        insert(&cache, "key", "alias", 10);
        let first = cache.try_pin_alias("alias".to_string()).unwrap();
        let second = cache.try_pin_alias("alias".to_string()).unwrap();
        cache.run_pending_tasks();
        assert_eq!(cache.entry_count(), 0);
        assert_eq!(cache.get(&"key".to_string()), Some(10));

        drop(first);
        cache.run_pending_tasks();
        assert_eq!(cache.entry_count(), 0);
        drop(second);
        cache.run_pending_tasks();
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn pin_before_insert_defers_moka_admission() {
        let cache = cache(100, Arc::new(Mutex::new(Vec::new())));
        let pin = cache.try_pin_alias("alias".to_string()).unwrap();
        insert(&cache, "key", "alias", 10);
        assert_eq!(cache.entry_count(), 0);
        drop(pin);
        cache.run_pending_tasks();
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn pin_and_evict_race_has_one_winner() {
        for iteration in 0..100 {
            let evicted = Arc::new(Mutex::new(Vec::new()));
            let cache = cache(100, evicted.clone());
            insert(&cache, "key", "alias", 10);
            let barrier = Arc::new(Barrier::new(3));
            let pin_cache = cache.clone();
            let pin_barrier = barrier.clone();
            let pin = thread::spawn(move || {
                pin_barrier.wait();
                pin_cache.try_pin_alias("alias".to_string())
            });
            let pop_cache = cache.clone();
            let pop_barrier = barrier.clone();
            let pop = thread::spawn(move || {
                pop_barrier.wait();
                pop_cache.evict_some(1)
            });
            barrier.wait();
            let pin = pin.join().unwrap();
            let selected_weight = pop.join().unwrap();
            assert_ne!(pin.is_some(), selected_weight != 0, "iteration {iteration}");
            assert_eq!(evicted.lock().len(), usize::from(selected_weight != 0));
            drop(pin);
        }
    }

    #[test]
    fn stale_guard_does_not_readmit_a_new_generation() {
        let cache = cache(100, Arc::new(Mutex::new(Vec::new())));
        let stale = cache.try_pin_alias("old-alias".to_string()).unwrap();
        insert(&cache, "key", "old-alias", 10);
        insert(&cache, "key", "new-alias", 20);
        drop(stale);
        cache.run_pending_tasks();
        assert_eq!(cache.get(&"key".to_string()), Some(20));
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn selected_entry_rejects_pin_until_restore_or_invalidate() {
        let evicted = Arc::new(Mutex::new(Vec::new()));
        let cache = cache(100, evicted.clone());
        insert(&cache, "key", "alias", 10);
        assert_eq!(cache.evict_some(1), 10);
        assert_eq!(&*evicted.lock(), &["key"]);
        assert!(cache.try_pin_alias("alias".to_string()).is_none());

        insert(&cache, "key", "alias", 10);
        let pin = cache.try_pin_alias("alias".to_string()).unwrap();
        drop(pin);
        assert!(cache.invalidate(&"key".to_string()));
        assert!(!cache.contains_key(&"key".to_string()));
    }

    #[test]
    fn weighted_pop_keeps_key_granularity_and_can_overshoot() {
        let evicted = Arc::new(Mutex::new(Vec::new()));
        let cache = cache(100, evicted.clone());
        insert(&cache, "first", "a", 12);
        insert(&cache, "second", "b", 7);
        assert_eq!(cache.evict_some(13), 19);
        assert_eq!(evicted.lock().len(), 2);
    }
}
