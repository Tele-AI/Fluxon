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
        // Moka defines zero as a bounded cache that immediately selects every
        // admitted entry.  Keep that native meaning: a zero GlobalShared
        // budget must retain the same pin/reclaim state machine so a runtime
        // boundary can drain to zero and later grow again without replacing
        // the controller or losing generation-fenced resident accounting.
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

/// Result of one explicit, predicate-scoped selection pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PinAwareMokaSelection {
    pub selected_entries: usize,
    pub selected_weight: u64,
    pub pinned_entries: usize,
    pub already_selected_entries: usize,
}

/// Exact resident-capacity rejection for an atomic bounded insertion.
///
/// Unlike Moka's `weighted_size`, `resident_weight` includes entries that are
/// currently pinned or already selected for asynchronous reclaim. Those
/// entries still own their physical backing and therefore cannot be reused as
/// admission credit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PinAwareMokaResidentCapacityError {
    pub max_capacity: u64,
    pub resident_weight: u64,
    pub displaced_weight: u64,
    pub requested_weight: u64,
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
    fn insert_under_mutation(
        &self,
        key: Key,
        pin_aliases: Arc<[PinAlias]>,
        value: Value,
        weight: u32,
        resident_limit: Option<u64>,
    ) -> Result<u64, PinAwareMokaResidentCapacityError> {
        let zero_capacity = self.cache.policy().max_capacity() == Some(0);
        let (generation, admit, selected, displaced) = {
            let mut state = self.state.lock();

            let mut displaced_keys = HashSet::new();
            if state.entries.contains_key(&key) {
                displaced_keys.insert(key.clone());
            }
            for alias in pin_aliases.iter() {
                if let Some((old_key, _)) = state.aliases.get(alias) {
                    displaced_keys.insert(old_key.clone());
                }
            }
            let resident_weight = state.entries.values().fold(0u64, |total, entry| {
                total.saturating_add(u64::from(entry.weight))
            });
            let displaced_weight = displaced_keys.iter().fold(0u64, |total, old_key| {
                total.saturating_add(
                    state
                        .entries
                        .get(old_key)
                        .map(|entry| u64::from(entry.weight))
                        .unwrap_or(0),
                )
            });
            if let Some(max_capacity) = resident_limit {
                let requested_weight = u64::from(weight);
                if resident_weight
                    .saturating_sub(displaced_weight)
                    .saturating_add(requested_weight)
                    > max_capacity
                {
                    return Err(PinAwareMokaResidentCapacityError {
                        max_capacity,
                        resident_weight,
                        displaced_weight,
                        requested_weight,
                    });
                }
            }

            let generation = state.next_generation;
            state.next_generation = state
                .next_generation
                .checked_add(1)
                .expect("pin-aware Moka generation space exhausted");

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
            let residency = if pin_count != 0 {
                Residency::Pinned
            } else if zero_capacity {
                // Moka intentionally drops a new entry at capacity zero
                // without an eviction notification. Select it explicitly so
                // the wrapper retains its generation until the existing
                // external reclaim listener reaches a terminal.
                Residency::Selected
            } else {
                Residency::InMoka
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
                value: value.clone(),
                weight,
                _pin_alias: std::marker::PhantomData,
            });
            let selected = (residency == Residency::Selected).then_some(value);
            (generation, admit, selected, displaced)
        };
        for (old_key, old_generation) in displaced {
            self.remove_moka_generation(&old_key, old_generation);
        }
        if let Some(entry) = admit {
            self.cache.insert(key.clone(), entry);
        }
        if let Some(value) = selected
            && let Some(listener) = self.eviction_listener.as_ref()
        {
            listener(Arc::new(key), value, RemovalCause::Size);
        }
        Ok(generation)
    }

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
        let zero_capacity = self.cache.policy().max_capacity() == Some(0);
        let (entry_to_admit, entry_to_select) = {
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
                    (None, None)
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
                        if zero_capacity {
                            entry.residency = Residency::Selected;
                            (None, Some((key, entry.value.clone())))
                        } else {
                            entry.residency = Residency::InMoka;
                            (
                                Some((
                                    key,
                                    MokaEntry {
                                        generation,
                                        value: entry.value.clone(),
                                        weight: entry.weight,
                                        _pin_alias: std::marker::PhantomData,
                                    },
                                )),
                                None,
                            )
                        }
                    } else {
                        (None, None)
                    }
                }
            }
        };
        if let Some((key, entry)) = entry_to_admit {
            self.cache.insert(key, entry);
        }
        if let Some((key, value)) = entry_to_select
            && let Some(listener) = self.eviction_listener.as_ref()
        {
            listener(Arc::new(key), value, RemovalCause::Size);
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
        self.inner
            .insert_under_mutation(key, pin_aliases, value, weight, None)
            .expect("unbounded pin-aware Moka insertion cannot reject")
    }

    /// Atomically insert only when every physically resident entry plus the
    /// new generation fits the current configured capacity.
    ///
    /// Pinned and already-selected entries remain charged until they are
    /// explicitly invalidated after their external lifecycle completes.
    pub fn try_insert_with_resident_capacity(
        &self,
        key: Key,
        pin_aliases: impl IntoIterator<Item = PinAlias>,
        value: Value,
    ) -> Result<u64, PinAwareMokaResidentCapacityError> {
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
        let max_capacity = self.inner.cache.policy().max_capacity().unwrap_or(0);
        self.inner
            .insert_under_mutation(key, pin_aliases, value, weight, Some(max_capacity))
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

    /// Select every unpinned entry accepted by `predicate`.
    ///
    /// Each selected entry keeps the same single-entry listener contract as a
    /// Moka size eviction. Pinned entries remain resident and are reported so
    /// a higher-level drain transaction can wait and retry without selecting
    /// unrelated keys.
    pub fn select_entries_if(
        &self,
        mut predicate: impl FnMut(&Key, &Value) -> bool,
    ) -> PinAwareMokaSelection {
        let _mutation = self.inner.mutation.lock();
        let (selected, result) = {
            let mut state = self.inner.state.lock();
            let mut selected = Vec::new();
            let mut result = PinAwareMokaSelection::default();
            for (key, entry) in &mut state.entries {
                if !predicate(key, &entry.value) {
                    continue;
                }
                match entry.residency {
                    Residency::InMoka => {
                        entry.residency = Residency::Selected;
                        result.selected_entries = result.selected_entries.saturating_add(1);
                        result.selected_weight = result
                            .selected_weight
                            .saturating_add(u64::from(entry.weight));
                        selected.push((key.clone(), entry.generation, entry.value.clone()));
                    }
                    Residency::Pinned => {
                        result.pinned_entries = result.pinned_entries.saturating_add(1);
                    }
                    Residency::Selected => {
                        result.already_selected_entries =
                            result.already_selected_entries.saturating_add(1);
                    }
                }
            }
            (selected, result)
        };

        for (key, generation, value) in selected {
            self.inner.remove_moka_generation(&key, generation);
            if let Some(listener) = self.inner.eviction_listener.as_ref() {
                listener(Arc::new(key), value, RemovalCause::Size);
            }
        }
        result
    }

    /// Aggregate the weight currently counted by Moka by a caller-defined group.
    ///
    /// Pinned and already-selected entries are intentionally excluded because
    /// Moka's `weighted_size` and capacity enforcement exclude them too.  The
    /// callback runs under the wrapper's mutation lock and must not call back
    /// into this cache.
    pub fn in_moka_weight_by<Group>(
        &self,
        mut classify: impl FnMut(&Key, &Value) -> Option<Group>,
    ) -> HashMap<Group, u64>
    where
        Group: Eq + Hash,
    {
        let _mutation = self.inner.mutation.lock();
        let state = self.inner.state.lock();
        let mut weights = HashMap::<Group, u64>::new();
        for (key, entry) in &state.entries {
            if entry.residency != Residency::InMoka {
                continue;
            }
            let Some(group) = classify(key, &entry.value) else {
                continue;
            };
            let weight = weights.entry(group).or_default();
            *weight = weight.saturating_add(u64::from(entry.weight));
        }
        weights
    }

    pub fn run_pending_tasks(&self) {
        let _mutation = self.inner.mutation.lock();
        self.inner.cache.run_pending_tasks();
    }

    pub fn set_max_capacity(&self, capacity: u64) -> Result<(), moka::CapacityError> {
        let _mutation = self.inner.mutation.lock();
        self.inner.cache.set_max_capacity(capacity)
    }

    /// Lower or raise Moka's capacity only when the current resident weight
    /// already fits.  This is used after an explicitly selected physical
    /// extent has drained: returning `Ok(false)` guarantees that this call did
    /// not ask Moka to choose a second, unrelated victim set.
    pub fn try_set_max_capacity_without_eviction(
        &self,
        capacity: u64,
    ) -> Result<bool, moka::CapacityError> {
        let _mutation = self.inner.mutation.lock();
        self.inner.cache.run_pending_tasks();
        if self.inner.cache.weighted_size() > capacity {
            return Ok(false);
        }
        self.inner.cache.set_max_capacity(capacity)?;
        self.inner.cache.run_pending_tasks();
        debug_assert!(self.inner.cache.weighted_size() <= capacity);
        Ok(true)
    }

    pub fn max_capacity(&self) -> Option<u64> {
        self.inner.cache.policy().max_capacity()
    }

    pub fn weighted_size(&self) -> u64 {
        self.inner.cache.weighted_size()
    }

    /// Weight of every wrapper-resident generation, including entries hidden
    /// from Moka while pinned or after selection for asynchronous reclaim.
    pub fn resident_weighted_size(&self) -> u64 {
        let _mutation = self.inner.mutation.lock();
        self.inner
            .state
            .lock()
            .entries
            .values()
            .fold(0u64, |total, entry| {
                total.saturating_add(u64::from(entry.weight))
            })
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
    fn resident_capacity_charges_pinned_and_selected_entries_until_invalidation() {
        let evicted = Arc::new(Mutex::new(Vec::new()));
        let cache = cache(100, evicted.clone());
        insert(&cache, "old", "old-alias", 70);
        assert_eq!(cache.weighted_size(), 70);
        assert_eq!(cache.resident_weighted_size(), 70);

        let pin = cache.try_pin_alias("old-alias".to_string()).unwrap();
        cache.run_pending_tasks();
        assert_eq!(cache.weighted_size(), 0);
        assert_eq!(cache.resident_weighted_size(), 70);
        let pinned_reject = cache
            .try_insert_with_resident_capacity("new".to_string(), ["new-alias".to_string()], 40)
            .unwrap_err();
        assert_eq!(
            pinned_reject,
            PinAwareMokaResidentCapacityError {
                max_capacity: 100,
                resident_weight: 70,
                displaced_weight: 0,
                requested_weight: 40,
            }
        );

        drop(pin);
        cache.run_pending_tasks();
        assert_eq!(cache.evict_some(1), 70);
        assert_eq!(evicted.lock().as_slice(), &["old".to_string()]);
        assert_eq!(cache.weighted_size(), 0);
        assert_eq!(cache.resident_weighted_size(), 70);
        assert!(
            cache
                .try_insert_with_resident_capacity(
                    "new".to_string(),
                    ["new-alias".to_string()],
                    40,
                )
                .is_err(),
            "selected entries still own their physical backing"
        );

        assert!(cache.invalidate(&"old".to_string()));
        assert_eq!(cache.resident_weighted_size(), 0);
        cache
            .try_insert_with_resident_capacity("new".to_string(), ["new-alias".to_string()], 40)
            .unwrap();
        cache.run_pending_tasks();
        assert_eq!(cache.weighted_size(), 40);
        assert_eq!(cache.resident_weighted_size(), 40);
    }

    #[test]
    fn zero_capacity_is_a_valid_disabled_boundary() {
        let evicted = Arc::new(Mutex::new(Vec::new()));
        let cache = cache(0, evicted.clone());
        assert_eq!(cache.max_capacity(), Some(0));

        let rejected = cache
            .try_insert_with_resident_capacity(
                "rejected".to_string(),
                ["rejected-alias".to_string()],
                1,
            )
            .unwrap_err();
        assert_eq!(
            rejected,
            PinAwareMokaResidentCapacityError {
                max_capacity: 0,
                resident_weight: 0,
                displaced_weight: 0,
                requested_weight: 1,
            }
        );
        assert_eq!(cache.resident_weighted_size(), 0);

        // The unbounded route-publication path still enters the existing
        // single-entry listener/reclaim protocol instead of bypassing it.
        insert(&cache, "selected", "selected-alias", 1);
        assert_eq!(cache.weighted_size(), 0);
        assert_eq!(cache.resident_weighted_size(), 1);
        assert_eq!(evicted.lock().as_slice(), &["selected".to_string()]);
        assert!(cache.invalidate(&"selected".to_string()));
        assert_eq!(cache.resident_weighted_size(), 0);

        evicted.lock().clear();
        let pin = cache
            .try_pin_alias("pinned-alias".to_string())
            .expect("pending pin at a zero boundary remains valid");
        insert(&cache, "pinned", "pinned-alias", 1);
        assert!(evicted.lock().is_empty());
        assert_eq!(cache.resident_weighted_size(), 1);
        drop(pin);
        assert_eq!(evicted.lock().as_slice(), &["pinned".to_string()]);
        assert!(cache.invalidate(&"pinned".to_string()));
        assert_eq!(cache.resident_weighted_size(), 0);
    }

    #[test]
    fn shrinking_to_zero_selects_existing_entries() {
        let evicted = Arc::new(Mutex::new(Vec::new()));
        let cache = cache(100, evicted.clone());
        insert(&cache, "first", "first-alias", 40);
        insert(&cache, "second", "second-alias", 30);
        assert_eq!(cache.weighted_size(), 70);

        cache.set_max_capacity(0).unwrap();
        cache.run_pending_tasks();
        assert_eq!(cache.max_capacity(), Some(0));
        assert_eq!(cache.weighted_size(), 0);
        assert_eq!(cache.resident_weighted_size(), 70);
        assert_eq!(evicted.lock().len(), 2);
    }

    #[test]
    fn zero_born_boundary_can_grow_without_losing_pin_or_reclaim_accounting() {
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            let evicted = Arc::new(Mutex::new(Vec::new()));
            let cache = cache(0, evicted.clone());
            let pin = cache.try_pin_alias("alias".to_string()).unwrap();
            insert(&cache, "pinned", "alias", 40);
            assert_eq!(cache.resident_weighted_size(), 40);
            assert_eq!(cache.weighted_size(), 0);
            cache.set_max_capacity(100).unwrap();
            drop(pin);
            cache.run_pending_tasks();
            assert_eq!(cache.weighted_size(), 40);
            cache
                .try_insert_with_resident_capacity("second".into(), ["other".into()], 60)
                .unwrap();
            cache.run_pending_tasks();
            assert_eq!(cache.resident_weighted_size(), 100);
            assert!(
                cache
                    .try_insert_with_resident_capacity("extra".into(), ["extra".into()], 1)
                    .is_err()
            );
            cache.set_max_capacity(0).unwrap();
            cache.run_pending_tasks();
            assert_eq!(cache.weighted_size(), 0);
            assert_eq!(cache.resident_weighted_size(), 100);
            assert_eq!(evicted.lock().len(), 2);
            cache.set_max_capacity(100).unwrap();
            assert!(
                cache
                    .try_insert_with_resident_capacity("extra".into(), ["extra".into()], 1)
                    .is_err(),
                "selection must not create reclaim credit"
            );
            assert!(cache.invalidate(&"pinned".into()));
            assert!(cache.invalidate(&"second".into()));
            cache
                .try_insert_with_resident_capacity("new".into(), ["new".into()], 100)
                .unwrap();
            cache.run_pending_tasks();
            assert_eq!(cache.resident_weighted_size(), 100);
            assert_eq!(cache.weighted_size(), 100);
            tx.send(()).unwrap();
        });
        rx.recv_timeout(std::time::Duration::from_secs(2))
            .expect("zero-born cache must reopen without stalling a final unpin");
        worker.join().unwrap();
    }

    #[test]
    fn concurrent_resident_admission_cannot_overbook_capacity() {
        let cache = cache(100, Arc::new(Mutex::new(Vec::new())));
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for index in 0..2 {
            let cache = cache.clone();
            let barrier = barrier.clone();
            workers.push(thread::spawn(move || {
                barrier.wait();
                cache
                    .try_insert_with_resident_capacity(
                        format!("key-{index}"),
                        [format!("alias-{index}")],
                        60,
                    )
                    .is_ok()
            }));
        }
        barrier.wait();
        let accepted = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|accepted| *accepted)
            .count();
        assert_eq!(accepted, 1);
        assert_eq!(cache.resident_weighted_size(), 60);
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

    #[test]
    fn explicit_weighted_pop_skips_pinned_entries_until_they_are_released() {
        let evicted = Arc::new(Mutex::new(Vec::new()));
        let cache = cache(100, evicted.clone());
        insert(&cache, "pinned", "pin", 40);
        insert(&cache, "first", "a", 30);
        insert(&cache, "second", "b", 30);
        let pin = cache.try_pin_alias("pin".to_string()).unwrap();

        assert_eq!(cache.evict_some(50), 60);
        assert_eq!(evicted.lock().len(), 2);
        assert_eq!(cache.get(&"pinned".to_string()), Some(40));

        drop(pin);
        cache.run_pending_tasks();
        assert_eq!(cache.evict_some(1), 40);
        assert_eq!(evicted.lock().len(), 3);
    }

    #[test]
    fn predicate_selection_skips_pins_and_reuses_single_entry_listener() {
        let evicted = Arc::new(Mutex::new(Vec::new()));
        let cache = cache(100, evicted.clone());
        insert(&cache, "first", "a", 12);
        insert(&cache, "second", "b", 7);
        insert(&cache, "third", "c", 5);
        let pin = cache.try_pin_alias("b".to_string()).unwrap();

        let first = cache.select_entries_if(|key, _| key != "third");
        assert_eq!(
            first,
            PinAwareMokaSelection {
                selected_entries: 1,
                selected_weight: 12,
                pinned_entries: 1,
                already_selected_entries: 0,
            }
        );
        assert_eq!(&*evicted.lock(), &["first"]);

        let second = cache.select_entries_if(|key, _| key != "third");
        assert_eq!(second.selected_entries, 0);
        assert_eq!(second.pinned_entries, 1);
        assert_eq!(second.already_selected_entries, 1);

        drop(pin);
        let third = cache.select_entries_if(|key, _| key != "third");
        assert_eq!(third.selected_entries, 1);
        assert_eq!(third.selected_weight, 7);
        assert_eq!(third.already_selected_entries, 1);
        assert_eq!(evicted.lock().len(), 2);
    }

    #[test]
    fn grouped_weight_counts_only_entries_currently_charged_to_moka() {
        let cache = cache(100, Arc::new(Mutex::new(Vec::new())));
        insert(&cache, "grant-1-a", "a", 12);
        insert(&cache, "grant-1-b", "b", 7);
        insert(&cache, "grant-2", "c", 5);
        let pin = cache.try_pin_alias("b".to_string()).unwrap();

        let weights = cache.in_moka_weight_by(|key, _| {
            key.strip_prefix("grant-")
                .and_then(|suffix| suffix.split('-').next())
                .map(str::to_string)
        });
        assert_eq!(weights.get("1"), Some(&12));
        assert_eq!(weights.get("2"), Some(&5));

        drop(pin);
        cache.run_pending_tasks();
        let weights = cache.in_moka_weight_by(|key, _| {
            key.strip_prefix("grant-")
                .and_then(|suffix| suffix.split('-').next())
                .map(str::to_string)
        });
        assert_eq!(weights.get("1"), Some(&19));
    }

    #[test]
    fn capacity_update_refuses_to_create_an_unrelated_victim_set() {
        let evicted = Arc::new(Mutex::new(Vec::new()));
        let cache = cache(100, evicted.clone());
        insert(&cache, "large", "a", 60);
        insert(&cache, "small", "b", 30);

        assert!(!cache.try_set_max_capacity_without_eviction(80).unwrap());
        assert_eq!(cache.max_capacity(), Some(100));
        assert!(evicted.lock().is_empty());

        assert_eq!(
            cache
                .select_entries_if(|key, _| key == "large")
                .selected_weight,
            60
        );
        assert!(cache.try_set_max_capacity_without_eviction(40).unwrap());
        assert_eq!(cache.max_capacity(), Some(40));
        assert_eq!(&*evicted.lock(), &["large"]);
    }
}
