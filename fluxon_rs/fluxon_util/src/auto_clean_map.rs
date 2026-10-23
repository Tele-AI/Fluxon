use dashmap::DashMap;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::{Arc, Weak};
use uuid::Uuid;

/// Map 中实际存储的结构：包含值及其唯一版本 ID
struct StoredEntry<V> {
    value: Arc<V>,
    version_id: Uuid,
}

/// 一个具备自动清理能力的并发 Map。
///
/// - Map 只持有 `Weak` 引用，调用方通过返回的 `AutoCleanMapEntry` 保持强引用；
/// - 当 `AutoCleanMapEntry` 被 Drop，且该版本是最后一个强引用时，自动从 Map 移除；
/// - 重复 get_or_init 时如果弱引用可升级则直接复用，否则使用 `init_fn` 覆盖并生成新版本。
pub struct AutoCleanMap<K, V> {
    // DashMap 存储 K -> Weak<StoredEntry<V>> 的引用，Map 本身不持有强引用。
    inner: Arc<DashMap<K, Weak<StoredEntry<V>>>>,
}

impl<K, V> AutoCleanMap<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    /// 获取 Key 对应的值。如果不存在，则使用 init_fn 初始化。
    pub fn get_or_init<F>(&self, key: K, init_fn: F) -> AutoCleanMapEntry<K, V>
    where
        F: FnOnce() -> V,
    {
        let (e, _created) = self.get_or_init_with(key, init_fn);
        e
    }

    /// 和 `get_or_init` 一样，但返回是否新建。
    pub fn get_or_init_with<F>(&self, key: K, init_fn: F) -> (AutoCleanMapEntry<K, V>, bool)
    where
        F: FnOnce() -> V,
    {
        let entry = self.inner.entry(key.clone());

        match entry {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                // 尝试升级 Weak 指针
                if let Some(strong_stored) = occupied.get().upgrade() {
                    // 升级成功，直接返回带有版本号的 Entry
                    return (
                        AutoCleanMapEntry {
                            key,
                            version_id: strong_stored.version_id,
                            stored: strong_stored,
                            map_ref: Arc::downgrade(&self.inner),
                        },
                        false,
                    );
                }

                // Weak 指针失效，说明之前的版本已过期，需要覆盖
                let version_id = Uuid::new_v4();
                let value_arc = Arc::new(init_fn());

                let new_stored = Arc::new(StoredEntry {
                    value: value_arc.clone(),
                    version_id,
                });
                occupied.insert(Arc::downgrade(&new_stored));

                (
                    AutoCleanMapEntry {
                        key,
                        version_id,
                        stored: new_stored,
                        map_ref: Arc::downgrade(&self.inner),
                    },
                    true,
                )
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                // Key 不存在，创建新值和新版本
                let version_id = Uuid::new_v4();
                let value_arc = Arc::new(init_fn());

                let new_stored = Arc::new(StoredEntry {
                    value: value_arc.clone(),
                    version_id,
                });
                vacant.insert(Arc::downgrade(&new_stored));

                (
                    AutoCleanMapEntry {
                        key,
                        version_id,
                        stored: new_stored,
                        map_ref: Arc::downgrade(&self.inner),
                    },
                    true,
                )
            }
        }
    }

    /// Return a guard for an existing live entry without creating a new value.
    pub fn get_existing(&self, key: &K) -> Option<AutoCleanMapEntry<K, V>> {
        let stored = self
            .inner
            .get(key)
            .and_then(|entry| entry.value().upgrade())?;
        Some(AutoCleanMapEntry {
            key: key.clone(),
            version_id: stored.version_id,
            stored,
            map_ref: Arc::downgrade(&self.inner),
        })
    }

    /// 若 key 存在且 Weak 可升级，执行闭包并返回结果；否则返回 None。
    pub fn with_existing<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        self.inner
            .get(key)
            .and_then(|r| r.value().upgrade())
            .map(|stored| f(&stored.value))
    }

    /// 拍快照：对当前所有可升级的条目执行映射。
    pub fn snapshot_map<T, F>(&self, mut f: F) -> Vec<T>
    where
        F: FnMut(&K, &V) -> T,
    {
        let mut out = Vec::new();
        for r in self.inner.iter() {
            if let Some(stored) = r.value().upgrade() {
                out.push(f(r.key(), &stored.value));
            }
        }
        out
    }

    /// 带谓词的拍快照：对满足 `pred` 的可升级条目执行映射。
    pub fn snapshot_filter_map<T, P, F>(&self, pred: P, mut f: F) -> Vec<T>
    where
        P: Fn(&K, &V) -> bool,
        F: FnMut(&K, &V) -> T,
    {
        let mut out = Vec::new();
        for r in self.inner.iter() {
            if let Some(stored) = r.value().upgrade() {
                let vref = &stored.value;
                if pred(r.key(), vref) {
                    out.push(f(r.key(), vref));
                }
            }
        }
        out
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// Map 返回的句柄，持有数据的强引用，并携带其版本号。
pub struct AutoCleanMapEntry<K, V>
where
    K: Eq + Hash,
{
    key: K,
    version_id: Uuid,            // 携带版本号用于析构时的校验
    stored: Arc<StoredEntry<V>>, // 强引用 StoredEntry，用于精确校验
    // Map 的 Weak 引用指向 Weak<StoredEntry<V>>
    map_ref: Weak<DashMap<K, Weak<StoredEntry<V>>>>,
}

impl<K, V> Drop for AutoCleanMapEntry<K, V>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        // 这里不依赖 value 的 strong_count，而是基于 StoredEntry<V> 的强引用计数与版本校验
        if let Some(map) = self.map_ref.upgrade() {
            let self_ptr = Arc::as_ptr(&self.stored);
            let version_id = self.version_id;
            // 使用 remove_if 保证并发场景下的原子校验与删除
            map.remove_if(&self.key, |_, weak_stored| {
                // 避免升级 weak 引起强引用数 +1，直接比较底层指针
                let same_entry = std::ptr::eq(weak_stored.as_ptr(), self_ptr);
                // 仅当当前就是最后一个持有者才移除
                let is_last_ref = Arc::strong_count(&self.stored) == 1;
                let version_matches = self.stored.version_id == version_id;
                same_entry && version_matches && is_last_ref
            });
        }
    }
}

// 实现 Deref，使其使用起来像 &V
impl<K: Eq + Hash, V> Deref for AutoCleanMapEntry<K, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.stored.value
    }
}

// 实现 Clone，增加 Arc 引用计数和版本号的复制
impl<K, V> Clone for AutoCleanMapEntry<K, V>
where
    K: Clone + Eq + Hash,
{
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            version_id: self.version_id,
            stored: self.stored.clone(),
            map_ref: self.map_ref.clone(),
        }
    }
}
