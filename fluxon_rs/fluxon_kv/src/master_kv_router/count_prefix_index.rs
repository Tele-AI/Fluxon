use std::collections::HashMap;

type KeyVersion = (u64, u32);

/// Radix tree node used for prefix counting.
///
/// Each node tracks the total number of keys in its subtree via `count`.
#[derive(Default)]
struct Node {
    count: u64,
    children: HashMap<u8, Node>,
}

impl Node {
    fn new() -> Self {
        Self {
            count: 0,
            children: HashMap::new(),
        }
    }
}

/// Simple radix tree for string prefixes, storing counts only.
///
/// Callers must ensure `insert` and `remove` are balanced for each key;
/// violations are treated as logic bugs and will panic.
#[derive(Default)]
pub struct PrefixRadixTree {
    root: Node,
    /// Current version for every logical key represented in the radix counts.
    ///
    /// Route publication and final-route reclaim are intentionally processed by separate async
    /// actors. Their events can therefore be duplicated or observed out of order. Keeping the
    /// version here makes both operations idempotent and prevents an old remove from deleting a
    /// newer incarnation of the same key.
    versions: HashMap<String, KeyVersion>,
}

impl PrefixRadixTree {
    pub fn new() -> Self {
        Self {
            root: Node::new(),
            versions: HashMap::new(),
        }
    }

    /// Insert or update a key version. Returns true only when a new logical key was counted.
    pub fn insert(&mut self, key: &str, version: KeyVersion) -> bool {
        if let Some(current) = self.versions.get_mut(key) {
            if *current != version {
                *current = version;
            }
            return false;
        }

        self.versions.insert(key.to_string(), version);
        let bytes = key.as_bytes();
        let mut node = &mut self.root;
        node.count = node
            .count
            .checked_add(1)
            .expect("PrefixRadixTree count overflow on insert (root)");

        for &b in bytes {
            node = node.children.entry(b).or_insert_with(Node::new);
            node.count = node
                .count
                .checked_add(1)
                .expect("PrefixRadixTree count overflow on insert (child)");
        }
        true
    }

    /// Remove exactly one key version. Missing, duplicate, or stale removes are harmless.
    pub fn remove(&mut self, key: &str, version: KeyVersion) -> bool {
        if !self
            .versions
            .get(key)
            .is_some_and(|current| *current == version)
        {
            return false;
        }

        // Validate the complete path before mutating any count. This keeps the operation
        // non-panicking even if an index built by older code was inconsistent.
        let mut current = &self.root;
        for &byte in key.as_bytes() {
            let Some(child) = current.children.get(&byte) else {
                self.versions.remove(key);
                return false;
            };
            current = child;
        }
        self.versions.remove(key);

        fn remove_inner(node: &mut Node, bytes: &[u8], idx: usize) -> bool {
            node.count = node.count.saturating_sub(1);

            if idx == bytes.len() {
                return node.count == 0;
            }

            let b = bytes[idx];
            let Some(child) = node.children.get_mut(&b) else {
                return node.count == 0;
            };
            let should_prune = remove_inner(child, bytes, idx + 1);
            if should_prune {
                node.children.remove(&b);
            }
            node.count == 0
        }

        remove_inner(&mut self.root, key.as_bytes(), 0);
        true
    }

    /// Count keys whose name starts with the given prefix.
    pub fn count_prefix(&self, prefix: &str) -> u64 {
        let mut node = &self.root;
        for &b in prefix.as_bytes() {
            match node.children.get(&b) {
                Some(child) => node = child,
                None => return 0,
            }
        }
        node.count
    }
}

#[cfg(test)]
mod tests {
    use super::PrefixRadixTree;

    #[test]
    fn duplicate_insert_and_remove_are_idempotent() {
        let mut tree = PrefixRadixTree::new();
        assert!(tree.insert("prefix/key", (1, 0)));
        assert!(!tree.insert("prefix/key", (1, 0)));
        assert_eq!(tree.count_prefix("prefix/"), 1);

        assert!(tree.remove("prefix/key", (1, 0)));
        assert!(!tree.remove("prefix/key", (1, 0)));
        assert_eq!(tree.count_prefix("prefix/"), 0);
    }

    #[test]
    fn stale_remove_cannot_delete_newer_key_version() {
        let mut tree = PrefixRadixTree::new();
        assert!(tree.insert("prefix/key", (1, 0)));
        assert!(!tree.insert("prefix/key", (2, 0)));
        assert!(!tree.remove("prefix/key", (1, 0)));
        assert_eq!(tree.count_prefix("prefix/"), 1);

        assert!(tree.remove("prefix/key", (2, 0)));
        assert_eq!(tree.count_prefix("prefix/"), 0);
    }

    #[test]
    fn remove_before_delayed_insert_is_a_noop() {
        let mut tree = PrefixRadixTree::new();
        assert!(!tree.remove("prefix/key", (3, 0)));
        assert!(tree.insert("prefix/key", (3, 0)));
        assert_eq!(tree.count_prefix("prefix/"), 1);
    }
}
