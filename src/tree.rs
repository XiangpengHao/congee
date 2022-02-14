use std::{marker::PhantomData, mem::ManuallyDrop};

use crossbeam_epoch::Guard;

use crate::{
    base_node::{BaseNode, Node, Prefix, MAX_STORED_PREFIX_LEN},
    child_ptr::NodePtr,
    key::RawKey,
    lock::ReadGuard,
    node_256::Node256,
    node_4::Node4,
    range_scan::{KeyTracker, RangeScan},
    utils::Backoff,
};

enum CheckPrefixResult {
    NotMatch,
    Match(u32),
    OptimisticMatch(u32),
}

enum CheckPrefixPessimisticResult {
    Match,
    NotMatch((u8, Prefix)),
}

/// Raw interface to the ART tree.
/// The `Art` is a wrapper around the `RawArt` that provides a safe interface.
/// Unlike `Art`, it support arbitrary `Key` types, see also `RawKey`.
pub struct RawTree<K: RawKey> {
    // use ManuallyDrop to avoid calling drop on the root node:
    // On drop(), the Box will try deallocate the memory BaseNode
    root: ManuallyDrop<Box<Node256>>,
    _pt_key: PhantomData<K>,
}

impl<K: RawKey> Default for RawTree<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: RawKey> Drop for RawTree<T> {
    fn drop(&mut self) {
        let v = unsafe { ManuallyDrop::take(&mut self.root) };
        let mut sub_nodes = vec![Box::into_raw(v) as *const BaseNode];

        while !sub_nodes.is_empty() {
            let node = sub_nodes.pop().unwrap();
            let children = unsafe { &*node }.get_children(0, 255);
            for (_k, n) in children.iter() {
                if !n.is_leaf() {
                    sub_nodes.push(n.as_ptr());
                }
            }
            unsafe {
                std::ptr::drop_in_place(node as *mut BaseNode);
            }
        }
    }
}

impl<T: RawKey> RawTree<T> {
    pub fn new() -> Self {
        RawTree {
            root: ManuallyDrop::new(Node256::new(&[])),
            _pt_key: PhantomData,
        }
    }
}

impl<T: RawKey> RawTree<T> {
    pub fn get(&self, key: &T, _guard: &Guard) -> Option<usize> {
        'outer: loop {
            let mut parent_node;
            let mut level = 0;
            let mut opt_prefix_match = false;

            let mut node = if let Ok(v) = self.root.base().read_lock() {
                v
            } else {
                continue;
            };

            loop {
                match Self::check_prefix(node.as_ref(), key, level) {
                    CheckPrefixResult::NotMatch => {
                        return None;
                    }
                    CheckPrefixResult::Match(l) => {
                        level = l;
                    }
                    CheckPrefixResult::OptimisticMatch(l) => {
                        level = l;
                        opt_prefix_match = true;
                    }
                }

                if key.len() <= level as usize {
                    return None;
                }

                parent_node = node;
                let child_node = parent_node
                    .as_ref()
                    .get_child(key.as_bytes()[level as usize]);
                if ReadGuard::check_version(&parent_node).is_err() {
                    continue 'outer;
                }

                let child_node = child_node?;

                if child_node.is_leaf() {
                    let tid = child_node.as_tid();
                    if (level as usize) < key.len() - 1 || opt_prefix_match {
                        return None;
                    }
                    return Some(tid);
                }
                level += 1;

                node = if let Ok(n) = unsafe { &*child_node.as_ptr() }.read_lock() {
                    n
                } else {
                    continue 'outer;
                };
            }
        }
    }

    #[inline]
    fn insert_inner(&self, k: &T, tid: usize, guard: &Guard) -> Result<(), usize> {
        let mut parent_node = None;
        let mut next_node = self.root.as_ref() as *const Node256 as *const BaseNode;
        let mut parent_key: u8;
        let mut node_key: u8 = 0;
        let mut level = 0;

        let mut node;

        loop {
            parent_key = node_key;
            node = unsafe { &*next_node }.read_lock()?;

            let mut next_level = level;
            let res = self.check_prefix_pessimistic(node.as_ref(), k, &mut next_level);
            match res {
                CheckPrefixPessimisticResult::Match => {
                    level = next_level;
                    node_key = k.as_bytes()[level as usize];

                    let next_node_tmp = node.as_ref().get_child(node_key);

                    node.check_version()?;

                    let next_node_tmp = if let Some(n) = next_node_tmp {
                        n
                    } else {
                        let new_leaf = {
                            if level as usize == k.len() - 1 {
                                // last key, just insert the tid
                                NodePtr::from_tid(tid)
                            } else {
                                let new_prefix = k.as_bytes();
                                let mut n4 =
                                    Node4::new(&new_prefix[level as usize + 1..k.len() - 1]);
                                n4.insert(k.as_bytes()[k.len() - 1], NodePtr::from_tid(tid));
                                NodePtr::from_node(Box::into_raw(n4) as *mut BaseNode)
                            }
                        };

                        if BaseNode::insert_and_unlock(
                            node,
                            parent_node,
                            parent_key,
                            node_key,
                            new_leaf,
                            guard,
                        )
                        .is_err()
                        {
                            if level as usize != k.len() - 1 {
                                unsafe {
                                    // TODO: this is UB
                                    std::ptr::drop_in_place(new_leaf.as_ptr() as *mut BaseNode);
                                }
                            }
                            return Err(0);
                        }

                        return Ok(());
                    };

                    if let Some(p) = parent_node {
                        p.unlock()?;
                    }

                    if next_node_tmp.is_tid() {
                        // At this point, the level must point to the last u8 of the key,
                        // meaning that we are updating an existing value.
                        let mut write_n = node.upgrade().map_err(|(_n, v)| v)?;
                        write_n
                            .as_mut()
                            .change(k.as_bytes()[level as usize], NodePtr::from_tid(tid));

                        return Ok(());
                    }
                    next_node = next_node_tmp.as_ptr();
                    level += 1;
                }

                CheckPrefixPessimisticResult::NotMatch((no_match_key, prefix)) => {
                    let mut write_p = parent_node.unwrap().upgrade().map_err(|(_n, v)| v)?;
                    let mut write_n = node.upgrade().map_err(|(_n, v)| v)?;

                    // 1) Create new node which will be parent of node, Set common prefix, level to this node
                    let mut new_node =
                        Node4::new(&write_n.as_ref().prefix()[0..((next_level - level) as usize)]);

                    // 2)  add node and (tid, *k) as children
                    if next_level as usize == k.len() - 1 {
                        // this is the last key, just insert to node
                        new_node.insert(k.as_bytes()[next_level as usize], NodePtr::from_tid(tid));
                    } else {
                        // otherwise create a new node
                        let mut single_new_node =
                            Node4::new(&k.as_bytes()[(next_level as usize + 1)..k.len() - 1]);

                        single_new_node.insert(k.as_bytes()[k.len() - 1], NodePtr::from_tid(tid));
                        new_node.insert(
                            k.as_bytes()[next_level as usize],
                            NodePtr::from_node(Box::into_raw(single_new_node) as *const BaseNode),
                        );
                    }

                    new_node.insert(no_match_key, NodePtr::from_node(write_n.as_mut()));

                    // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
                    write_p.as_mut().change(
                        parent_key,
                        NodePtr::from_node(Box::into_raw(new_node) as *mut BaseNode),
                    );

                    // 4) update prefix of node, unlock
                    let prefix_len = write_n.as_ref().prefix_len();
                    write_n
                        .as_mut()
                        .set_prefix(&prefix[0..(prefix_len - (next_level - level + 1)) as usize]);
                    return Ok(());
                }
            }
            parent_node = Some(node);
        }
    }

    pub fn insert(&self, k: T, tid: usize, guard: &Guard) {
        let backoff = Backoff::new();
        while self.insert_inner(&k, tid, guard).is_err() {
            backoff.spin();
        }
    }

    #[inline]
    fn check_prefix(node: &BaseNode, key: &T, mut level: u32) -> CheckPrefixResult {
        if node.has_prefix() {
            if (key.len() as u32) <= level + node.prefix_len() {
                return CheckPrefixResult::NotMatch;
            }
            for i in 0..std::cmp::min(node.prefix_len(), MAX_STORED_PREFIX_LEN as u32) {
                if node.prefix()[i as usize] != key.as_bytes()[level as usize] {
                    return CheckPrefixResult::NotMatch;
                }
                level += 1;
            }

            if node.prefix_len() > MAX_STORED_PREFIX_LEN as u32 {
                level += node.prefix_len() - MAX_STORED_PREFIX_LEN as u32;
                return CheckPrefixResult::OptimisticMatch(level);
            }
        }
        CheckPrefixResult::Match(level)
    }

    #[inline]
    fn check_prefix_pessimistic(
        &self,
        n: &BaseNode,
        key: &T,
        level: &mut u32,
    ) -> CheckPrefixPessimisticResult {
        if n.has_prefix() {
            for i in 0..n.prefix_len() {
                let cur_key = n.prefix()[i as usize];
                if cur_key != key.as_bytes()[*level as usize] {
                    let no_matching_key = cur_key;

                    let mut prefix = Prefix::default();
                    for (j, v) in prefix
                        .iter_mut()
                        .enumerate()
                        .take((n.prefix_len() - i - 1) as usize)
                    {
                        *v = n.prefix()[j + 1 + i as usize];
                    }

                    return CheckPrefixPessimisticResult::NotMatch((no_matching_key, prefix));
                }
                *level += 1;
            }
        }

        CheckPrefixPessimisticResult::Match
    }

    pub fn range(&self, start: &T, end: &T, result: &mut [usize], _guard: &Guard) -> Option<usize> {
        let mut range_scan = RangeScan::new(
            start,
            end,
            result,
            self.root.as_ref() as *const Node256 as *const BaseNode,
        );

        if !range_scan.is_valid_key_pair() {
            return None;
        }

        let backoff = Backoff::new();
        loop {
            let scanned = range_scan.scan();
            match scanned {
                Ok(n) => {
                    return n;
                }
                Err(_) => {
                    backoff.spin();
                }
            }
        }
    }

    pub fn remove_inner(&self, k: &T, guard: &Guard) -> Result<(), usize> {
        let mut next_node = self.root.as_ref() as *const Node256 as *const BaseNode;
        let mut parent_node: Option<ReadGuard> = None;

        let mut parent_key: u8;
        let mut node_key: u8 = 0;
        let mut level = 0;
        let mut key_tracker = KeyTracker::default();

        let mut node;

        loop {
            parent_key = node_key;

            node = unsafe { &*next_node }.read_lock()?;

            match Self::check_prefix(node.as_ref(), k, level) {
                CheckPrefixResult::NotMatch => {
                    return Ok(());
                }
                CheckPrefixResult::Match(l) | CheckPrefixResult::OptimisticMatch(l) => {
                    for i in level..l {
                        key_tracker.push(k.as_bytes()[i as usize]);
                    }
                    level = l;
                    node_key = k.as_bytes()[level as usize];

                    let next_node_tmp = node.as_ref().get_child(node_key);

                    node.check_version()?;

                    let next_node_tmp = match next_node_tmp {
                        Some(n) => n,
                        None => {
                            return Ok(());
                        }
                    };

                    if next_node_tmp.is_leaf() {
                        key_tracker.push(node_key);
                        let full_key = key_tracker.to_usize_key();
                        let input_key = std::intrinsics::bswap(unsafe {
                            *(k.as_bytes().as_ptr() as *const usize)
                        });
                        if full_key != input_key {
                            return Ok(());
                        }

                        if parent_node.is_some() && node.as_ref().get_count() == 1 {
                            let mut write_p =
                                parent_node.unwrap().upgrade().map_err(|(_n, v)| v)?;

                            let mut write_n = node.upgrade().map_err(|(_n, v)| v)?;

                            write_p.as_mut().remove(parent_key);

                            write_n.mark_obsolete();
                            guard.defer(move || unsafe {
                                std::ptr::drop_in_place(write_n.as_mut());
                                std::mem::forget(write_n);
                            });
                        } else {
                            debug_assert!(parent_node.is_some());
                            let mut write_n = node.upgrade().map_err(|(_n, v)| v)?;

                            write_n.as_mut().remove(node_key);
                        }
                        return Ok(());
                    }
                    next_node = next_node_tmp.as_ptr();

                    level += 1;
                    key_tracker.push(node_key);
                }
            }
            parent_node = Some(node);
        }
    }

    #[allow(clippy::unnecessary_unwrap)]
    pub fn remove(&self, k: &T, guard: &Guard) {
        let backoff = Backoff::new();
        while self.remove_inner(k, guard).is_err() {
            backoff.spin();
        }
    }
}
