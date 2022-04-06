use std::marker::PhantomData;

use crossbeam_epoch::Guard;

use crate::{
    base_node::{BaseNode, Node, Prefix},
    key::RawKey,
    lock::ReadGuard,
    node_256::Node256,
    node_4::Node4,
    node_ptr::NodePtr,
    range_scan::RangeScan,
    utils::{ArtError, Backoff},
};

/// Raw interface to the ART tree.
/// The `Art` is a wrapper around the `RawArt` that provides a safe interface.
/// Unlike `Art`, it support arbitrary `Key` types, see also `RawKey`.
pub(crate) struct RawTree<K: RawKey> {
    pub(crate) root: *const Node256,
    _pt_key: PhantomData<K>,
}

unsafe impl<K: RawKey> Send for RawTree<K> {}
unsafe impl<K: RawKey> Sync for RawTree<K> {}

impl<K: RawKey> Default for RawTree<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: RawKey> Drop for RawTree<T> {
    fn drop(&mut self) {
        let mut sub_nodes = vec![(self.root as *const BaseNode, 0)];

        while !sub_nodes.is_empty() {
            let (node, level) = sub_nodes.pop().unwrap();

            let children = unsafe { &*node }.get_children(0, 255);
            for (_k, n) in children {
                if level != 7 {
                    sub_nodes.push((
                        n.as_ptr(),
                        level + 1 + unsafe { &*n.as_ptr() }.prefix().len(),
                    ));
                }
            }
            unsafe {
                BaseNode::drop_node(node as *mut BaseNode);
            }
        }
    }
}

impl<T: RawKey> RawTree<T> {
    pub fn new() -> Self {
        RawTree {
            root: BaseNode::make_node::<Node256>(&[]),
            _pt_key: PhantomData,
        }
    }
}

impl<T: RawKey> RawTree<T> {
    #[inline]
    pub(crate) fn get(&self, key: &T, _guard: &Guard) -> Option<usize> {
        'outer: loop {
            let mut parent_node;
            let mut level = 0;

            let mut node = if let Ok(v) = unsafe { &*self.root }.base().read_lock() {
                v
            } else {
                continue;
            };

            loop {
                level = Self::check_prefix(node.as_ref(), key, level)?;

                if key.len() <= level as usize {
                    return None;
                }

                parent_node = node;
                let child_node = parent_node
                    .as_ref()
                    .get_child(key.as_bytes()[level as usize]);
                if parent_node.check_version().is_err() {
                    continue 'outer;
                }

                let child_node = child_node?;

                if level == 7 {
                    // 7 is the last level, we can return the value
                    let tid = child_node.as_tid();
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
    fn insert_inner(&self, k: &T, tid: usize, guard: &Guard) -> Result<Option<usize>, ArtError> {
        let mut parent_node = None;
        let mut next_node = self.root as *const BaseNode;
        let mut parent_key: u8;
        let mut node_key: u8 = 0;
        let mut level = 0;

        let mut node;

        loop {
            parent_key = node_key;
            node = unsafe { &*next_node }.read_lock()?;

            let mut next_level = level;
            let res = self.check_prefix_not_match(node.as_ref(), k, &mut next_level);
            match res {
                None => {
                    level = next_level;
                    node_key = k.as_bytes()[level as usize];

                    let next_node_tmp = node.as_ref().get_child(node_key);

                    node.check_version()?;

                    let next_node_tmp = if let Some(n) = next_node_tmp {
                        n
                    } else {
                        let new_leaf = {
                            if level == 7 {
                                // last key, just insert the tid
                                NodePtr::from_tid(tid)
                            } else {
                                let new_prefix = k.as_bytes();
                                let n4 = BaseNode::make_node::<Node4>(
                                    &new_prefix[level as usize + 1..k.len() - 1],
                                );
                                unsafe { &mut *n4 }
                                    .insert(k.as_bytes()[k.len() - 1], NodePtr::from_tid(tid));
                                NodePtr::from_node(n4 as *mut BaseNode)
                            }
                        };

                        if let Err(e) = BaseNode::insert_and_unlock(
                            node,
                            parent_node,
                            parent_key,
                            node_key,
                            new_leaf,
                            guard,
                        ) {
                            if level != 7 {
                                unsafe {
                                    BaseNode::drop_node(new_leaf.as_ptr() as *mut BaseNode);
                                }
                            }
                            return Err(e);
                        }

                        return Ok(None);
                    };

                    if let Some(p) = parent_node {
                        p.unlock()?;
                    }

                    if level == 7 {
                        // At this point, the level must point to the last u8 of the key,
                        // meaning that we are updating an existing value.
                        let mut write_n = node.upgrade().map_err(|(_n, v)| v)?;
                        let old = write_n
                            .as_mut()
                            .change(k.as_bytes()[level as usize], NodePtr::from_tid(tid));

                        return Ok(Some(old.as_tid()));
                    }
                    next_node = next_node_tmp.as_ptr();
                    level += 1;
                }

                Some((no_match_key, prefix)) => {
                    let mut write_p = parent_node.unwrap().upgrade().map_err(|(_n, v)| v)?;
                    let mut write_n = node.upgrade().map_err(|(_n, v)| v)?;

                    // 1) Create new node which will be parent of node, Set common prefix, level to this node
                    let new_node = BaseNode::make_node::<Node4>(
                        write_n
                            .as_ref()
                            .prefix_range(0..((next_level - level) as usize)),
                    );

                    // 2)  add node and (tid, *k) as children
                    if next_level == 7 {
                        // this is the last key, just insert to node
                        unsafe { &mut *new_node }
                            .insert(k.as_bytes()[next_level as usize], NodePtr::from_tid(tid));
                    } else {
                        // otherwise create a new node
                        let single_new_node = BaseNode::make_node::<Node4>(
                            &k.as_bytes()[(next_level as usize + 1)..k.len() - 1],
                        );

                        unsafe { &mut *single_new_node }
                            .insert(k.as_bytes()[k.len() - 1], NodePtr::from_tid(tid));
                        unsafe { &mut *new_node }.insert(
                            k.as_bytes()[next_level as usize],
                            NodePtr::from_node(single_new_node as *const BaseNode),
                        );
                    }

                    unsafe { &mut *new_node }
                        .insert(no_match_key, NodePtr::from_node(write_n.as_mut()));

                    // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
                    write_p
                        .as_mut()
                        .change(parent_key, NodePtr::from_node(new_node as *mut BaseNode));

                    // 4) update prefix of node, unlock
                    let prefix_len = write_n.as_ref().prefix().len();
                    write_n
                        .as_mut()
                        .set_prefix(&prefix[0..(prefix_len - (next_level - level + 1) as usize)]);
                    return Ok(None);
                }
            }
            parent_node = Some(node);
        }
    }

    #[inline]
    pub(crate) fn insert(&self, k: T, tid: usize, guard: &Guard) -> Option<usize> {
        let backoff = Backoff::new();
        loop {
            match self.insert_inner(&k, tid, guard) {
                Ok(v) => return v,
                Err(_e) => {
                    backoff.spin();
                }
            }
        }
    }

    #[inline]
    fn check_prefix(node: &BaseNode, key: &T, mut level: u32) -> Option<u32> {
        let n_prefix = node.prefix();
        if !n_prefix.is_empty() {
            if key.len() <= level as usize + n_prefix.len() {
                return None;
            }

            for v in n_prefix {
                if *v != key.as_bytes()[level as usize] {
                    return None;
                }
                level += 1;
            }
        }
        Some(level)
    }

    #[inline]
    fn check_prefix_not_match(
        &self,
        n: &BaseNode,
        key: &T,
        level: &mut u32,
    ) -> Option<(u8, Prefix)> {
        let n_prefix = n.prefix();
        if !n_prefix.is_empty() {
            for (i, v) in n_prefix.iter().enumerate() {
                if *v != key.as_bytes()[*level as usize] {
                    let no_matching_key = *v;

                    let mut prefix = Prefix::default();
                    for (j, v) in prefix.iter_mut().enumerate().take(n_prefix.len() - i - 1) {
                        *v = n_prefix[j + 1 + i as usize];
                    }

                    return Some((no_matching_key, prefix));
                }
                *level += 1;
            }
        }

        None
    }

    #[inline]
    pub(crate) fn range(
        &self,
        start: &T,
        end: &T,
        result: &mut [(usize, usize)],
        _guard: &Guard,
    ) -> usize {
        let mut range_scan = RangeScan::new(start, end, result, self.root as *const BaseNode);

        if !range_scan.is_valid_key_pair() {
            return 0;
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

    pub(crate) fn remove_inner(&self, k: &T, guard: &Guard) -> Result<Option<usize>, ArtError> {
        let mut next_node = self.root as *const BaseNode;
        let mut parent_node: Option<ReadGuard> = None;

        let mut parent_key: u8;
        let mut node_key: u8 = 0;
        let mut level = 0;
        let mut key_tracker = crate::utils::KeyTracker::default();

        let mut node;

        loop {
            parent_key = node_key;

            node = unsafe { &*next_node }.read_lock()?;

            match Self::check_prefix(node.as_ref(), k, level) {
                None => {
                    return Ok(None);
                }
                Some(l) => {
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
                            return Ok(None);
                        }
                    };

                    if level == 7 {
                        key_tracker.push(node_key);
                        let full_key = key_tracker.to_usize_key();
                        let input_key =
                            unsafe { *(k.as_bytes().as_ptr() as *const usize) }.swap_bytes();
                        if full_key != input_key {
                            return Ok(None);
                        }

                        if parent_node.is_some() && node.as_ref().get_count() == 1 {
                            let mut write_p =
                                parent_node.unwrap().upgrade().map_err(|(_n, v)| v)?;

                            let mut write_n = node.upgrade().map_err(|(_n, v)| v)?;

                            write_p.as_mut().remove(parent_key);

                            write_n.mark_obsolete();
                            guard.defer(move || unsafe {
                                BaseNode::drop_node(write_n.as_mut());
                                std::mem::forget(write_n);
                            });
                        } else {
                            debug_assert!(parent_node.is_some());
                            let mut write_n = node.upgrade().map_err(|(_n, v)| v)?;

                            write_n.as_mut().remove(node_key);
                        }
                        return Ok(Some(next_node_tmp.as_tid()));
                    }
                    next_node = next_node_tmp.as_ptr();

                    level += 1;
                    key_tracker.push(node_key);
                }
            }
            parent_node = Some(node);
        }
    }

    #[inline]
    pub(crate) fn remove(&self, k: &T, guard: &Guard) -> Option<usize> {
        let backoff = Backoff::new();
        loop {
            match self.remove_inner(k, guard) {
                Ok(n) => return n,
                Err(_) => backoff.spin(),
            }
        }
    }

    #[inline]
    pub(crate) fn compute_if_present_inner<F>(
        &self,
        k: &T,
        remapping_function: F,
        _guard: &Guard,
    ) -> Result<Option<(usize, usize)>, ArtError>
    where
        F: FnOnce(usize) -> usize,
    {
        let mut parent_node;
        let mut level = 0;

        let mut node = unsafe { &*self.root }.base().read_lock()?;

        loop {
            level = if let Some(v) = Self::check_prefix(node.as_ref(), k, level) {
                v
            } else {
                return Ok(None);
            };

            if k.len() <= level as usize {
                return Ok(None);
            }

            parent_node = node;
            let child_node = parent_node.as_ref().get_child(k.as_bytes()[level as usize]);
            parent_node.check_version()?;

            let child_node = match child_node {
                Some(n) => n,
                None => return Ok(None),
            };

            if level == 7 {
                let tid = child_node.as_tid();
                let mut write_n = parent_node.upgrade().map_err(|(_n, v)| v)?;
                let new_v = remapping_function(tid);
                let old = write_n
                    .as_mut()
                    .change(k.as_bytes()[level as usize], NodePtr::from_tid(new_v));
                return Ok(Some((old.as_tid(), new_v)));
            }

            level += 1;

            node = unsafe { &*child_node.as_ptr() }.read_lock()?;
        }
    }

    #[inline]
    pub(crate) fn compute_if_present<F>(
        &self,
        k: &T,
        remapping_function: F,
        guard: &Guard,
    ) -> Option<(usize, usize)>
    where
        F: Fn(usize) -> usize,
    {
        let backoff = Backoff::new();
        loop {
            match self.compute_if_present_inner(k, &remapping_function, guard) {
                Ok(n) => return n,
                Err(_) => backoff.spin(),
            }
        }
    }

    #[inline]
    pub(crate) fn get_random(
        &self,
        rng: &mut impl rand::Rng,
        guard: &Guard,
    ) -> Option<(usize, usize)> {
        let backoff = Backoff::new();
        loop {
            match self.get_random_inner(rng, guard) {
                Ok(n) => return n,
                Err(_) => backoff.spin(),
            }
        }
    }

    #[inline]
    pub(crate) fn get_random_inner(
        &self,
        rng: &mut impl rand::Rng,
        _guard: &Guard,
    ) -> Result<Option<(usize, usize)>, ArtError> {
        let mut node = unsafe { &*self.root }.base().read_lock()?;

        let mut key_tracker = crate::utils::KeyTracker::default();

        loop {
            for k in node.as_ref().prefix() {
                key_tracker.push(*k);
            }

            let child_node = node.as_ref().get_random_child(rng);
            node.check_version()?;

            let (k, child_node) = match child_node {
                Some(n) => n,
                None => return Ok(None),
            };

            key_tracker.push(k);

            if key_tracker.len() == 8 {
                return Ok(Some((key_tracker.to_usize_key(), child_node.as_tid())));
            }

            node = unsafe { &*child_node.as_ptr() }.read_lock()?;
        }
    }
}
