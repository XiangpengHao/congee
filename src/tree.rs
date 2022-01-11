#![allow(clippy::uninit_assumed_init)]
use std::marker::PhantomData;

use crossbeam_epoch::Guard;

use crate::{
    base_node::{BaseNode, Node, NodeType, Prefix, MAX_STORED_PREFIX_LEN},
    key::Key,
    lock::ReadGuard,
    node_16::Node16,
    node_256::Node256,
    node_4::Node4,
    node_48::Node48,
    range_scan::RangeScan,
};

enum CheckPrefixResult {
    NotMatch,
    Match(u32),
    OptimisticMatch(u32),
}

enum CheckPrefixPessimisticResult {
    Match,
    NeedRestart,
    NotMatch((u8, Prefix)),
}

pub struct Tree<K: Key> {
    root: *mut BaseNode,
    _pt_key: PhantomData<K>,
}

impl<K: Key> Default for Tree<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Key> Drop for Tree<T> {
    fn drop(&mut self) {
        let mut sub_nodes = vec![self.root];

        while !sub_nodes.is_empty() {
            let node = sub_nodes.pop().unwrap();
            let children = match unsafe { &*node }.get_type() {
                NodeType::N4 => {
                    let n = node as *mut Node4;
                    let (_v, children) = unsafe { &*n }.get_children(0, 255).unwrap();
                    children
                }
                NodeType::N16 => {
                    let n = node as *mut Node16;
                    let (_v, children) = unsafe { &*n }.get_children(0, 255).unwrap();
                    children
                }
                NodeType::N48 => {
                    let n = node as *mut Node48;
                    let (_v, children) = unsafe { &*n }.get_children(0, 255).unwrap();
                    children
                }
                NodeType::N256 => {
                    let n = node as *mut Node256;
                    let (_v, children) = unsafe { &*n }.get_children(0, 255).unwrap();
                    children
                }
            };
            for (_k, n) in children.iter() {
                if !BaseNode::is_leaf(*n) {
                    sub_nodes.push(*n);
                }
            }
            BaseNode::destroy_node(node);
        }
    }
}

unsafe impl<T: Key> Send for Tree<T> {}
unsafe impl<T: Key> Sync for Tree<T> {}

impl<T: Key> Tree<T> {
    pub fn pin(&self) -> Guard {
        crossbeam_epoch::pin()
    }

    pub fn new() -> Self {
        Tree {
            root: Node256::new(std::ptr::null(), 0) as *mut BaseNode,
            _pt_key: PhantomData,
        }
    }
}

impl<T: Key> Tree<T> {
    pub fn get(&self, key: &T, _guard: &Guard) -> Option<usize> {
        'outer: loop {
            let mut parent_node;
            let mut level = 0;
            let mut opt_prefix_match = false;

            let mut node = if let Ok(v) = unsafe { &*self.root }.read_lock_n() {
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
                let child_node =
                    BaseNode::get_child(key.as_bytes()[level as usize], parent_node.as_ref());
                if ReadGuard::check_version(&parent_node).is_err() {
                    continue 'outer;
                }

                let child_node = child_node?;

                if BaseNode::is_leaf(child_node) {
                    let tid = BaseNode::get_leaf(child_node);
                    if (level as usize) < key.len() - 1 || opt_prefix_match {
                        return None;
                    }
                    return Some(tid);
                }
                level += 1;

                node = if let Ok(n) = unsafe { &*child_node }.read_lock_n() {
                    n
                } else {
                    continue 'outer;
                };
            }
        }
    }

    pub fn insert(&self, k: T, tid: usize, guard: &Guard) {
        'outer: loop {
            let mut node: *mut BaseNode = std::ptr::null_mut();
            let mut next_node = self.root;
            let mut parent_node: *mut BaseNode;

            let mut parent_key: u8;
            let mut node_key: u8 = 0;
            let mut parent_version = 0;
            let mut level = 0;

            loop {
                parent_node = node;
                parent_key = node_key;
                node = next_node;

                let mut v = if let Ok(v) = unsafe { &*node }.read_lock() {
                    v
                } else {
                    continue 'outer;
                };

                let mut next_level = level;

                let res = self.check_prefix_pessimistic(unsafe { &*node }, &k, &mut next_level);
                match res {
                    CheckPrefixPessimisticResult::NeedRestart => {
                        continue 'outer;
                    }
                    CheckPrefixPessimisticResult::Match => {
                        level = next_level;
                        node_key = k.as_bytes()[level as usize];
                        let next_node_tmp = BaseNode::get_child(node_key, unsafe { &*node });
                        if unsafe { &*node }.check_or_restart(v).is_err() {
                            continue 'outer;
                        }

                        next_node = if let Some(n) = next_node_tmp {
                            n
                        } else {
                            let new_leaf = {
                                if level as usize == k.len() - 1 {
                                    // last key, just insert the tid
                                    BaseNode::set_leaf(tid)
                                } else {
                                    let n4 = Node4::new(
                                        unsafe { k.as_bytes().as_ptr().add(level as usize + 1) },
                                        k.len() - level as usize - 2,
                                    );
                                    unsafe { &mut *n4 }
                                        .insert(k.as_bytes()[k.len() - 1], BaseNode::set_leaf(tid));
                                    n4 as *mut BaseNode
                                }
                            };

                            if BaseNode::insert_and_unlock(
                                node,
                                v,
                                parent_node,
                                parent_version,
                                parent_key,
                                node_key,
                                new_leaf,
                                guard,
                            )
                            .is_err()
                            {
                                continue 'outer;
                            }

                            return;
                        };

                        if !parent_node.is_null()
                            && unsafe { &*parent_node }
                                .read_unlock(parent_version)
                                .is_err()
                        {
                            continue 'outer;
                        }

                        if BaseNode::is_leaf(next_node) {
                            if unsafe { &*node }
                                .upgrade_to_write_lock_or_restart(&mut v)
                                .is_err()
                            {
                                continue 'outer;
                            };

                            // At this point, the level must point to the last u8 of the key,
                            // meaning that we are updating an existing value.
                            BaseNode::change(
                                node,
                                k.as_bytes()[level as usize],
                                BaseNode::set_leaf(tid),
                            );

                            unsafe { &*node }.write_unlock();
                            return;
                        }
                        level += 1;
                        parent_version = v;
                    }

                    CheckPrefixPessimisticResult::NotMatch((no_match_key, prefix)) => {
                        if unsafe { &*parent_node }
                            .upgrade_to_write_lock_or_restart(&mut parent_version)
                            .is_err()
                        {
                            continue 'outer;
                        }

                        if unsafe { &*node }
                            .upgrade_to_write_lock_or_restart(&mut v)
                            .is_err()
                        {
                            unsafe { &*parent_node }.write_unlock();
                            continue 'outer;
                        };

                        // 1) Create new node which will be parent of node, Set common prefix, level to this node
                        let new_node = Node4::new(
                            unsafe { &*node }.get_prefix().as_ptr(),
                            (next_level - level) as usize,
                        );

                        // 2)  add node and (tid, *k) as children
                        if next_level as usize == k.len() - 1 {
                            // this is the last key, just insert to node
                            unsafe { &mut *new_node }
                                .insert(k.as_bytes()[next_level as usize], BaseNode::set_leaf(tid));
                        } else {
                            // otherwise create a new node
                            let single_new_node = Node4::new(
                                unsafe { k.as_bytes().as_ptr().add(next_level as usize + 1) },
                                k.len() - next_level as usize - 2,
                            );
                            unsafe { &mut *single_new_node }
                                .insert(k.as_bytes()[k.len() - 1], BaseNode::set_leaf(tid));
                            unsafe { &mut *new_node }.insert(
                                k.as_bytes()[next_level as usize],
                                single_new_node as *mut BaseNode,
                            );
                        }

                        unsafe { &mut *new_node }.insert(no_match_key, node);

                        // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
                        BaseNode::change(parent_node, parent_key, new_node as *mut BaseNode);
                        unsafe { &*parent_node }.write_unlock();

                        // 4) update prefix of node, unlock
                        let mut_node = unsafe { &mut *node };
                        let prefix_len = mut_node.get_prefix_len();
                        mut_node.set_prefix(
                            prefix.as_ptr(),
                            (prefix_len - (next_level - level + 1)) as usize,
                        );
                        mut_node.write_unlock();
                        return;
                    }
                }
            }
        }
    }

    fn check_prefix(node: &BaseNode, key: &T, mut level: u32) -> CheckPrefixResult {
        if node.has_prefix() {
            if (key.len() as u32) <= level + node.get_prefix_len() {
                return CheckPrefixResult::NotMatch;
            }
            for i in 0..std::cmp::min(node.get_prefix_len(), MAX_STORED_PREFIX_LEN as u32) {
                if node.get_prefix()[i as usize] != key.as_bytes()[level as usize] {
                    return CheckPrefixResult::NotMatch;
                }
                level += 1;
            }

            if node.get_prefix_len() > MAX_STORED_PREFIX_LEN as u32 {
                level += node.get_prefix_len() - MAX_STORED_PREFIX_LEN as u32;
                return CheckPrefixResult::OptimisticMatch(level);
            }
        }
        CheckPrefixResult::Match(level)
    }

    fn check_prefix_pessimistic(
        &self,
        n: &BaseNode,
        key: &T,
        level: &mut u32,
    ) -> CheckPrefixPessimisticResult {
        if n.has_prefix() {
            let pre_level = *level;
            let mut new_key;
            for i in 0..n.get_prefix_len() {
                let cur_key = n.get_prefix()[i as usize];
                if cur_key != key.as_bytes()[*level as usize] {
                    let no_matching_key = cur_key;
                    if n.get_prefix_len() as usize > MAX_STORED_PREFIX_LEN {
                        if i < MAX_STORED_PREFIX_LEN as u32 {
                            let any_tid = if let Ok(tid) = BaseNode::get_any_child_tid(n) {
                                tid
                            } else {
                                return CheckPrefixPessimisticResult::NeedRestart;
                            };
                            new_key = T::key_from(any_tid);
                            unsafe {
                                let mut prefix: Prefix = Prefix::default();
                                std::ptr::copy_nonoverlapping(
                                    new_key.as_bytes().as_ptr().add(*level as usize + 1),
                                    prefix.as_mut_ptr(),
                                    std::cmp::min(
                                        (n.get_prefix_len() - (*level - pre_level) - 1) as usize,
                                        MAX_STORED_PREFIX_LEN,
                                    ),
                                )
                            }
                        }
                    } else {
                        let prefix = unsafe {
                            let mut prefix: Prefix = Prefix::default();
                            std::ptr::copy_nonoverlapping(
                                n.get_prefix().as_ptr().add(i as usize + 1),
                                prefix.as_mut_ptr(),
                                (n.get_prefix_len() - i - 1) as usize,
                            );
                            prefix
                        };
                        return CheckPrefixPessimisticResult::NotMatch((no_matching_key, prefix));
                    }
                }
                *level += 1;
            }
        }

        CheckPrefixPessimisticResult::Match
    }

    pub fn look_up_range(&self, start: &T, end: &T, result: &mut [usize]) -> Option<usize> {
        let mut range_scan = RangeScan::new(start, end, result, self.root);
        range_scan.scan()
    }
}
