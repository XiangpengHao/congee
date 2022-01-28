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
    range_scan::{KeyTracker, RangeScan},
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
        let mut sub_nodes = vec![self.root as *const BaseNode];

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
            unsafe {
                std::ptr::drop_in_place(node as *mut BaseNode);
            }
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
            root: Node256::new(&[]) as *mut BaseNode,
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
            let mut parent_node = None;
            let mut next_node = self.root as *const BaseNode;
            let mut parent_key: u8;
            let mut node_key: u8 = 0;
            let mut level = 0;

            let mut node;

            loop {
                parent_key = node_key;
                node = if let Ok(v) = unsafe { &*next_node }.read_lock_n() {
                    v
                } else {
                    continue 'outer;
                };

                let mut next_level = level;
                let res = self.check_prefix_pessimistic(node.as_ref(), &k, &mut next_level);
                match res {
                    CheckPrefixPessimisticResult::NeedRestart => {
                        continue 'outer;
                    }
                    CheckPrefixPessimisticResult::Match => {
                        level = next_level;
                        node_key = k.as_bytes()[level as usize];

                        let next_node_tmp = BaseNode::get_child(node_key, node.as_ref());

                        if node.check_version().is_err() {
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
                                    let new_prefix = k.as_bytes();
                                    let n4 =
                                        Node4::new(&new_prefix[level as usize + 1..k.len() - 1]);
                                    // let n4 = Node4::new(
                                    //     unsafe { k.as_bytes().as_ptr().add(level as usize + 1) },
                                    //     k.len() - level as usize - 2,
                                    // );
                                    unsafe { &mut *n4 }
                                        .insert(k.as_bytes()[k.len() - 1], BaseNode::set_leaf(tid));
                                    n4 as *mut BaseNode
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
                                        std::ptr::drop_in_place(new_leaf);
                                    }
                                }
                                continue 'outer;
                            }

                            return;
                        };

                        if let Some(p) = parent_node {
                            if p.unlock().is_err() {
                                continue 'outer;
                            }
                        }

                        if BaseNode::is_leaf(next_node) {
                            // At this point, the level must point to the last u8 of the key,
                            // meaning that we are updating an existing value.
                            let mut write_n = if let Ok(n) = node.upgrade_to_write_lock() {
                                n
                            } else {
                                continue 'outer;
                            };

                            BaseNode::change(
                                write_n.as_mut(),
                                k.as_bytes()[level as usize],
                                BaseNode::set_leaf(tid),
                            );

                            return;
                        }
                        level += 1;
                    }

                    CheckPrefixPessimisticResult::NotMatch((no_match_key, prefix)) => {
                        let mut write_p =
                            if let Ok(n) = parent_node.unwrap().upgrade_to_write_lock() {
                                n
                            } else {
                                continue 'outer;
                            };

                        let mut write_n = if let Ok(n) = node.upgrade_to_write_lock() {
                            n
                        } else {
                            continue 'outer;
                        };

                        // 1) Create new node which will be parent of node, Set common prefix, level to this node
                        let new_node = Node4::new(
                            &write_n.as_ref().get_prefix()[0..((next_level - level) as usize)],
                        );
                        // let new_node = Node4::new(
                        //     write_n.as_ref().get_prefix().as_ptr(),
                        //     (next_level - level) as usize,
                        // );

                        // 2)  add node and (tid, *k) as children
                        if next_level as usize == k.len() - 1 {
                            // this is the last key, just insert to node
                            unsafe { &mut *new_node }
                                .insert(k.as_bytes()[next_level as usize], BaseNode::set_leaf(tid));
                        } else {
                            // otherwise create a new node
                            let single_new_node =
                                Node4::new(&k.as_bytes()[(next_level as usize + 1)..k.len() - 1]);
                            // k.len() - next_level as usize - 2,
                            // let single_new_node = Node4::new(
                            //     unsafe { k.as_bytes().as_ptr().add(next_level as usize + 1) },
                            //     k.len() - next_level as usize - 2,
                            // );

                            unsafe { &mut *single_new_node }
                                .insert(k.as_bytes()[k.len() - 1], BaseNode::set_leaf(tid));
                            unsafe { &mut *new_node }.insert(
                                k.as_bytes()[next_level as usize],
                                single_new_node as *mut BaseNode,
                            );
                        }

                        unsafe { &mut *new_node }.insert(no_match_key, write_n.as_mut());

                        // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
                        BaseNode::change(write_p.as_mut(), parent_key, new_node as *mut BaseNode);

                        // 4) update prefix of node, unlock
                        let prefix_len = write_n.as_ref().get_prefix_len();
                        write_n.as_mut().set_prefix(
                            prefix.as_ptr(),
                            (prefix_len - (next_level - level + 1)) as usize,
                        );
                        return;
                    }
                }
                parent_node = Some(node);
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

    #[allow(clippy::unnecessary_unwrap)]
    pub fn remove(&self, k: &T, guard: &Guard) {
        'outer: loop {
            let mut next_node = self.root as *const BaseNode;
            let mut parent_node: Option<ReadGuard> = None;

            let mut parent_key: u8;
            let mut node_key: u8 = 0;
            let mut level = 0;
            let mut key_tracker = KeyTracker::default();

            let mut node;

            loop {
                parent_key = node_key;

                node = if let Ok(v) = unsafe { &*next_node }.read_lock_n() {
                    v
                } else {
                    continue 'outer;
                };

                match Self::check_prefix(node.as_ref(), k, level) {
                    CheckPrefixResult::NotMatch => {
                        return;
                    }
                    CheckPrefixResult::Match(l) | CheckPrefixResult::OptimisticMatch(l) => {
                        for i in level..l {
                            key_tracker.push(k.as_bytes()[i as usize]);
                        }
                        level = l;
                        node_key = k.as_bytes()[level as usize];

                        let next_node_tmp = BaseNode::get_child(node_key, node.as_ref());

                        if node.check_version().is_err() {
                            continue 'outer;
                        };

                        next_node = match next_node_tmp {
                            Some(n) => n,
                            None => {
                                return;
                            }
                        };

                        if BaseNode::is_leaf(next_node) {
                            key_tracker.push(node_key);
                            let full_key = key_tracker.to_usize_key();
                            let input_key = std::intrinsics::bswap(unsafe {
                                *(k.as_bytes().as_ptr() as *const usize)
                            });
                            if full_key != input_key {
                                return;
                            }

                            if parent_node.is_some() && node.as_ref().get_count() == 1 {
                                let mut write_p =
                                    if let Ok(p) = parent_node.unwrap().upgrade_to_write_lock() {
                                        p
                                    } else {
                                        continue 'outer;
                                    };

                                let mut write_n = if let Ok(n) = node.upgrade_to_write_lock() {
                                    n
                                } else {
                                    continue 'outer;
                                };

                                BaseNode::remove_key(write_p.as_mut(), parent_key);

                                write_n.mark_obsolete();
                                guard.defer(move || unsafe {
                                    std::ptr::drop_in_place(write_n.as_mut());
                                    std::mem::forget(write_n);
                                });
                            } else {
                                debug_assert!(parent_node.is_some());
                                let mut write_n = if let Ok(n) = node.upgrade_to_write_lock() {
                                    n
                                } else {
                                    continue 'outer;
                                };

                                BaseNode::remove_key(write_n.as_mut(), node_key);
                            }
                            return;
                        }
                        level += 1;
                        key_tracker.push(node_key);
                    }
                }
                parent_node = Some(node);
            }
        }
    }
}
