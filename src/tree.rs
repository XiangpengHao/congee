use std::{mem::MaybeUninit, ops::Add};

use crate::{
    base_node::{BaseNode, Prefix, MAX_STORED_PREFIX_LEN},
    key::Key,
    node_256::Node256,
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

pub struct Tree {
    root: *mut BaseNode,
}

impl Tree {
    pub fn new() -> Self {
        Tree {
            root: Node256::new(),
        }
    }

    pub fn look_up(&self, key: &Key) -> Option<usize> {
        loop {
            let mut node = self.root;
            let mut parent_node: *mut BaseNode;

            let mut level = 0;
            let mut opt_prefix_match = false;

            let (mut version, need_restart) = unsafe { &*node }.read_lock_or_restart();
            if need_restart {
                continue;
            }

            loop {
                match Self::check_prefix(unsafe { &*node }, &key, level) {
                    CheckPrefixResult::NotMatch => {
                        let need_restart = unsafe { &*node }.read_unlock_or_restart(version);
                        if need_restart {
                            break;
                        }
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
                if key.get_key_len() <= level {
                    return None;
                }

                parent_node = node;
                let child_node = BaseNode::get_child(key[level as usize], parent_node);
                let need_restart = unsafe { &*parent_node }.check_or_restart(version);
                if need_restart {
                    break;
                }

                node = child_node?;

                if BaseNode::is_leaf(node) {
                    let need_restart = unsafe { &*parent_node }.read_unlock_or_restart(version);
                    if need_restart {
                        break;
                    }
                    let tid = BaseNode::get_leaf(node);
                    if level < key.get_key_len() - 1 || opt_prefix_match {
                        return Some(Self::check_key(tid, &key));
                    }
                    return Some(tid);
                }
                level += 1;

                let (nv, need_restart) = unsafe { &*node }.read_lock_or_restart();
                if need_restart {
                    break;
                }

                let need_restart = unsafe { &*parent_node }.read_unlock_or_restart(version);
                if need_restart {
                    break;
                }

                version = nv;
            }
        }
    }
    pub fn look_up_range(&self, start: Key, end: Key) {}

    pub fn insert(&self, key: Key, tid: usize) {
        loop {
            let mut node: *mut BaseNode = std::ptr::null_mut();
            let mut next_node = self.root;
            let mut parent_node: *mut BaseNode;

            let mut parent_key: u8 = 0;
            let mut node_key: u8 = 0;
            let mut level = 0;

            loop {
                parent_node = node;
                parent_key = node_key;
                node = next_node;

                let (version, need_restart) = unsafe { &*node }.read_lock_or_restart();
                if need_restart {
                    break;
                }

                let next_level = level;
                let no_matching_key: u8;
                let remaining_prefix: Prefix;
            }
        }
    }

    fn check_prefix(node: &BaseNode, key: &Key, mut level: u32) -> CheckPrefixResult {
        if node.has_prefix() {
            if key.get_key_len() <= level + node.get_prefix_len() {
                return CheckPrefixResult::NotMatch;
            }
            for i in 0..std::cmp::min(node.get_prefix_len(), MAX_STORED_PREFIX_LEN as u32) {
                if node.get_prefix()[i as usize] != key[level as usize] {
                    return CheckPrefixResult::NotMatch;
                }
                level += 1;
            }

            if node.get_prefix_len() > MAX_STORED_PREFIX_LEN as u32 {
                level = level + (node.get_prefix_len() - MAX_STORED_PREFIX_LEN as u32);
                return CheckPrefixResult::OptimisticMatch(level);
            }
        }
        return CheckPrefixResult::Match(level);
    }

    fn check_prefix_pessimistic(
        &self,
        n: &BaseNode,
        key: &Key,
        level: u32,
    ) -> CheckPrefixPessimisticResult {
        if n.has_prefix() {
            let pre_level = level;
            let new_key = Key::new();
            for i in 0..n.get_prefix_len() {
                let cur_key = n.get_prefix()[i as usize];
                if cur_key != key[level as usize] {
                    let no_matching_key = cur_key;
                    if n.get_prefix_len() as usize > MAX_STORED_PREFIX_LEN {
                        if i < MAX_STORED_PREFIX_LEN as u32 {}
                    } else {
                        let prefix = unsafe {
                            let mut prefix: Prefix = MaybeUninit::uninit().assume_init();
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
            }
        }

        CheckPrefixPessimisticResult::Match
    }

    /// TODO: is this correct?
    fn check_key(tid: usize, _key: &Key) -> usize {
        tid
    }
}
