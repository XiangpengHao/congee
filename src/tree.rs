use std::mem::MaybeUninit;

use crate::{
    base_node::{BaseNode, Node, Prefix, MAX_STORED_PREFIX_LEN},
    key::{load_key, Key},
    node_256::Node256,
    node_4::Node4,
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

unsafe impl Send for Tree {}
unsafe impl Sync for Tree {}

impl Tree {
    pub fn new() -> Self {
        Tree {
            root: Node256::new(std::ptr::null(), 0) as *mut BaseNode,
        }
    }

    pub fn look_up(&self, key: &Key) -> Option<usize> {
        loop {
            let mut node = self.root;
            let mut parent_node: *mut BaseNode;

            let mut level = 0;
            let mut opt_prefix_match = false;

            let mut version = if let Ok(v) = unsafe { &*node }.read_lock_or_restart() {
                v
            } else {
                continue;
            };

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
                        return Self::check_key(tid, &key);
                    }
                    return Some(tid);
                }
                level += 1;

                let nv = if let Ok(nv) = unsafe { &*node }.read_lock_or_restart() {
                    nv
                } else {
                    break;
                };

                let need_restart = unsafe { &*parent_node }.read_unlock_or_restart(version);
                if need_restart {
                    break;
                }

                version = nv;
            }
        }
    }

    pub fn insert(&self, k: Key, tid: usize) {
        loop {
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

                let mut v = if let Ok(v) = unsafe { &*node }.read_lock_or_restart() {
                    v
                } else {
                    break;
                };

                let mut next_level = level;

                let res = self.check_prefix_pessimistic(unsafe { &*node }, &k, &mut next_level);
                match res {
                    CheckPrefixPessimisticResult::NeedRestart => {
                        break;
                    }
                    CheckPrefixPessimisticResult::Match => {
                        level = next_level;
                        node_key = k[level as usize];
                        let next_node_tmp = BaseNode::get_child(node_key, node);
                        let need_restart = unsafe { &*node }.check_or_restart(v);
                        if need_restart {
                            break;
                        }

                        next_node = if let Some(n) = next_node_tmp {
                            n
                        } else {
                            let need_restart = BaseNode::insert_and_unlock(
                                node,
                                v,
                                parent_node,
                                parent_version,
                                parent_key,
                                node_key,
                                BaseNode::set_leaf(tid),
                            );
                            if need_restart {
                                break;
                            }
                            return;
                        };

                        if !parent_node.is_null() {
                            if unsafe { &*parent_node }.read_unlock_or_restart(parent_version) {
                                break;
                            }
                        }

                        if BaseNode::is_leaf(next_node) {
                            if unsafe { &*node }
                                .upgrade_to_write_lock_or_restart(&mut v)
                                .is_err()
                            {
                                break;
                            };

                            let mut key = Key::new();
                            load_key(BaseNode::get_leaf(next_node), &mut key);

                            level += 1;
                            let mut prefix_len = 0;

                            while key[(level + prefix_len) as usize]
                                == k[(level + prefix_len) as usize]
                            {
                                prefix_len += 1;
                            }

                            let n4 = Node4::new(
                                unsafe { k.as_ptr().add(level as usize) },
                                prefix_len as usize,
                            );
                            let n4_ref = unsafe { &mut *n4 };
                            n4_ref
                                .insert(k[(level + prefix_len) as usize], BaseNode::set_leaf(tid));
                            n4_ref.insert(key[(level + prefix_len) as usize], next_node);
                            BaseNode::change(node, k[level as usize - 1], n4 as *mut BaseNode);
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
                            break;
                        }

                        if unsafe { &*node }
                            .upgrade_to_write_lock_or_restart(&mut v)
                            .is_err()
                        {
                            unsafe { &*parent_node }.write_unlock();
                            break;
                        };

                        // 1) Create new node which will be parent of node, Set common prefix, level to this node
                        let new_node = Node4::new(
                            unsafe { &*node }.get_prefix().as_ptr(),
                            (next_level - level) as usize,
                        );

                        // 2)  add node and (tid, *k) as children
                        unsafe { &mut *new_node }
                            .insert(k[next_level as usize], BaseNode::set_leaf(tid));
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
        level: &mut u32,
    ) -> CheckPrefixPessimisticResult {
        if n.has_prefix() {
            let pre_level = *level;
            let mut new_key = Key::new();
            for i in 0..n.get_prefix_len() {
                if i == MAX_STORED_PREFIX_LEN as u32 {
                    let any_tid = if let Ok(tid) = BaseNode::get_any_child_tid(n) {
                        tid
                    } else {
                        return CheckPrefixPessimisticResult::NeedRestart;
                    };
                    load_key(any_tid, &mut new_key);
                }
                let cur_key = n.get_prefix()[i as usize];
                if cur_key != key[*level as usize] {
                    let no_matching_key = cur_key;
                    if n.get_prefix_len() as usize > MAX_STORED_PREFIX_LEN {
                        if i < MAX_STORED_PREFIX_LEN as u32 {
                            let any_tid = if let Ok(tid) = BaseNode::get_any_child_tid(n) {
                                tid
                            } else {
                                return CheckPrefixPessimisticResult::NeedRestart;
                            };
                            load_key(any_tid, &mut new_key);
                            unsafe {
                                let mut prefix: Prefix = MaybeUninit::uninit().assume_init();
                                std::ptr::copy_nonoverlapping(
                                    new_key.as_ptr().add(*level as usize + 1),
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
                *level += 1;
            }
        }

        CheckPrefixPessimisticResult::Match
    }

    fn check_key(tid: usize, k: &Key) -> Option<usize> {
        let mut key = Key::new();
        load_key(tid, &mut key);
        if k == &key {
            return Some(tid);
        }
        None
    }
}
