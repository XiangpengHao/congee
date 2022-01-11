use crate::{base_node::BaseNode, key::Key};

enum PrefixCheckEqualsResult {
    BothMatch,
    Contained,
    NotMatch,
}

enum PrefixCompareResult {
    Smaller,
    Equal,
    Bigger,
}

pub(crate) struct RangeScan<'a, T: Key> {
    start: &'a T,
    end: &'a T,
    result: &'a mut [usize],
    root: *const BaseNode,
    to_continue: usize,
    result_found: usize,
}

#[derive(Default, Clone)]
struct KeyTracker {
    len: u8,
    data: [u8; 8],
}

impl KeyTracker {
    fn push(&mut self, key: u8) {
        debug_assert!(self.len <= 8);

        self.data[self.len as usize] = key;
        self.len += 1;
    }

    fn pop(&mut self) -> u8 {
        debug_assert!(self.len > 0);

        let v = self.data[self.len as usize - 1];
        self.len -= 1;
        v
    }

    fn to_usize_key(&self) -> usize {
        let val = unsafe { *((&self.data) as *const [u8; 8] as *const usize) };
        std::intrinsics::bswap(val)
    }
}

impl<'a, T: Key> RangeScan<'a, T> {
    pub(crate) fn new(
        start: &'a T,
        end: &'a T,
        result: &'a mut [usize],
        root: *const BaseNode,
    ) -> Self {
        Self {
            start,
            end,
            result,
            root,
            to_continue: 0,
            result_found: 0,
        }
    }

    fn is_valid_key_pair(&self) -> bool {
        self.start < self.end
    }

    fn key_in_range(&self, key: &KeyTracker) -> bool {
        debug_assert_eq!(key.len, 8);
        let cur_key = key.to_usize_key();

        let start_key =
            std::intrinsics::bswap(unsafe { *(self.start.as_bytes().as_ptr() as *const usize) });
        let end_key =
            std::intrinsics::bswap(unsafe { *(self.end.as_bytes().as_ptr() as *const usize) });

        if start_key <= cur_key && cur_key < end_key {
            return true;
        }
        false
    }

    pub(crate) fn scan(&mut self) -> Option<usize> {
        if !self.is_valid_key_pair() {
            return None;
        }

        'outer: loop {
            let mut level = 0;
            let mut node = std::ptr::null();
            let mut next_node = self.root;
            let mut parent_node: *const BaseNode;
            let mut v = 0;
            let mut vp;
            self.to_continue = 0;
            self.result_found = 0;

            let mut key_tracker = KeyTracker::default();

            loop {
                parent_node = node;
                vp = v;
                node = next_node;

                assert!(!BaseNode::is_leaf(node));

                v = match unsafe { &*node }.read_lock() {
                    Ok(v) => v,
                    Err(_) => continue 'outer,
                };

                let prefix_check_result =
                    match self.check_prefix_equals(unsafe { &*node }, &mut level, &mut key_tracker)
                    {
                        Ok(v) => v,
                        Err(_) => continue 'outer,
                    };

                if !parent_node.is_null() && unsafe { &*parent_node }.read_unlock(vp).is_err() {
                    continue 'outer;
                }

                if unsafe { &*node }.read_unlock(v).is_err() {
                    continue 'outer;
                }

                match prefix_check_result {
                    PrefixCheckEqualsResult::BothMatch => {
                        let start_level = if self.start.len() > level as usize {
                            self.start.as_bytes()[level as usize]
                        } else {
                            0
                        };
                        let end_level = if self.end.len() > level as usize {
                            self.end.as_bytes()[level as usize]
                        } else {
                            255
                        };

                        if start_level != end_level {
                            let (v, children) = if let Ok(val) =
                                BaseNode::get_children(unsafe { &*node }, start_level, end_level)
                            {
                                val
                            } else {
                                continue 'outer;
                            };

                            for (k, n) in children.iter() {
                                key_tracker.push(*k);
                                if *k == start_level {
                                    if self
                                        .find_start(*n, *k, level + 1, node, v, &mut key_tracker)
                                        .is_err()
                                    {
                                        continue 'outer;
                                    };
                                } else if *k > start_level && *k < end_level {
                                    if self.copy_node(*n, &mut key_tracker).is_err() {
                                        continue 'outer;
                                    };
                                } else if *k == end_level {
                                    if self
                                        .find_end(*n, *k, level + 1, node, v, &mut key_tracker)
                                        .is_err()
                                    {
                                        continue 'outer;
                                    }
                                }
                                key_tracker.pop();

                                if self.to_continue > 0 {
                                    continue 'outer;
                                }
                            }
                        } else {
                            next_node = BaseNode::get_child(start_level, unsafe { &*node })?;
                            if unsafe { &*node }.read_unlock(v).is_err() {
                                continue 'outer;
                            };

                            key_tracker.push(start_level);
                            if BaseNode::is_leaf(next_node) {
                                if self.copy_node(next_node, &mut key_tracker).is_err() {
                                    continue 'outer;
                                };
                                break;
                            }

                            level += 1;
                            continue;
                        }
                    }
                    PrefixCheckEqualsResult::Contained => {
                        if self.copy_node(node, &mut key_tracker).is_err() {
                            continue 'outer;
                        }
                    }
                    PrefixCheckEqualsResult::NotMatch => {
                        return None;
                    }
                }
                break;
            }

            if self.result_found > 0 {
                return Some(self.result_found);
            } else {
                return None;
            }
        }
    }

    fn find_end(
        &mut self,
        mut node: *const BaseNode,
        node_k: u8,
        mut level: u32,
        parent_node: *const BaseNode,
        mut vp: usize,
        key_tracker: &mut KeyTracker,
    ) -> Result<(), ()> {
        if BaseNode::is_leaf(node) {
            return self.copy_node(node, key_tracker);
        }

        let mut prefix_result;
        'outer: loop {
            let v = if let Ok(v) = unsafe { &*node }.read_lock() {
                v
            } else {
                continue;
            };

            prefix_result = if let Ok(r) =
                self.check_prefix_compare(unsafe { &*node }, self.end, 255, &mut level, key_tracker)
            {
                r
            } else {
                continue;
            };

            if unsafe { &*parent_node }.read_unlock(vp).is_err() {
                loop {
                    vp = if let Ok(v) = unsafe { &*parent_node }.read_lock() {
                        v
                    } else {
                        continue;
                    };

                    let node_tmp = BaseNode::get_child(node_k, unsafe { &*parent_node });

                    if unsafe { &*parent_node }.read_unlock(vp).is_err() {
                        continue;
                    }

                    node = if let Some(n) = node_tmp {
                        n
                    } else {
                        return Ok(());
                    };

                    if BaseNode::is_leaf(node) {
                        return Ok(());
                    }
                    continue 'outer;
                }
            }
            if unsafe { &*node }.read_unlock(v).is_err() {
                continue;
            };
            break;
        }

        match prefix_result {
            PrefixCompareResult::Bigger => Ok(()),
            PrefixCompareResult::Equal => {
                let end_level = if self.end.len() > level as usize {
                    self.end.as_bytes()[level as usize]
                } else {
                    255
                };

                let (v, children) = BaseNode::get_children(unsafe { &*node }, 0, end_level)?;
                for (k, n) in children.iter() {
                    key_tracker.push(*k);
                    if *k == end_level {
                        self.find_end(*n, *k, level + 1, node, v, key_tracker)?;
                    } else if *k < end_level {
                        self.copy_node(*n, key_tracker)?;
                    }
                    key_tracker.pop();
                    if self.to_continue != 0 {
                        break;
                    }
                }
                Ok(())
            }
            PrefixCompareResult::Smaller => self.copy_node(node, key_tracker),
        }
    }

    fn find_start(
        &mut self,
        mut node: *const BaseNode,
        node_k: u8,
        mut level: u32,
        parent_node: *const BaseNode,
        mut vp: usize,
        key_tracker: &mut KeyTracker,
    ) -> Result<(), ()> {
        if BaseNode::is_leaf(node) {
            return self.copy_node(node, key_tracker);
        }

        let mut prefix_result;
        'outer: loop {
            let v = if let Ok(v) = unsafe { &*node }.read_lock() {
                v
            } else {
                continue;
            };

            prefix_result = if let Ok(r) =
                self.check_prefix_compare(unsafe { &*node }, self.start, 0, &mut level, key_tracker)
            {
                r
            } else {
                continue;
            };

            if unsafe { &*parent_node }.read_unlock(vp).is_err() {
                loop {
                    vp = if let Ok(v) = unsafe { &*parent_node }.read_lock() {
                        v
                    } else {
                        continue;
                    };

                    let node_tmp = BaseNode::get_child(node_k, unsafe { &*parent_node });

                    if unsafe { &*parent_node }.read_unlock(vp).is_err() {
                        continue;
                    };

                    node = if let Some(n) = node_tmp {
                        n
                    } else {
                        return Ok(());
                    };

                    if BaseNode::is_leaf(node) {
                        return self.copy_node(node, key_tracker);
                    }
                    continue 'outer;
                }
            };
            if unsafe { &*node }.read_unlock(v).is_err() {
                continue;
            };
            break;
        }

        match prefix_result {
            PrefixCompareResult::Bigger => self.copy_node(node, key_tracker),
            PrefixCompareResult::Equal => {
                let start_level = if self.start.len() > level as usize {
                    self.start.as_bytes()[level as usize]
                } else {
                    0
                };
                let (v, children) = BaseNode::get_children(unsafe { &*node }, start_level, 255)?;

                for (k, n) in children.iter() {
                    key_tracker.push(*k);
                    if *k == start_level {
                        self.find_start(*n, *k, level + 1, node, v, key_tracker)?;
                    } else if *k > start_level {
                        self.copy_node(*n, key_tracker)?;
                    }
                    key_tracker.pop();
                    if self.to_continue != 0 {
                        break;
                    }
                }
                Ok(())
            }
            PrefixCompareResult::Smaller => Ok(()),
        }
    }

    fn copy_node(&mut self, node: *const BaseNode, key_tracker: &mut KeyTracker) -> Result<(), ()> {
        if BaseNode::is_leaf(node) {
            if self.key_in_range(key_tracker) {
                if self.result_found == self.result.len() {
                    self.to_continue = BaseNode::get_leaf(node);
                    return Ok(());
                }
                self.result[self.result_found] = BaseNode::get_leaf(node);
                self.result_found += 1;
            };
        } else {
            let (_v, children) = BaseNode::get_children(unsafe { &*node }, 0, 255)?;
            for (k, c) in children.iter() {
                let mut cur_key = if !BaseNode::is_leaf(*c) {
                    let c_ref = unsafe { &*(*c) };

                    let mut cur_key = key_tracker.clone();
                    cur_key.push(*k);
                    for i in 0..c_ref.get_prefix_len() as usize {
                        cur_key.push(c_ref.get_prefix()[i]);
                    }
                    cur_key
                } else {
                    let mut cur_key = key_tracker.clone();
                    cur_key.push(*k);
                    cur_key
                };

                self.copy_node(*c, &mut cur_key)?;

                if self.to_continue != 0 {
                    break;
                }
            }
        }
        Ok(())
    }

    fn check_prefix_compare(
        &self,
        n: &BaseNode,
        k: &T,
        fill_key: u8,
        level: &mut u32,
        key_tracker: &mut KeyTracker,
    ) -> Result<PrefixCompareResult, ()> {
        if n.has_prefix() {
            for i in 0..n.get_prefix_len() as usize {
                let k_level = if k.len() as u32 > *level {
                    k.as_bytes()[*level as usize]
                } else {
                    fill_key
                };

                let cur_key = n.get_prefix()[i];
                if cur_key < k_level {
                    return Ok(PrefixCompareResult::Smaller);
                } else if cur_key > k_level {
                    return Ok(PrefixCompareResult::Bigger);
                }
                *level += 1;
                key_tracker.push(cur_key);
            }
        }
        Ok(PrefixCompareResult::Equal)
    }

    fn check_prefix_equals(
        &self,
        n: &BaseNode,
        level: &mut u32,
        key_tracker: &mut KeyTracker,
    ) -> Result<PrefixCheckEqualsResult, ()> {
        if n.has_prefix() {
            for i in 0..n.get_prefix_len() as usize {
                let start_level = if self.start.len() as u32 > *level {
                    self.start.as_bytes()[*level as usize]
                } else {
                    0
                };

                let end_level = if self.end.len() as u32 > *level {
                    self.end.as_bytes()[*level as usize]
                } else {
                    255
                };

                let cur_key = n.get_prefix()[i as usize];

                if (cur_key == start_level) && (cur_key == end_level) {
                    *level += 1;
                    key_tracker.push(cur_key);
                    continue;
                } else if (cur_key >= start_level) && (cur_key <= end_level) {
                    key_tracker.push(cur_key);
                    for j in (i + 1)..n.get_prefix_len() as usize {
                        key_tracker.push(n.get_prefix()[j]);
                    }
                    return Ok(PrefixCheckEqualsResult::Contained);
                } else if cur_key < start_level || cur_key > end_level {
                    return Ok(PrefixCheckEqualsResult::NotMatch);
                }
            }
        }
        Ok(PrefixCheckEqualsResult::BothMatch)
    }
}
