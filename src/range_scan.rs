use crate::{
    base_node::{BaseNode, MAX_STORED_PREFIX_LEN},
    key::load_key,
    Key,
};

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

pub(crate) struct RangeScan<'a> {
    start: &'a Key,
    end: &'a Key,
    result: &'a mut [usize],
    root: *const BaseNode,
    to_continue: usize,
    result_found: usize,
}

impl<'a> RangeScan<'a> {
    pub(crate) fn new(
        start: &'a Key,
        end: &'a Key,
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

    pub(crate) fn scan(&mut self) -> Option<usize> {
        for i in 0..std::cmp::min(self.start.get_key_len(), self.end.get_key_len()) as usize {
            if self.start[i] > self.end[i] {
                return None;
            } else if self.start[i] < self.end[i] {
                break;
            }
        }

        loop {
            let mut level = 0;
            let mut node = std::ptr::null();
            let mut next_node = self.root;
            let mut parent_node: *const BaseNode;
            let mut v = 0;
            let mut vp;

            loop {
                parent_node = node;
                vp = v;
                node = next_node;

                v = match unsafe { &*node }.read_lock_or_restart() {
                    Ok(v) => v,
                    Err(_) => break,
                };

                let prefix_check_result =
                    match self.check_prefix_equals(unsafe { &*node }, &mut level) {
                        Ok(v) => v,
                        Err(_) => break,
                    };
                if !parent_node.is_null() {
                    if unsafe { &*parent_node }.read_unlock_or_restart(vp) {
                        break;
                    }
                }
                if unsafe { &*node }.read_unlock_or_restart(v) {
                    break;
                }

                match prefix_check_result {
                    PrefixCheckEqualsResult::BothMatch => {
                        let start_level = if self.start.get_key_len() > level {
                            self.start[level as usize]
                        } else {
                            0
                        };
                        let end_level = if self.end.get_key_len() > level {
                            self.end[level as usize]
                        } else {
                            0
                        };

                        if start_level != end_level {
                            let mut children = [(0, std::ptr::null_mut()); 256];
                            let (v, child_cnt) = BaseNode::get_children(
                                unsafe { &*node },
                                start_level,
                                end_level,
                                &mut children,
                            );
                            for i in 0..child_cnt {
                                let (k, n) = children[i];
                                if k == start_level {
                                    self.find_start(n, k, level + 1, node, v);
                                } else if k > start_level && k < end_level {
                                    self.copy_node(n);
                                } else if k == end_level {
                                    self.find_end(n, k, level + 1, node, v);
                                }
                                if self.to_continue > 0 {
                                    break;
                                }
                            }
                        } else {
                            next_node = BaseNode::get_child(start_level, node).unwrap();
                            if unsafe { &*node }.read_unlock_or_restart(v) {
                                break;
                            };
                            level += 1;
                            continue;
                        }
                    }
                    PrefixCheckEqualsResult::Contained => {
                        self.copy_node(node);
                    }
                    PrefixCheckEqualsResult::NotMatch => {
                        return None;
                    }
                }
            }

            if self.to_continue != 0 {
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
    ) {
        if BaseNode::is_leaf(node) {
            return;
        }

        let mut prefix_result;
        'outer: loop {
            let v = if let Ok(v) = unsafe { &*node }.read_lock_or_restart() {
                v
            } else {
                continue;
            };

            prefix_result = if let Ok(r) =
                self.check_prefix_compare(unsafe { &*node }, self.end, 255, &mut level)
            {
                r
            } else {
                continue;
            };

            if unsafe { &*parent_node }.read_unlock_or_restart(vp) {
                loop {
                    vp = if let Ok(v) = unsafe { &*parent_node }.read_lock_or_restart() {
                        v
                    } else {
                        continue;
                    };

                    let node_tmp = BaseNode::get_child(node_k, parent_node);

                    if unsafe { &*parent_node }.read_unlock_or_restart(vp) {
                        continue;
                    }

                    node = if let Some(n) = node_tmp {
                        n
                    } else {
                        return;
                    };

                    if BaseNode::is_leaf(node) {
                        return;
                    }
                    continue 'outer;
                }
            }
            if unsafe { &*node }.read_unlock_or_restart(v) {
                continue;
            };
            break;
        }

        match prefix_result {
            PrefixCompareResult::Bigger => {}
            PrefixCompareResult::Equal => {
                let end_level = if self.end.get_key_len() > level {
                    self.end[level as usize]
                } else {
                    255
                };

                let mut children = [(0, std::ptr::null_mut())];
                let (v, child_cnt) =
                    BaseNode::get_children(unsafe { &*node }, 0, end_level, &mut children);
                for i in 0..child_cnt {
                    let (k, n) = children[i];
                    if k == end_level {
                        self.find_end(n, k, level + 1, node, v);
                    } else if k < end_level {
                        self.copy_node(n);
                    }
                    if self.to_continue != 0 {
                        break;
                    }
                }
            }
            PrefixCompareResult::Smaller => {
                self.copy_node(node);
            }
        }
    }

    fn find_start(
        &mut self,
        mut node: *const BaseNode,
        node_k: u8,
        mut level: u32,
        parent_node: *const BaseNode,
        mut vp: usize,
    ) {
        if BaseNode::is_leaf(node) {
            self.copy_node(node);
            return;
        }

        let mut prefix_result;
        'outer: loop {
            let v = if let Ok(v) = unsafe { &*node }.read_lock_or_restart() {
                v
            } else {
                continue;
            };

            prefix_result = if let Ok(r) =
                self.check_prefix_compare(unsafe { &*node }, self.start, 0, &mut level)
            {
                r
            } else {
                continue;
            };

            if unsafe { &*parent_node }.read_unlock_or_restart(vp) {
                loop {
                    vp = if let Ok(v) = unsafe { &*parent_node }.read_lock_or_restart() {
                        v
                    } else {
                        continue;
                    };

                    let node_tmp = BaseNode::get_child(node_k, parent_node);

                    if unsafe { &*parent_node }.read_unlock_or_restart(vp) {
                        continue;
                    };

                    node = if let Some(n) = node_tmp {
                        n
                    } else {
                        return;
                    };

                    if BaseNode::is_leaf(node) {
                        self.copy_node(node);
                        return;
                    }
                    continue 'outer;
                }
            };
            if unsafe { &*node }.read_unlock_or_restart(v) {
                continue;
            };
            break;
        }

        match prefix_result {
            PrefixCompareResult::Bigger => {
                self.copy_node(node);
            }
            PrefixCompareResult::Equal => {
                let start_level = if self.start.get_key_len() > level {
                    self.start[level as usize]
                } else {
                    0
                };
                let mut children = [(0, std::ptr::null_mut()); 256];
                let (v, child_cnt) =
                    BaseNode::get_children(unsafe { &*node }, start_level, 255, &mut children);
                for i in 0..child_cnt {
                    let (k, n) = children[i];
                    if k == start_level {
                        self.find_start(n, k, level + 1, node, v);
                    } else if k > start_level {
                        self.copy_node(n);
                    }
                    if self.to_continue != 0 {
                        break;
                    }
                }
            }
            PrefixCompareResult::Smaller => {}
        }
    }

    fn copy_node(&mut self, node: *const BaseNode) {
        if BaseNode::is_leaf(node) {
            if self.result_found == self.result.len() {
                self.to_continue = BaseNode::get_leaf(node);
                return;
            }
            self.result[self.result_found] = BaseNode::get_leaf(node);
            self.result_found += 1;
        } else {
            let mut children = [(0, std::ptr::null_mut()); 256];
            let (_v, child_cnt) = BaseNode::get_children(unsafe { &*node }, 0, 255, &mut children);
            for i in 0..child_cnt {
                self.copy_node(children[i].1);
                if self.to_continue != 0 {
                    break;
                }
            }
        }
    }

    fn check_prefix_compare(
        &self,
        n: &BaseNode,
        k: &Key,
        fill_key: u8,
        level: &mut u32,
    ) -> Result<PrefixCompareResult, ()> {
        if n.has_prefix() {
            let mut kt = Key::new();
            for i in 0..n.get_prefix_len() as usize {
                if i == MAX_STORED_PREFIX_LEN {
                    let any_tid = BaseNode::get_any_child_tid(n)?;
                    load_key(any_tid, &mut kt);
                }
                let k_level = if k.get_key_len() > *level {
                    k[*level as usize]
                } else {
                    fill_key
                };

                let cur_key = if i >= MAX_STORED_PREFIX_LEN {
                    kt[*level as usize]
                } else {
                    n.get_prefix()[i]
                };
                if cur_key < k_level {
                    return Ok(PrefixCompareResult::Smaller);
                } else if cur_key > k_level {
                    return Ok(PrefixCompareResult::Bigger);
                }
                *level += 1;
            }
        }
        Ok(PrefixCompareResult::Equal)
    }

    fn check_prefix_equals(
        &self,
        n: &BaseNode,
        level: &mut u32,
    ) -> Result<PrefixCheckEqualsResult, ()> {
        if n.has_prefix() {
            let mut kt = Key::new();

            for i in 0..n.get_prefix_len() as usize {
                if i == MAX_STORED_PREFIX_LEN {
                    let tid = BaseNode::get_any_child_tid(n)?;
                    load_key(tid, &mut kt);
                }

                let start_level = if self.start.get_key_len() > *level {
                    self.start[*level as usize]
                } else {
                    0
                };

                let end_level = if self.end.get_key_len() > *level {
                    self.end[*level as usize]
                } else {
                    0
                };

                let cur_key = if i >= MAX_STORED_PREFIX_LEN {
                    kt[*level as usize]
                } else {
                    n.get_prefix()[i as usize]
                };

                if (cur_key > start_level) && (cur_key < end_level) {
                    return Ok(PrefixCheckEqualsResult::Contained);
                } else if cur_key < start_level || cur_key > end_level {
                    return Ok(PrefixCheckEqualsResult::NotMatch);
                }
                *level += 1;
            }
        }
        Ok(PrefixCheckEqualsResult::BothMatch)
    }
}
