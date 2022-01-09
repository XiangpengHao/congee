use crate::{
    base_node::{BaseNode, MAX_STORED_PREFIX_LEN},
    key::Key,
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

pub(crate) struct RangeScan<'a, T: Key> {
    start: &'a T,
    end: &'a T,
    result: &'a mut [usize],
    root: *const BaseNode,
    to_continue: usize,
    result_found: usize,
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

    fn key_in_range(&self, key: &T) -> bool {
        key >= self.start && key < self.end
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
                    match self.check_prefix_equals(unsafe { &*node }, &mut level) {
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
                            let (v, children) =
                                BaseNode::get_children(unsafe { &*node }, start_level, end_level);
                            for (k, n) in children.iter() {
                                if *k == start_level {
                                    self.find_start(*n, *k, level + 1, node, v);
                                } else if *k > start_level && *k < end_level {
                                    self.copy_node(*n);
                                } else if *k == end_level {
                                    self.find_end(*n, *k, level + 1, node, v);
                                }
                                if self.to_continue > 0 {
                                    continue 'outer;
                                }
                            }
                        } else {
                            next_node = BaseNode::get_child(start_level, unsafe { &*node })?;
                            if unsafe { &*node }.read_unlock(v).is_err() {
                                continue 'outer;
                            };

                            if BaseNode::is_leaf(next_node) {
                                self.copy_node(next_node);
                                break;
                            }

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
    ) {
        if BaseNode::is_leaf(node) {
            self.copy_node(node);
            return;
        }

        let mut prefix_result;
        'outer: loop {
            let v = if let Ok(v) = unsafe { &*node }.read_lock() {
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
                        return;
                    };

                    if BaseNode::is_leaf(node) {
                        return;
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
            PrefixCompareResult::Bigger => {}
            PrefixCompareResult::Equal => {
                let end_level = if self.end.len() > level as usize {
                    self.end.as_bytes()[level as usize]
                } else {
                    255
                };

                let (v, children) = BaseNode::get_children(unsafe { &*node }, 0, end_level);
                for (k, n) in children.iter() {
                    if *k == end_level {
                        self.find_end(*n, *k, level + 1, node, v);
                    } else if *k < end_level {
                        self.copy_node(*n);
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
            let v = if let Ok(v) = unsafe { &*node }.read_lock() {
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
                        return;
                    };

                    if BaseNode::is_leaf(node) {
                        self.copy_node(node);
                        return;
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
            PrefixCompareResult::Bigger => {
                self.copy_node(node);
            }
            PrefixCompareResult::Equal => {
                let start_level = if self.start.len() > level as usize {
                    self.start.as_bytes()[level as usize]
                } else {
                    0
                };
                let (v, children) = BaseNode::get_children(unsafe { &*node }, start_level, 255);

                for (k, n) in children.iter() {
                    if *k == start_level {
                        self.find_start(*n, *k, level + 1, node, v);
                    } else if *k > start_level {
                        self.copy_node(*n);
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
            let tid = BaseNode::get_leaf(node);
            let key = T::key_from(tid);
            if self.key_in_range(&key) {
                if self.result_found == self.result.len() {
                    self.to_continue = BaseNode::get_leaf(node);
                    return;
                }
                self.result[self.result_found] = BaseNode::get_leaf(node);
                self.result_found += 1;
            }
        } else {
            let (_v, children) = BaseNode::get_children(unsafe { &*node }, 0, 255);
            for c in children.iter() {
                self.copy_node((*c).1);
                if self.to_continue != 0 {
                    break;
                }
            }
        }
    }

    fn check_prefix_compare(
        &self,
        n: &BaseNode,
        k: &T,
        fill_key: u8,
        level: &mut u32,
    ) -> Result<PrefixCompareResult, ()> {
        if n.has_prefix() {
            let mut kt = T::default();
            for i in 0..n.get_prefix_len() as usize {
                if i == MAX_STORED_PREFIX_LEN {
                    let any_tid = BaseNode::get_any_child_tid(n)?;
                    kt = T::key_from(any_tid);
                }
                let k_level = if k.len() as u32 > *level {
                    k.as_bytes()[*level as usize]
                } else {
                    fill_key
                };

                let cur_key = if i >= MAX_STORED_PREFIX_LEN {
                    kt.as_bytes()[*level as usize]
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
            let mut kt = T::default();

            for i in 0..n.get_prefix_len() as usize {
                if i == MAX_STORED_PREFIX_LEN {
                    let tid = BaseNode::get_any_child_tid(n)?;
                    kt = T::key_from(tid);
                }

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

                let cur_key = if i >= MAX_STORED_PREFIX_LEN {
                    kt.as_bytes()[*level as usize]
                } else {
                    n.get_prefix()[i as usize]
                };

                if (cur_key == start_level) && (cur_key == end_level) {
                    *level += 1;
                    continue;
                } else if (cur_key >= start_level) && (cur_key <= end_level) {
                    return Ok(PrefixCheckEqualsResult::Contained);
                } else if cur_key < start_level || cur_key > end_level {
                    return Ok(PrefixCheckEqualsResult::NotMatch);
                }
            }
        }
        Ok(PrefixCheckEqualsResult::BothMatch)
    }
}
