use crate::{base_node::BaseNode, child_ptr::NodePtr, key::RawKey, lock::ReadGuard};

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

pub(crate) struct RangeScan<'a, T: RawKey> {
    start: &'a T,
    end: &'a T,
    result: &'a mut [usize],
    root: *const BaseNode,
    to_continue: usize,
    result_found: usize,
}

#[derive(Default, Clone)]
pub(crate) struct KeyTracker {
    len: usize,
    data: [u8; 8],
}

impl KeyTracker {
    #[inline]
    pub(crate) fn push(&mut self, key: u8) {
        debug_assert!(self.len <= 8);

        self.data[self.len as usize] = key;
        self.len += 1;
    }

    #[inline]
    pub(crate) fn pop(&mut self) -> u8 {
        debug_assert!(self.len > 0);

        let v = self.data[self.len as usize - 1];
        self.len -= 1;
        v
    }

    pub(crate) fn to_usize_key(&self) -> usize {
        assert!(self.len == 8);
        let val = unsafe { *((&self.data) as *const [u8; 8] as *const usize) };
        std::intrinsics::bswap(val)
    }

    pub(crate) fn append_prefix(node: NodePtr, key_tracker: &KeyTracker) -> KeyTracker {
        let mut cur_key = key_tracker.clone();
        if node.is_leaf() {
            cur_key
        } else {
            let node_ref = unsafe { &*node.as_ptr() };
            let n_prefix = node_ref.prefix();
            for i in n_prefix.iter() {
                cur_key.push(*i);
            }
            cur_key
        }
    }
}

impl<'a, T: RawKey> RangeScan<'a, T> {
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

    pub(crate) fn is_valid_key_pair(&self) -> bool {
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

    pub(crate) fn scan(&mut self) -> Result<Option<usize>, usize> {
        let mut level = 0;
        let mut node: ReadGuard;
        let mut next_node = self.root;
        let mut parent_node: Option<ReadGuard> = None;
        self.to_continue = 0;
        self.result_found = 0;

        let mut key_tracker = KeyTracker::default();

        'inner: loop {
            node = unsafe { &*next_node }.read_lock()?;

            let prefix_check_result =
                self.check_prefix_equals(node.as_ref(), &mut level, &mut key_tracker);

            if parent_node.is_some() {
                parent_node.as_ref().unwrap().check_version()?;
            }

            node.check_version()?;

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
                        let children = node.as_ref().get_children(start_level, end_level);
                        node.check_version()?;

                        for (k, n) in children.iter() {
                            key_tracker.push(*k);
                            if *k == start_level {
                                self.find_start(*n, level + 1, &node, key_tracker.clone())
                                    .map_err(|_| 0_usize)?;
                            } else if *k > start_level && *k < end_level {
                                let cur_key = KeyTracker::append_prefix(*n, &key_tracker);
                                self.copy_node(*n, &cur_key).map_err(|_| 0_usize)?;
                            } else if *k == end_level {
                                self.find_end(*n, level + 1, &node, key_tracker.clone())
                                    .map_err(|_| 0_usize)?;
                            }
                            key_tracker.pop();

                            if self.to_continue > 0 {
                                break 'inner;
                            }
                        }
                    } else {
                        let next_node_tmp = if let Some(n) = node.as_ref().get_child(start_level) {
                            n
                        } else {
                            return Ok(None);
                        };
                        node.check_version()?;

                        key_tracker.push(start_level);
                        if next_node_tmp.is_leaf() {
                            self.copy_node(next_node_tmp, &key_tracker)
                                .map_err(|_| 0_usize)?;
                            break;
                        }
                        next_node = next_node_tmp.as_ptr();

                        level += 1;
                        parent_node = Some(node);
                        continue;
                    }
                    break;
                }
                PrefixCheckEqualsResult::Contained => {
                    self.copy_node(NodePtr::from_node(node.as_ref()), &key_tracker)
                        .map_err(|_| 0_usize)?;
                }
                PrefixCheckEqualsResult::NotMatch => {
                    return Ok(None);
                }
            }
            break;
        }

        if self.result_found > 0 {
            Ok(Some(self.result_found))
        } else {
            Ok(None)
        }
    }

    fn find_end(
        &mut self,
        node: NodePtr,
        mut level: u32,
        parent_node: &ReadGuard,
        mut key_tracker: KeyTracker,
    ) -> Result<(), ()> {
        if node.is_leaf() {
            return self.copy_node(node, &key_tracker);
        }

        let node = unsafe { &*node.as_ptr() }.read_lock().map_err(|_| {})?;
        let prefix_result =
            self.check_prefix_compare(node.as_ref(), self.end, 255, &mut level, &mut key_tracker)?;

        parent_node.check_version().map_err(|_| {})?;
        node.check_version().map_err(|_| {})?;

        match prefix_result {
            PrefixCompareResult::Bigger => Ok(()),
            PrefixCompareResult::Equal => {
                let end_level = if self.end.len() > level as usize {
                    self.end.as_bytes()[level as usize]
                } else {
                    255
                };

                let children = node.as_ref().get_children(0, end_level);
                node.check_version().map_err(|_| ())?;
                for (k, n) in children.iter() {
                    key_tracker.push(*k);
                    if *k == end_level {
                        self.find_end(*n, level + 1, &node, key_tracker.clone())?;
                    } else if *k < end_level {
                        let cur_key = KeyTracker::append_prefix(*n, &key_tracker);
                        self.copy_node(*n, &cur_key)?;
                    }
                    key_tracker.pop();
                    if self.to_continue != 0 {
                        break;
                    }
                }
                Ok(())
            }
            PrefixCompareResult::Smaller => {
                self.copy_node(NodePtr::from_node(node.as_ref()), &key_tracker)
            }
        }
    }

    fn find_start(
        &mut self,
        node: NodePtr,
        mut level: u32,
        parent_node: &ReadGuard,
        mut key_tracker: KeyTracker,
    ) -> Result<(), ()> {
        if node.is_leaf() {
            return self.copy_node(node, &key_tracker);
        }

        let node = unsafe { &*node.as_ptr() }.read_lock().map_err(|_| {})?;
        let prefix_result =
            self.check_prefix_compare(node.as_ref(), self.start, 0, &mut level, &mut key_tracker)?;

        parent_node.check_version().map_err(|_| {})?;
        node.check_version().map_err(|_| {})?;

        match prefix_result {
            PrefixCompareResult::Bigger => {
                self.copy_node(NodePtr::from_node(node.as_ref()), &key_tracker)
            }
            PrefixCompareResult::Equal => {
                let start_level = if self.start.len() > level as usize {
                    self.start.as_bytes()[level as usize]
                } else {
                    0
                };
                let children = node.as_ref().get_children(start_level, 255);
                node.check_version().map_err(|_| ())?;

                for (k, n) in children.iter() {
                    key_tracker.push(*k);
                    if *k == start_level {
                        self.find_start(*n, level + 1, &node, key_tracker.clone())?;
                    } else if *k > start_level {
                        let cur_key = KeyTracker::append_prefix(*n, &key_tracker);
                        self.copy_node(*n, &cur_key)?;
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

    // FIXME: copy node should check parent version to make sure the node is not changed
    fn copy_node(&mut self, node: NodePtr, key_tracker: &KeyTracker) -> Result<(), ()> {
        if node.is_leaf() {
            if self.key_in_range(key_tracker) {
                if self.result_found == self.result.len() {
                    self.to_continue = node.as_tid();
                    return Ok(());
                }
                self.result[self.result_found] = node.as_tid();
                self.result_found += 1;
            };
        } else {
            let node = unsafe { &*node.as_ptr() }.read_lock().map_err(|_| ())?;
            let mut key_tracker = key_tracker.clone();

            let children = node.as_ref().get_children(0, 255);
            node.check_version().map_err(|_| ())?;

            for (k, c) in children.iter() {
                key_tracker.push(*k);

                let cur_key = KeyTracker::append_prefix(*c, &key_tracker);
                self.copy_node(*c, &cur_key)?;

                if self.to_continue != 0 {
                    break;
                }

                key_tracker.pop();
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
        let n_prefix = n.prefix();
        if !n_prefix.is_empty() {
            for (i, cur_key) in n_prefix.iter().enumerate() {
                let k_level = if k.len() as u32 > *level {
                    k.as_bytes()[*level as usize]
                } else {
                    fill_key
                };

                key_tracker.push(*cur_key);

                if *cur_key < k_level {
                    for v in n_prefix.iter().take(n_prefix.len()).skip(i + 1) {
                        key_tracker.push(*v);
                    }
                    return Ok(PrefixCompareResult::Smaller);
                } else if *cur_key > k_level {
                    for v in n_prefix.iter().take(n_prefix.len()).skip(i + 1) {
                        key_tracker.push(*v);
                    }
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
        key_tracker: &mut KeyTracker,
    ) -> PrefixCheckEqualsResult {
        let n_prefix = n.prefix();
        if !n_prefix.is_empty() {
            for (i, cur_key) in n_prefix.iter().enumerate() {
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

                if (*cur_key == start_level) && (*cur_key == end_level) {
                    *level += 1;
                    key_tracker.push(*cur_key);
                    continue;
                } else if (*cur_key >= start_level) && (*cur_key <= end_level) {
                    key_tracker.push(*cur_key);
                    for v in n_prefix.iter().take(n_prefix.len()).skip(i + 1) {
                        key_tracker.push(*v);
                    }
                    return PrefixCheckEqualsResult::Contained;
                } else if *cur_key < start_level || *cur_key > end_level {
                    return PrefixCheckEqualsResult::NotMatch;
                }
            }
        }
        PrefixCheckEqualsResult::BothMatch
    }
}
