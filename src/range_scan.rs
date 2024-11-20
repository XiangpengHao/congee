use crate::error::ArtError;
use crate::node_256::Node256;
use crate::utils::LastLevelKey;
use crate::{base_node::BaseNode, lock::ReadGuard, node_ptr::NodePtr, utils::KeyTracker};
use std::cmp;
use std::ptr::NonNull;

enum PrefixCheckEqualsResult {
    BothMatch,
    Contained,
    NotMatch,
}

pub(crate) struct RangeScan<'a, const K_LEN: usize> {
    start: &'a [u8; K_LEN],
    end: &'a [u8; K_LEN],
    result: &'a mut [([u8; K_LEN], usize)],
    root: NonNull<Node256>,
    to_continue: usize,
    result_found: usize,
}

impl<'a, const K_LEN: usize> RangeScan<'a, K_LEN> {
    pub(crate) fn new(
        start: &'a [u8; K_LEN],
        end: &'a [u8; K_LEN],
        result: &'a mut [([u8; K_LEN], usize)],
        root: NonNull<Node256>,
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

    fn key_in_range(&self, key: &LastLevelKey) -> bool {
        let cur_key = key.to_usize_key();

        let start_key = unsafe { *(self.start.as_ptr() as *const usize) }.swap_bytes();
        let end_key = unsafe { *(self.end.as_ptr() as *const usize) }.swap_bytes();

        if start_key <= cur_key && cur_key < end_key {
            return true;
        }
        false
    }

    pub(crate) fn scan(&mut self) -> Result<usize, ArtError> {
        let mut node = BaseNode::read_lock_root(self.root)?;
        let mut parent_node: Option<ReadGuard> = None;
        self.to_continue = 0;
        self.result_found = 0;

        let mut key_tracker = KeyTracker::default();

        loop {
            let prefix_check_result = self.check_prefix_equals(node.as_ref(), &mut key_tracker);

            if parent_node.is_some() {
                parent_node.as_ref().unwrap().check_version()?;
            }

            node.check_version()?;

            match prefix_check_result {
                PrefixCheckEqualsResult::BothMatch => {
                    let level = key_tracker.len();
                    let start_level = if self.start.len() > level {
                        self.start[level]
                    } else {
                        0
                    };
                    let end_level = if self.end.len() > level {
                        self.end[level]
                    } else {
                        255
                    };

                    if start_level != end_level {
                        let children = node.as_ref().get_children(start_level, end_level);

                        for (k, n) in children {
                            node.check_version()?;

                            key_tracker.push(k);

                            if key_tracker.len() == K_LEN {
                                self.copy_node(n, &key_tracker)?;
                            } else if k == start_level {
                                self.find_start(n, &node, key_tracker.clone())?;
                            } else if k > start_level && k < end_level {
                                let cur_key = KeyTracker::append_prefix(n, &key_tracker);
                                self.copy_node(n, &cur_key)?;
                            } else if k == end_level {
                                self.find_end(n, &node, key_tracker.clone())?;
                            }
                            key_tracker.pop();

                            if self.to_continue > 0 {
                                return Ok(self.result_found);
                            }
                        }
                    } else {
                        let next_node_tmp = if let Some(n) = node.as_ref().get_child(start_level) {
                            n
                        } else {
                            return Ok(0);
                        };
                        node.check_version()?;

                        if key_tracker.len() == (K_LEN - 1) {
                            self.copy_node(next_node_tmp, &key_tracker)?;
                            return Ok(self.result_found);
                        }
                        key_tracker.push(start_level);

                        let next_node = BaseNode::read_lock_deprecated::<K_LEN>(next_node_tmp, level)?;
                        parent_node = Some(node);
                        node = next_node;
                        continue;
                    }
                    return Ok(self.result_found);
                }
                PrefixCheckEqualsResult::Contained => {
                    self.copy_node(NodePtr::from_node(node.as_ref()), &key_tracker)?;
                    return Ok(self.result_found);
                }
                PrefixCheckEqualsResult::NotMatch => {
                    return Ok(0);
                }
            }
        }
    }

    fn find_end(
        &mut self,
        node: NodePtr,
        parent_node: &ReadGuard,
        mut key_tracker: KeyTracker,
    ) -> Result<(), ArtError> {
        debug_assert!(key_tracker.len() != 8);

        let node = BaseNode::read_lock_deprecated::<K_LEN>(node, key_tracker.len())?;
        let prefix_result =
            self.check_prefix_compare(node.as_ref(), self.end, 255, &mut key_tracker);
        let level = key_tracker.len();

        parent_node.check_version()?;
        node.check_version()?;

        match prefix_result {
            cmp::Ordering::Greater => Ok(()),
            cmp::Ordering::Equal => {
                let end_level = if self.end.len() > level {
                    self.end[level]
                } else {
                    255
                };

                let children = node.as_ref().get_children(0, end_level);
                for (k, n) in children {
                    node.check_version()?;

                    key_tracker.push(k);

                    if key_tracker.len() == K_LEN {
                        self.copy_node(n, &key_tracker)?;
                    } else if k == end_level {
                        self.find_end(n, &node, key_tracker.clone())?;
                    } else if k < end_level {
                        let cur_key = KeyTracker::append_prefix(n, &key_tracker);
                        self.copy_node(n, &cur_key)?;
                    }
                    key_tracker.pop();
                    if self.to_continue != 0 {
                        break;
                    }
                }
                Ok(())
            }
            cmp::Ordering::Less => self.copy_node(NodePtr::from_node(node.as_ref()), &key_tracker),
        }
    }

    fn find_start(
        &mut self,
        node: NodePtr,
        parent_node: &ReadGuard,
        mut key_tracker: KeyTracker,
    ) -> Result<(), ArtError> {
        debug_assert!(key_tracker.len() != 8);

        let node = BaseNode::read_lock_deprecated::<K_LEN>(node, key_tracker.len())?;
        let prefix_result =
            self.check_prefix_compare(node.as_ref(), self.start, 0, &mut key_tracker);

        parent_node.check_version()?;
        node.check_version()?;

        match prefix_result {
            cmp::Ordering::Greater => {
                self.copy_node(NodePtr::from_node(node.as_ref()), &key_tracker)
            }
            cmp::Ordering::Equal => {
                let start_level = if self.start.len() > key_tracker.len() {
                    self.start[key_tracker.len()]
                } else {
                    0
                };

                let children = node.as_ref().get_children(start_level, 255);

                for (k, n) in children {
                    node.check_version()?;

                    key_tracker.push(k);
                    if key_tracker.len() == K_LEN {
                        self.copy_node(n, &key_tracker)?;
                    } else if k == start_level {
                        self.find_start(n, &node, key_tracker.clone())?;
                    } else if k > start_level {
                        let cur_key = KeyTracker::append_prefix(n, &key_tracker);
                        self.copy_node(n, &cur_key)?;
                    }
                    key_tracker.pop();
                    if self.to_continue != 0 {
                        break;
                    }
                }
                Ok(())
            }
            cmp::Ordering::Less => Ok(()),
        }
    }

    fn copy_node(&mut self, node: NodePtr, key_tracker: &KeyTracker) -> Result<(), ArtError> {
        if let Some(last_level_key) = key_tracker.as_last_level::<K_LEN>() {
            if self.key_in_range(&last_level_key) {
                if self.result_found == self.result.len() {
                    self.to_continue = node.as_payload(&last_level_key);
                    return Ok(());
                }
                self.result[self.result_found] =
                    (key_tracker.get_key(), node.as_payload(&last_level_key));
                self.result_found += 1;
            };
        } else {
            let node = BaseNode::read_lock_deprecated::<K_LEN>(node, key_tracker.len())?;
            let mut key_tracker = key_tracker.clone();

            let children = node.as_ref().get_children(0, 255);

            for (k, c) in children {
                node.check_version()?;

                key_tracker.push(k);

                let cur_key = KeyTracker::append_prefix(c, &key_tracker);
                self.copy_node(c, &cur_key)?;

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
        k: &[u8; K_LEN],
        fill_key: u8,
        key_tracker: &mut KeyTracker,
    ) -> cmp::Ordering {
        let n_prefix = n.prefix();
        if !n_prefix.is_empty() {
            let skip_len = key_tracker.len();
            for (i, cur_key) in n_prefix.iter().skip(skip_len).enumerate() {
                let k_level = if k.len() > key_tracker.len() {
                    k[key_tracker.len()]
                } else {
                    fill_key
                };

                key_tracker.push(*cur_key);

                if *cur_key < k_level {
                    for v in n_prefix
                        .iter()
                        .skip(skip_len)
                        .take(n_prefix.len() - skip_len)
                        .skip(i + 1)
                    {
                        key_tracker.push(*v);
                    }
                    return cmp::Ordering::Less;
                } else if *cur_key > k_level {
                    for v in n_prefix
                        .iter()
                        .skip(skip_len)
                        .take(n_prefix.len() - skip_len)
                        .skip(i + 1)
                    {
                        key_tracker.push(*v);
                    }
                    return cmp::Ordering::Greater;
                }
            }
        }
        cmp::Ordering::Equal
    }

    fn check_prefix_equals(
        &self,
        n: &BaseNode,
        key_tracker: &mut KeyTracker,
    ) -> PrefixCheckEqualsResult {
        let n_prefix = n.prefix();

        if !n_prefix.is_empty() {
            let skip_len = key_tracker.len();
            for (i, cur_key) in n_prefix.iter().skip(skip_len).enumerate() {
                let level = key_tracker.len();
                let start_level = if self.start.len() > level {
                    self.start[level]
                } else {
                    0
                };

                let end_level = if self.end.len() > level {
                    self.end[level]
                } else {
                    255
                };

                if (*cur_key == start_level) && (*cur_key == end_level) {
                    key_tracker.push(*cur_key);
                    continue;
                } else if (*cur_key >= start_level) && (*cur_key <= end_level) {
                    key_tracker.push(*cur_key);
                    for v in n_prefix
                        .iter()
                        .skip(skip_len)
                        .take(n_prefix.len() - skip_len)
                        .skip(i + 1)
                    {
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
