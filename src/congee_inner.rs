use std::{marker::PhantomData, ptr::NonNull, sync::Arc};

use crossbeam_epoch::Guard;

use crate::{
    Allocator, DefaultAllocator, cast_ptr,
    error::{ArtError, OOMError},
    lock::ReadGuard,
    nodes::{BaseNode, ChildIsPayload, ChildIsSubNode, Node, Node4, NodePtr, Parent, NodeType},
    range_scan::RangeScan,
    utils::{Backoff, KeyTracker},
};

// use crate::nodes::NodeType;
use crate::congee_flat_generated::congee_flat::{Child, CongeeFlat, CongeeFlatArgs, NodeType as FbNodeType, finish_congee_flat_buffer};
use crate::congee_flat_struct_generated::congee_flat::{
    Child as StructChild, CongeeFlat as StructCongeeFlat, CongeeFlatArgs as StructCongeeFlatArgs, 
    Node as StructNode, NodeArgs as StructNodeArgs, NodeType as StructNodeType, 
    finish_congee_flat_buffer as finish_struct_congee_flat_buffer
};
use flatbuffers::FlatBufferBuilder;
use std::collections::VecDeque;

#[cfg(all(feature = "shuttle", test))]
use shuttle::sync::atomic::AtomicPtr;
#[cfg(not(all(feature = "shuttle", test)))]
use std::sync::atomic::AtomicPtr;

/// Raw interface to the ART tree.
/// The `Art` is a wrapper around the `RawArt` that provides a safe interface.
pub(crate) struct CongeeInner<
    const K_LEN: usize,
    A: Allocator + Clone + Send + 'static = DefaultAllocator, // Allocator must be clone because it is used in the epoch guard.
> {
    pub(crate) root: AtomicPtr<BaseNode>,
    drain_callback: Arc<dyn Fn([u8; K_LEN], usize)>,
    allocator: A,
    _pt_key: PhantomData<[u8; K_LEN]>,
}

unsafe impl<const K_LEN: usize, A: Allocator + Clone + Send> Send for CongeeInner<K_LEN, A> {}
unsafe impl<const K_LEN: usize, A: Allocator + Clone + Send> Sync for CongeeInner<K_LEN, A> {}

impl<const K_LEN: usize> Default for CongeeInner<K_LEN> {
    fn default() -> Self {
        Self::new(DefaultAllocator {}, Arc::new(|_: [u8; K_LEN], _: usize| {}))
    }
}

pub(crate) trait CongeeVisitor<const K_LEN: usize> {
    fn visit_payload(&mut self, _key: [u8; K_LEN], _payload: usize) {}
    fn pre_visit_sub_node(&mut self, _node: NonNull<BaseNode>, _tree_level: usize) {}
    fn post_visit_sub_node(&mut self, _node: NonNull<BaseNode>, _tree_level: usize) {}
}

struct DropVisitor<const K_LEN: usize, A: Allocator + Clone + Send> {
    allocator: A,
    drain_callback: Arc<dyn Fn([u8; K_LEN], usize)>,
}

impl<const K_LEN: usize, A: Allocator + Clone + Send> CongeeVisitor<K_LEN>
    for DropVisitor<K_LEN, A>
{
    fn visit_payload(&mut self, key: [u8; K_LEN], payload: usize) {
        (self.drain_callback)(key, payload);
    }

    fn post_visit_sub_node(&mut self, node: NonNull<BaseNode>, _tree_level: usize) {
        unsafe {
            BaseNode::drop_node(node, self.allocator.clone());
        }
    }
}

struct LeafNodeKeyVisitor<const K_LEN: usize> {
    keys: Vec<[u8; K_LEN]>,
}

impl<const K_LEN: usize> CongeeVisitor<K_LEN> for LeafNodeKeyVisitor<K_LEN> {
    fn visit_payload(&mut self, key: [u8; K_LEN], _payload: usize) {
        self.keys.push(key);
    }
}

struct ValueCountVisitor<const K_LEN: usize> {
    value_count: usize,
}

impl<const K_LEN: usize> CongeeVisitor<K_LEN> for ValueCountVisitor<K_LEN> {
    fn visit_payload(&mut self, _key: [u8; K_LEN], _payload: usize) {
        self.value_count += 1;
    }
}

impl<const K_LEN: usize, A: Allocator + Clone + Send> Drop for CongeeInner<K_LEN, A> {
    fn drop(&mut self) {
        let mut visitor = DropVisitor::<K_LEN, A> {
            allocator: self.allocator.clone(),
            drain_callback: self.drain_callback.clone(),
        };
        self.dfs_visitor_slow(&mut visitor).unwrap();

        // see this: https://github.com/XiangpengHao/congee/issues/20
        for _ in 0..128 {
            crossbeam_epoch::pin().flush();
        }
    }
}

impl<const K_LEN: usize, A: Allocator + Clone + Send> CongeeInner<K_LEN, A> {
    pub fn new(allocator: A, drain_callback: Arc<dyn Fn([u8; K_LEN], usize)>) -> Self {
        let root = BaseNode::make_node::<Node4, A>(&[], &allocator)
            .expect("Can't allocate memory for root node!");
        CongeeInner {
            root: AtomicPtr::new(root.into_non_null().cast::<BaseNode>().as_ptr()),
            drain_callback,
            allocator,
            _pt_key: PhantomData,
        }
    }

    #[inline]
    fn load_root(&self) -> NonNull<BaseNode> {
        let root_ptr = self.root.load(std::sync::atomic::Ordering::Relaxed);
        // SAFETY: The root pointer is always non-null after initialization.
        unsafe { NonNull::new_unchecked(root_ptr) }
    }
}

impl<const K_LEN: usize, A: Allocator + Clone + Send> CongeeInner<K_LEN, A> {
    pub(crate) fn is_empty(&self, _guard: &Guard) -> bool {
        loop {
            let root = self.load_root();
            if let Ok(node) = BaseNode::read_lock(root) {
                let is_empty = node.as_ref().meta.count() == 0;
                if node.check_version().is_ok() {
                    return is_empty;
                }
            }
        }
    }

    #[inline]
    pub(crate) fn get(&self, key: &[u8; K_LEN], _guard: &Guard) -> Option<usize> {
        'outer: loop {
            let mut level = 0;

            let root = self.load_root();
            let mut node = if let Ok(v) = BaseNode::read_lock(root) {
                v
            } else {
                continue;
            };

            loop {
                level = node.as_ref().check_prefix(key, level)?;

                let child_node = node
                    .as_ref()
                    .get_child(unsafe { *key.get_unchecked(level) });
                if node.check_version().is_err() {
                    continue 'outer;
                }

                let child_node = child_node?;

                cast_ptr!(child_node => {
                    Payload(tid) => {
                        return Some(tid);
                    },
                    SubNode(sub_node) => {
                        level += 1;

                        node = if let Ok(n) = BaseNode::read_lock(sub_node) {
                            n
                        } else {
                            continue 'outer;
                        };
                    }
                });
            }
        }
    }

    pub(crate) fn keys(&self) -> Vec<[u8; K_LEN]> {
        loop {
            let mut visitor = LeafNodeKeyVisitor::<K_LEN> { keys: Vec::new() };
            if self.dfs_visitor_slow(&mut visitor).is_ok() {
                return visitor.keys;
            }
        }
    }

    fn is_last_level<'a>(current_level: usize) -> Result<ChildIsPayload<'a>, ChildIsSubNode<'a>> {
        if current_level == (K_LEN - 1) {
            Ok(ChildIsPayload::new())
        } else {
            Err(ChildIsSubNode::new())
        }
    }

    /// Depth-First Search visitor implemented recursively, use with caution
    pub(crate) fn dfs_visitor_slow<V: CongeeVisitor<K_LEN>>(
        &self,
        visitor: &mut V,
    ) -> Result<(), ArtError> {
        let root = self.load_root();
        let mut key_tracker = KeyTracker::empty();
        Self::recursive_dfs(root, &mut key_tracker, 0, visitor)?;
        Ok(())
    }

    fn recursive_dfs<V: CongeeVisitor<K_LEN>>(
        node_ptr: NonNull<BaseNode>,
        key_tracker: &mut KeyTracker<K_LEN>,
        tree_level: usize,
        visitor: &mut V,
    ) -> Result<(), ArtError> {
        visitor.pre_visit_sub_node(node_ptr, tree_level);
        let node_lock = BaseNode::read_lock(node_ptr)?;

        // Add this node's prefix to the key tracker
        let node_prefix = node_lock.as_ref().prefix();
        let prefix_len = node_prefix.len();
        for &byte in node_prefix {
            key_tracker.push(byte);
        }

        let children = node_lock.as_ref().get_children(0, 255);
        for (k, child_ptr) in children {
            // Add the edge key to the tracker
            key_tracker.push(k);

            cast_ptr!(child_ptr => {
                Payload(tid) => {
                    // We've reached a leaf, construct the key from the tracker
                    let mut key: [u8; K_LEN] = [0; K_LEN];
                    let tracker_slice = key_tracker.as_slice();
                    let copy_len = tracker_slice.len().min(K_LEN);
                    key[..copy_len].copy_from_slice(&tracker_slice[..copy_len]);
                    visitor.visit_payload(key, tid);
                },
                SubNode(sub_node) => {
                    Self::recursive_dfs(sub_node, key_tracker, tree_level + 1, visitor)?;
                }
            });

            // Remove the edge key from the tracker
            key_tracker.pop();
        }

        // Remove this node's prefix from the tracker
        for _ in 0..prefix_len {
            key_tracker.pop();
        }

        node_lock.check_version()?;
        visitor.post_visit_sub_node(node_ptr, tree_level);
        Ok(())
    }

    /// Returns the number of values in the tree.
    pub(crate) fn value_count(&self, _guard: &Guard) -> usize {
        loop {
            let mut visitor = ValueCountVisitor::<K_LEN> { value_count: 0 };
            if self.dfs_visitor_slow(&mut visitor).is_ok() {
                return visitor.value_count;
            }
        }
    }

    #[inline]
    fn insert_inner<F>(
        &self,
        k: &[u8; K_LEN],
        tid_func: &mut F,
        guard: &Guard,
    ) -> Result<Option<usize>, ArtError>
    where
        F: FnMut(Option<usize>) -> usize,
    {
        let mut parent = Parent::Root(&self.root);
        let root = self.load_root();
        let mut node = BaseNode::read_lock(root)?;
        let mut node_key: u8;
        let mut level = 0usize;

        loop {
            let mut next_level = level;
            let res = node.as_ref().check_prefix_not_match(k, &mut next_level);
            match res {
                None => {
                    level = next_level;
                    node_key = k[level];

                    let next_node = node.as_ref().get_child(node_key);

                    node.check_version()?;

                    let next_node = if let Some(n) = next_node {
                        n
                    } else {
                        let new_leaf = {
                            match Self::is_last_level(level) {
                                Ok(_is_last_level) => NodePtr::from_payload(tid_func(None)),
                                Err(_is_sub_node) => {
                                    // Create a new node that will hold the remaining part of the key
                                    // The prefix should be the remaining bytes after current level
                                    let remaining_prefix = &k[level + 1..k.len() - 1];
                                    let mut n4 = BaseNode::make_node::<Node4, A>(
                                        remaining_prefix,
                                        &self.allocator,
                                    )?;
                                    n4.as_mut().insert(
                                        k[k.len() - 1],
                                        NodePtr::from_payload(tid_func(None)),
                                    );
                                    n4.into_note_ptr()
                                }
                            }
                        };

                        if let Err(e) = BaseNode::insert_and_unlock(
                            node,
                            parent,
                            (node_key, new_leaf),
                            &self.allocator,
                            guard,
                        ) {
                            cast_ptr!(new_leaf => {
                                Payload(_) => {},
                                SubNode(sub_node) => unsafe {
                                    BaseNode::drop_node(sub_node, self.allocator.clone());
                                }
                            });
                            return Err(e);
                        }

                        return Ok(None);
                    };

                    if let Parent::Node(_, p) = parent {
                        p.unlock()?;
                    }

                    cast_ptr!(next_node => {
                        Payload(old) => {
                            // At this point, the level must point to the last u8 of the key,
                            // meaning that we are updating an existing value.
                            let new = tid_func(Some(old));
                            if old == new {
                                node.check_version()?;
                                return Ok(Some(old));
                            }

                            let mut write_n = node.upgrade().map_err(|(_n, v)| v)?;

                            write_n
                                .as_mut()
                                .change(node_key, NodePtr::from_payload(new));
                            return Ok(Some(old));
                        },
                        SubNode(sub_node) => {
                            parent = Parent::Node(node_key, node);
                            node = BaseNode::read_lock(sub_node)?;
                            level += 1;
                        }
                    });
                }

                Some((no_match_key, prefix)) => {
                    let (parent_key, parent_node) = match parent {
                        Parent::Node(key, node) => (key, node),
                        Parent::Root(_) => {
                            unreachable!("Root node should not have a prefix");
                        }
                    };

                    let mut write_p = parent_node.upgrade().map_err(|(_n, v)| v)?;
                    let mut write_n = node.upgrade().map_err(|(_n, v)| v)?;

                    // 1) Create new node which will be parent of node, Set common prefix, level to this node
                    let mut new_middle_node = BaseNode::make_node::<Node4, A>(
                        &write_n.as_ref().prefix()[0..(next_level - level)],
                        &self.allocator,
                    )?;

                    // 2)  add node and (tid, *k) as children
                    if next_level == (K_LEN - 1) {
                        // this is the last key, just insert to node
                        new_middle_node
                            .as_mut()
                            .insert(k[next_level], NodePtr::from_payload(tid_func(None)));
                    } else {
                        // otherwise create a new node
                        let mut single_new_node = BaseNode::make_node::<Node4, A>(
                            &k[(next_level + 1)..k.len() - 1],
                            &self.allocator,
                        )?;

                        single_new_node
                            .as_mut()
                            .insert(k[k.len() - 1], NodePtr::from_payload(tid_func(None)));
                        new_middle_node
                            .as_mut()
                            .insert(k[next_level], single_new_node.into_note_ptr());
                    }

                    new_middle_node
                        .as_mut()
                        .insert(no_match_key, NodePtr::from_node_ref(write_n.as_mut()));

                    // 3) update parentNode to point to the new node, unlock
                    write_p
                        .as_mut()
                        .change(parent_key, new_middle_node.into_note_ptr());

                    let prefix_len = write_n.as_ref().prefix().len();
                    write_n
                        .as_mut()
                        .set_prefix(&prefix[0..(prefix_len - (next_level - level + 1))]);

                    return Ok(None);
                }
            }
        }
    }

    #[inline]
    pub(crate) fn insert(
        &self,
        k: &[u8; K_LEN],
        tid: usize,
        guard: &Guard,
    ) -> Result<Option<usize>, OOMError> {
        let backoff = Backoff::new();
        loop {
            match self.insert_inner(k, &mut |_| tid, guard) {
                Ok(v) => return Ok(v),
                Err(e) => match e {
                    ArtError::Locked | ArtError::VersionNotMatch => {
                        backoff.spin();
                        continue;
                    }
                    ArtError::Oom => return Err(OOMError::new()),
                },
            }
        }
    }

    #[inline]
    pub(crate) fn compute_or_insert<F>(
        &self,
        k: &[u8; K_LEN],
        insert_func: &mut F,
        guard: &Guard,
    ) -> Result<Option<usize>, OOMError>
    where
        F: FnMut(Option<usize>) -> usize,
    {
        let backoff = Backoff::new();
        loop {
            match self.insert_inner(k, insert_func, guard) {
                Ok(v) => return Ok(v),
                Err(e) => match e {
                    ArtError::Locked | ArtError::VersionNotMatch => {
                        backoff.spin();
                        continue;
                    }
                    ArtError::Oom => return Err(OOMError::new()),
                },
            }
        }
    }

    #[inline]
    pub(crate) fn range(
        &self,
        start: &[u8; K_LEN],
        end: &[u8; K_LEN],
        result: &mut [([u8; K_LEN], usize)],
        _guard: &Guard,
    ) -> usize {
        let root = self.load_root();
        let mut range_scan = RangeScan::new(start, end, result, root);

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

    #[inline]
    fn compute_if_present_inner<F>(
        &self,
        k: &[u8; K_LEN],
        remapping_function: &mut F,
        guard: &Guard,
    ) -> Result<Option<(usize, Option<usize>)>, ArtError>
    where
        F: FnMut(usize) -> Option<usize>,
    {
        let mut parent: Option<(ReadGuard, u8)> = None;
        let mut node_key: u8;
        let mut level = 0;
        let root = self.load_root();
        let mut node = BaseNode::read_lock(root)?;

        loop {
            level = if let Some(v) = node.as_ref().check_prefix(k, level) {
                v
            } else {
                return Ok(None);
            };

            node_key = k[level];

            let child_node = node.as_ref().get_child(node_key);
            node.check_version()?;

            let child_node = match child_node {
                Some(n) => n,
                None => return Ok(None),
            };

            cast_ptr!(child_node => {
                Payload(tid) => {
                    let new_v = remapping_function(tid);

                    match new_v {
                        Some(new_v) => {
                            if new_v == tid {
                                // the value is not change, early return;
                                return Ok(Some((tid, Some(tid))));
                            }
                            let mut write_n = node.upgrade().map_err(|(_n, v)| v)?;
                            write_n
                                .as_mut()
                                .change(k[level], NodePtr::from_payload(new_v));

                            return Ok(Some((tid, Some(new_v))));
                        }
                        None => {
                            // new value is none, we need to delete this entry
                            debug_assert!(parent.is_some()); // reaching leaf means we must have parent, bcs root can't be leaf
                            if node.as_ref().value_count() == 1 {
                                let (parent_node, parent_key) = parent.unwrap();
                                let mut write_p = parent_node.upgrade().map_err(|(_n, v)| v)?;

                                let mut write_n = node.upgrade().map_err(|(_n, v)| v)?;

                                write_p.as_mut().remove(parent_key);

                                write_n.mark_obsolete();
                                let allocator = self.allocator.clone();
                                guard.defer(move || unsafe {
                                    let ptr = NonNull::from(write_n.as_mut());
                                    std::mem::forget(write_n);
                                    BaseNode::drop_node(ptr, allocator);
                                });
                            } else {
                                let mut write_n = node.upgrade().map_err(|(_n, v)| v)?;

                                write_n.as_mut().remove(node_key);
                            }
                            return Ok(Some((tid, None)));
                        }
                    }
                },
                SubNode(sub_node) => {
                    level += 1;
                    parent = Some((node, node_key));
                    node = BaseNode::read_lock(sub_node)?;
                }
            });
        }
    }

    #[inline]
    pub(crate) fn compute_if_present<F>(
        &self,
        k: &[u8; K_LEN],
        remapping_function: &mut F,
        guard: &Guard,
    ) -> Option<(usize, Option<usize>)>
    where
        F: FnMut(usize) -> Option<usize>,
    {
        let backoff = Backoff::new();
        loop {
            match self.compute_if_present_inner(k, &mut *remapping_function, guard) {
                Ok(n) => return n,
                Err(_) => backoff.spin(),
            }
        }
    }

    pub(crate) fn allocator(&self) -> &A {
        &self.allocator
    }

    pub(crate) fn to_flatbuffer(&self) -> Vec<u8> {
        let mut bldr = FlatBufferBuilder::new();
        // let mut bytes: Vec<u8> = Vec::new();

        let mut fb_node_types = Vec::new();
        
        let mut fb_prefix: Vec<u8> = Vec::new();
        let mut fb_prefix_offsets: Vec<u32> = Vec::new();
        let mut fb_children = Vec::new();
        let mut fb_children_offsets: Vec<u32> = Vec::new();

        let mut queue = VecDeque::new();

        let root = self.load_root();

        queue.push_back(root);
        
        while !queue.is_empty() {
            let node = queue.front().cloned().unwrap();
            queue.pop_front();
            
            let node = BaseNode::read_lock(node).unwrap();

            let node_prefix = node.as_ref().prefix();
            fb_prefix.extend(node_prefix);
            if fb_prefix_offsets.is_empty(){
                fb_prefix_offsets.push(0);
            }
            else {
                // let node_prefix_len = u16::try_from(node_prefix.len()).unwrap();
                let node_prefix_len: u32  = u32::try_from(node_prefix.len()).unwrap();
                let new_offset: u32 = fb_prefix_offsets.last().copied().unwrap() + node_prefix_len;
                fb_prefix_offsets.push(new_offset);
            }

            // println!("fb_prefix: {:?}", fb_prefix);
            // println!("fb_prefix_offsets: {:?}", fb_prefix_offsets);

            let mut fb_child_index: u32 = u32::try_from(fb_children.len()).unwrap() + 1;
            let mut child_cnt = 0;

            let mut node_type;
            let mut is_node_type_set = 0;
            for (key, child_ptr) in node.as_ref().get_children(0, 255) {

                if is_node_type_set == 0 {
                    cast_ptr!(child_ptr => {
                        Payload(_) => {
                            if node.as_ref().get_type() == NodeType::N4 {
                                node_type = FbNodeType::N4_LEAF;
                            }
                            else if node.as_ref().get_type() == NodeType::N16 {
                                node_type = FbNodeType::N16_LEAF;
                            }
                            else if node.as_ref().get_type() == NodeType::N48 {
                                node_type = FbNodeType::N48_LEAF;
                            }
                            else {
                                node_type = FbNodeType::N256_LEAF;
                            }
                        },
                        SubNode(_) => {
                            if  node.as_ref().get_type() == NodeType::N4 {
                                node_type = FbNodeType::N4_INTERNAL;
                            }
                            else if node.as_ref().get_type() == NodeType::N16 {
                                node_type = FbNodeType::N16_INTERNAL;
                            }
                            else if node.as_ref().get_type() == NodeType::N48 {
                                node_type = FbNodeType::N48_INTERNAL;
                            }
                            else {
                                node_type = FbNodeType::N256_INTERNAL;
                            }
                        }
                    });

                    is_node_type_set = 1;
                    fb_node_types.push(node_type);
                }

                let fb_child;
                cast_ptr!(child_ptr => {
                    Payload(_payload) => {
                        fb_child = Child::new(key, 0);
                        // fb_child_index += 1;
                    },
                    SubNode(sub_node) => {
                        fb_child = Child::new(key, fb_child_index);
                        fb_child_index += 1;
                        // let child_node = BaseNode::read_lock(sub_node).unwrap();
                        queue.push_back(sub_node);
                    }
                });

                child_cnt += 1;
                fb_children.push(fb_child);
            }

            if fb_children_offsets.is_empty() {
                fb_children_offsets.push(child_cnt);
            }
            else {
                fb_children_offsets.push(fb_children_offsets[fb_children_offsets.len() - 1] + child_cnt);
            }

            // println!("node_types: {:?}", fb_node_types);
            // println!("fb_children: {:?}", fb_children);
            // println!("fb_children_offsets: {:?}", fb_children_offsets);
            // println!("node_types: {:?", fb_node_types);
        }

        let node_types_wip = bldr.create_vector(&fb_node_types);
        
        let prefix_wip = bldr.create_vector(&fb_prefix);
        let prefix_offsets_wip = bldr.create_vector(&fb_prefix_offsets);

        let children_wip = bldr.create_vector(&fb_children);
        let children_offsets_wip = bldr.create_vector(&fb_children_offsets);

        let congee_flat_args = CongeeFlatArgs {
            node_types: Some(node_types_wip),
            prefix_bytes: Some(prefix_wip),
            prefix_offsets: Some(prefix_offsets_wip),
            children_data: Some(children_wip),
            children_offsets: Some(children_offsets_wip),
        };
    
        let congee_flat_offset = CongeeFlat::create(&mut bldr, &congee_flat_args);
    
        finish_congee_flat_buffer(&mut bldr, congee_flat_offset);
    
        // Copy the serialized FlatBuffers data to our own byte buffer.
        let finished_data = bldr.finished_data();
        
        finished_data.into()
    }

    pub(crate) fn to_flatbuffer_struct(&self) -> Vec<u8> {
        let mut bldr = FlatBufferBuilder::new();
        let mut fb_nodes = Vec::new();
        let mut queue = VecDeque::new();

        let root = self.load_root();
        queue.push_back(root);
        
        let mut next_child_index = 1;
        while !queue.is_empty() {
            let node = queue.front().cloned().unwrap();
            queue.pop_front();
            
            let node_lock = BaseNode::read_lock(node).unwrap();
            let node_prefix = node_lock.as_ref().prefix();
            
            // Create prefix vector
            let prefix_vec = Some(bldr.create_vector(node_prefix));
            
            // Build children
            let mut fb_children = Vec::new();
            // let mut next_child_index = (fb_children.len() + 1) as u16; // Next available node index
            
            let mut node_type = StructNodeType::N4_INTERNAL;
            let mut is_node_type_set = false;
            
            for (key, child_ptr) in node_lock.as_ref().get_children(0, 255) {
                if !is_node_type_set {
                    cast_ptr!(child_ptr => {
                        Payload(_) => {
                            node_type = match node_lock.as_ref().get_type() {
                                NodeType::N4 => StructNodeType::N4_LEAF,
                                NodeType::N16 => StructNodeType::N16_LEAF,
                                NodeType::N48 => StructNodeType::N48_LEAF,
                                NodeType::N256 => StructNodeType::N256_LEAF,
                            };
                        },
                        SubNode(_) => {
                            node_type = match node_lock.as_ref().get_type() {
                                NodeType::N4 => StructNodeType::N4_INTERNAL,
                                NodeType::N16 => StructNodeType::N16_INTERNAL,
                                NodeType::N48 => StructNodeType::N48_INTERNAL,
                                NodeType::N256 => StructNodeType::N256_INTERNAL,
                            };
                        }
                    });
                    is_node_type_set = true;
                }

                cast_ptr!(child_ptr => {
                    Payload(_payload) => {
                        fb_children.push(StructChild::new(key, 0));
                    },
                    SubNode(sub_node) => {
                        fb_children.push(StructChild::new(key, next_child_index));
                        next_child_index += 1;
                        queue.push_back(sub_node);
                    }
                });
            }

            // Create children vector
            let children_vec = Some(bldr.create_vector(&fb_children));
            
            // Create node
            let node_args = StructNodeArgs {
                node_type,
                prefix: prefix_vec,
                children: children_vec,
            };
            let fb_node = StructNode::create(&mut bldr, &node_args);
            fb_nodes.push(fb_node);
        }

        // Create nodes vector
        let nodes_vec = bldr.create_vector(&fb_nodes);
        
        // Create root table
        let congee_flat_args = StructCongeeFlatArgs {
            nodes: Some(nodes_vec),
        };
        let congee_flat_offset = StructCongeeFlat::create(&mut bldr, &congee_flat_args);
        
        finish_struct_congee_flat_buffer(&mut bldr, congee_flat_offset);
        
        let finished_data = bldr.finished_data();
        finished_data.into()
    }

    pub(crate) fn to_compact(&self) -> Vec<u8> {
        use crate::congee_compact::NodeType as CompactNodeType;
        use std::collections::VecDeque;
        
        let mut node_types_arr = Vec::new();
        
        let mut prefix_arr: Vec<u8> = Vec::new();
        let mut prefix_offsets: Vec<u32> = Vec::new();
        // let mut children_arr = Vec::new();
        let mut children_keys_arr = Vec::new();
        let mut children_indices_arr = Vec::new();
        let mut children_offsets: Vec<u32> = Vec::new();

        let mut queue = VecDeque::new();

        let root = self.load_root();

        queue.push_back(root);
        
        while !queue.is_empty() {
            let node = queue.front().cloned().unwrap();
            queue.pop_front();
            
            let node = BaseNode::read_lock(node).unwrap();

            let node_prefix = node.as_ref().prefix();
            prefix_arr.extend(node_prefix);
            if prefix_offsets.is_empty(){
                prefix_offsets.push(0);
            }
            else {
                // let node_prefix_len = u16::try_from(node_prefix.len()).unwrap();
                let node_prefix_len: u32  = u32::try_from(node_prefix.len()).unwrap();
                let new_offset: u32 = prefix_offsets.last().copied().unwrap() + node_prefix_len;
                prefix_offsets.push(new_offset);
            }

            // println!("prefix_arr: {:?}", prefix_arr);
            // println!("prefix_offsets: {:?}", prefix_offsets);

            let mut next_child_index: u32 = u32::try_from(children_keys_arr.len()).unwrap() + 1;
            let mut child_cnt = 0;

            let mut node_type;
            let mut is_node_type_set = 0;
            for (key, child_ptr) in node.as_ref().get_children(0, 255) {

                if is_node_type_set == 0 {
                    cast_ptr!(child_ptr => {
                        Payload(_) => {
                            if node.as_ref().get_type() == NodeType::N4 {
                                node_type = CompactNodeType::N4_LEAF;
                            }
                            else if node.as_ref().get_type() == NodeType::N16 {
                                node_type = CompactNodeType::N16_LEAF;
                            }
                            else if node.as_ref().get_type() == NodeType::N48 {
                                node_type = CompactNodeType::N48_LEAF;
                            }
                            else {
                                node_type = CompactNodeType::N256_LEAF;
                            }
                        },
                        SubNode(_) => {
                            if  node.as_ref().get_type() == NodeType::N4 {
                                node_type = CompactNodeType::N4_INTERNAL;
                            }
                            else if node.as_ref().get_type() == NodeType::N16 {
                                node_type = CompactNodeType::N16_INTERNAL;
                            }
                            else if node.as_ref().get_type() == NodeType::N48 {
                                node_type = CompactNodeType::N48_INTERNAL;
                            }
                            else {
                                node_type = CompactNodeType::N256_INTERNAL;
                            }
                        }
                    });

                    is_node_type_set = 1;
                    node_types_arr.push(node_type);
                }

                cast_ptr!(child_ptr => {
                    Payload(_payload) => {
                        children_keys_arr.push(key);
                        children_indices_arr.push(0);
                        // child_data = Child::new(key, 0);
                        // next_child_index += 1;
                    },
                    SubNode(sub_node) => {
                        children_keys_arr.push(key);
                        children_indices_arr.push(next_child_index);
                        // child_data = Child::new(key, next_child_index);
                        next_child_index += 1;
                        // let child_node = BaseNode::read_lock(sub_node).unwrap();
                        queue.push_back(sub_node);
                    }
                });

                child_cnt += 1;
                // children_arr.push(fb_child);
            }

            if children_offsets.is_empty() {
                children_offsets.push(child_cnt);
            }
            else {
                children_offsets.push(children_offsets[children_offsets.len() - 1] + child_cnt);
            }

            // println!("node_types: {:?}", node_types_arr);
            // println!("children_arr: {:?}", children_arr);
            // println!("children_offsets: {:?}", children_offsets);
            // println!("node_types: {:?", node_types_arr);
        }
        
        // Create binary format
        let mut buf = Vec::new();
        
        // Header (32 bytes)
        // buf.extend_from_slice(&0x434F4D50414354u64.to_le_bytes()); // "COMPACT"
        buf.extend_from_slice(&(node_types_arr.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(prefix_arr.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(children_keys_arr.len() as u32).to_le_bytes());
        buf.extend_from_slice(&[0u8; 4]); // reserved
        
        // println!("node_types_arr: {:?}, len: {}", node_types_arr, node_types_arr.len());
        // println!("prefix_arr: {:?}, len: {}", prefix_arr, prefix_arr.len());
        // println!("prefix_offsets: {:?}, len: {}", prefix_offsets, prefix_offsets.len());
        // println!("children_keys_arr: {:?}, len: {}", children_keys_arr, children_keys_arr.len());
        // println!("children_indices_arr: {:?}, len: {}", children_indices_arr, children_indices_arr.len());
        // println!("children_offsets: {:?}, len: {}", children_offsets, children_offsets.len());
        
        // Data sections
        buf.extend_from_slice(&node_types_arr);
        
        // Convert u32 vectors to bytes
        for offset in &prefix_offsets {
            buf.extend_from_slice(&offset.to_le_bytes());
        }
        
        buf.extend_from_slice(&prefix_arr);

        for offset in &children_offsets {
            buf.extend_from_slice(&offset.to_le_bytes());
        }
        
        // buf.extend_from_slice(&prefix_arr);
        buf.extend_from_slice(&children_keys_arr);
        
        for index in &children_indices_arr {
            buf.extend_from_slice(&index.to_le_bytes());
        }
        
        buf
    }

    pub(crate) fn to_compact_v2(&self) -> Vec<u8> {
        use crate::congee_compact_v2::NodeType as CompactNodeType;
        use std::collections::VecDeque;
        
        let mut buf = Vec::new();
        let mut queue = VecDeque::new();

        let root = self.load_root();
        let node = BaseNode::read_lock(root).unwrap();
        
        // Check if tree is empty (root has no children)
        if node.as_ref().meta.count() == 0 {
            return buf; // Empty tree
        }
        
        drop(node); // Release the lock before continuing
        queue.push_back(root);
        
        // First pass: collect all nodes and assign indices
        let mut nodes_data = Vec::new();
        let mut node_counter = 0u32;
        
        while let Some(node_ptr) = queue.pop_front() {
            let node = BaseNode::read_lock(node_ptr).unwrap();
            let node_prefix = node.as_ref().prefix().to_vec();
            
            // Collect children data first to determine node type and count
            let mut children: Vec<(u8, Option<u32>)> = Vec::new();
            let mut is_leaf = false;
            
            for (key, child_ptr) in node.as_ref().get_children(0, 255) {
                cast_ptr!(child_ptr => {
                    Payload(_) => {
                        children.push((key, None)); // Leaf child, no node index
                        is_leaf = true;
                    },
                    SubNode(sub_node) => {
                        // Assign the next available node index
                        node_counter += 1;
                        children.push((key, Some(node_counter)));
                        queue.push_back(sub_node);
                    }
                });
            }
            
            // Determine node type based on base node type and whether it's a leaf
            let node_type = match (node.as_ref().get_type(), is_leaf) {
                (NodeType::N4, false) => CompactNodeType::N4_INTERNAL,
                (NodeType::N16, false) => CompactNodeType::N16_INTERNAL,
                (NodeType::N48, false) => CompactNodeType::N48_INTERNAL,
                (NodeType::N256, false) => CompactNodeType::N256_INTERNAL,
                (NodeType::N4, true) => CompactNodeType::N4_LEAF,
                (NodeType::N16, true) => CompactNodeType::N16_LEAF,
                (NodeType::N48, true) => CompactNodeType::N48_LEAF,
                (NodeType::N256, true) => CompactNodeType::N256_LEAF,
            };
            
            nodes_data.push((node_type, node_prefix, children, is_leaf));
        }
        
        // Calculate all node offsets first - CRITICAL: do this before writing any indices
        let mut node_offsets = Vec::new();
        let mut current_offset = 0usize;
        
        for (node_type, node_prefix, children, _is_leaf) in &nodes_data {
            node_offsets.push(current_offset);
            
            // Calculate node size: header + prefix + children
            let header_size = 4; // NodeHeader
            let prefix_size = node_prefix.len();
            let children_size = match *node_type {
                CompactNodeType::N48_INTERNAL => 256 + children.len() * 4, // key array + child indices
                CompactNodeType::N48_LEAF => 256, // presence array only
                CompactNodeType::N256_INTERNAL => 256 * 4, // direct node indices
                CompactNodeType::N256_LEAF => 256, // presence array
                CompactNodeType::N4_LEAF | CompactNodeType::N16_LEAF => children.len(), // keys only
                _ => children.len() * 5, // key + offset pairs
            };
            
            current_offset += header_size + prefix_size + children_size;
        }
        
        // Second pass: serialize all nodes, replacing indices with actual offsets
        for (node_index, (node_type, node_prefix, children, is_leaf)) in nodes_data.into_iter().enumerate() {
            // Write node header
            buf.push(node_type);
            buf.push(node_prefix.len() as u8);
            buf.extend_from_slice(&(children.len() as u16).to_le_bytes());
            
            // Write prefix
            buf.extend_from_slice(&node_prefix);
            
            // Write children based on node type, using offsets instead of indices
            match node_type {
                CompactNodeType::N48_INTERNAL => {
                    // N48 Internal: 256-byte key array + child offset array
                    let mut key_array = [0u8; 256]; // 0 means not present
                    let mut child_offsets = Vec::new();
                    
                    for (key, node_index_opt) in children {
                        key_array[key as usize] = (child_offsets.len() + 1) as u8; // 1-based index into child_offsets
                        let offset = if let Some(idx) = node_index_opt {
                            node_offsets[idx as usize] as u32
                        } else {
                            0 // Should not happen for internal nodes
                        };
                        child_offsets.push(offset);
                    }
                    
                    // Write key array (256 bytes)
                    buf.extend_from_slice(&key_array);
                    // Write child offsets (4 bytes each)
                    for &offset in &child_offsets {
                        buf.extend_from_slice(&offset.to_le_bytes());
                    }
                },
                CompactNodeType::N48_LEAF => {
                    // N48 Leaf: 256-byte presence array only
                    let mut presence_array = [0u8; 256];
                    
                    for (key, _) in children {
                        presence_array[key as usize] = 1; // 1 means key is present
                    }
                    
                    // Write presence array (256 bytes)
                    buf.extend_from_slice(&presence_array);
                },
                CompactNodeType::N256_INTERNAL => {
                    // N256 Internal: 256 x 4-byte direct node offsets
                    let mut direct_children = [0u32; 256];
                    
                    for (key, node_index_opt) in children {
                        let offset = if let Some(idx) = node_index_opt {
                            node_offsets[idx as usize] as u32
                        } else {
                            0 // Should not happen for internal nodes
                        };
                        direct_children[key as usize] = offset;
                    }
                    
                    // Write direct offsets (1024 bytes)
                    for &offset in &direct_children {
                        buf.extend_from_slice(&offset.to_le_bytes());
                    }
                },
                CompactNodeType::N256_LEAF => {
                    // N256 Leaf: 256-byte direct presence indicators
                    let mut presence_array = [0u8; 256];
                    
                    for (key, _) in children {
                        presence_array[key as usize] = 1; // 1 means key is present
                    }
                    
                    // Write presence array (256 bytes)
                    buf.extend_from_slice(&presence_array);
                },
                _ => {
                    // N4 and N16: key + offset pairs
                    for (key, node_index_opt) in children {
                        buf.push(key);
                        
                        // Only write offset for internal nodes
                        if !is_leaf {
                            let offset = if let Some(idx) = node_index_opt {
                                node_offsets[idx as usize] as u32
                            } else {
                                0 // Should not happen for internal nodes
                            };
                            buf.extend_from_slice(&offset.to_le_bytes());
                        }
                    }
                }
            }
        }
        
        buf
    }

}
