#[cfg(all(feature = "shuttle", test))]
use shuttle::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::ptr::NonNull;
#[cfg(not(all(feature = "shuttle", test)))]
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crossbeam_epoch::Guard;

use crate::{
    Allocator,
    error::ArtError,
    lock::{ReadGuard, TypedReadGuard},
    nodes::{
        AllocatedNode, NodePtr,
        node_4::{Node4, Node4Iter},
        node_16::{Node16, Node16Iter},
        node_48::{Node48, Node48Iter},
        node_256::{Node256, Node256Iter},
    },
};

pub(crate) const MAX_KEY_LEN: usize = 8;

/// Prefix has to be 8 bytes for better alignment.
const MAX_PREFIX_LEN: usize = 8;
pub(crate) type Prefix = [u8; MAX_PREFIX_LEN];

/// Represents either a normal parent node or the root of the tree
pub(crate) enum Parent<'a> {
    Node(u8, ReadGuard<'a>),
    Root(&'a AtomicPtr<BaseNode>),
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum NodeType {
    N4 = 0,
    N16 = 1,
    N48 = 2,
    N256 = 3,
}

impl NodeType {
    pub(crate) fn node_layout(&self) -> std::alloc::Layout {
        match *self {
            NodeType::N4 => std::alloc::Layout::from_size_align(
                std::mem::size_of::<Node4>(),
                std::mem::align_of::<Node4>(),
            )
            .unwrap(),
            NodeType::N16 => std::alloc::Layout::from_size_align(
                std::mem::size_of::<Node16>(),
                std::mem::align_of::<Node16>(),
            )
            .unwrap(),
            NodeType::N48 => std::alloc::Layout::from_size_align(
                std::mem::size_of::<Node48>(),
                std::mem::align_of::<Node48>(),
            )
            .unwrap(),
            NodeType::N256 => std::alloc::Layout::from_size_align(
                std::mem::size_of::<Node256>(),
                std::mem::align_of::<Node256>(),
            )
            .unwrap(),
        }
    }
}

pub(crate) trait Node {
    fn base(&self) -> &BaseNode;
    fn is_full(&self) -> bool;
    fn insert(&mut self, key: u8, node: NodePtr);
    fn change(&mut self, key: u8, val: NodePtr) -> NodePtr;
    fn get_child(&self, key: u8) -> Option<NodePtr>;
    fn get_children(&self, start: u8, end: u8) -> NodeIter<'_>;
    fn remove(&mut self, k: u8);
    fn copy_to<N: Node>(&self, dst: &mut N);
    fn get_type() -> NodeType;
}

pub(crate) enum NodeIter<'a> {
    N4(Node4Iter<'a>),
    N16(Node16Iter<'a>),
    N48(Node48Iter<'a>),
    N256(Node256Iter<'a>),
}

impl Iterator for NodeIter<'_> {
    type Item = (u8, NodePtr);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            NodeIter::N4(iter) => iter.next(),
            NodeIter::N16(iter) => iter.next(),
            NodeIter::N48(iter) => iter.next(),
            NodeIter::N256(iter) => iter.next(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct BaseNode {
    // 2b type | 60b version | 1b lock | 1b obsolete
    pub(crate) type_version_lock_obsolete: AtomicUsize,
    pub(crate) meta: NodeMeta,
}

#[derive(Debug)]
pub(crate) struct NodeMeta {
    prefix_cnt: u32,
    pub(crate) count: u16,
    node_type: NodeType,
    prefix: Prefix,
}

#[cfg(not(feature = "shuttle"))]
mod layout_assertion {
    use super::*;
    const _: () = assert!(std::mem::size_of::<NodeMeta>() == 16);
    const _: () = assert!(std::mem::align_of::<NodeMeta>() == 4);
    const _: () = assert!(std::mem::size_of::<BaseNode>() == 24);
    const _: () = assert!(std::mem::align_of::<BaseNode>() == 8);
}

macro_rules! gen_method {
    ($method_name:ident, ($($arg_n:ident : $args:ty),*), $return:ty) => {
        impl BaseNode {
            pub(crate) fn $method_name(&self, $($arg_n : $args),*) -> $return {
                match self.get_type() {
                    NodeType::N4 => {
                        let node = self.as_n4();
                        node.$method_name($($arg_n),*)
                    },
                    NodeType::N16 => {
                        let node = self.as_n16();
                        node.$method_name($($arg_n),*)
                    },
                    NodeType::N48 => {
                        let node = self.as_n48();
                        node.$method_name($($arg_n),*)
                    },
                    NodeType::N256 => {
                        let node = self.as_n256();
                        node.$method_name($($arg_n),*)
                    },
                }
            }
        }
    };
}

macro_rules! gen_method_mut {
    ($method_name:ident, ($($arg_n:ident : $args:ty),*), $return:ty) => {
        impl BaseNode {
            pub(crate) fn $method_name(&mut self, $($arg_n : $args),*) -> $return {
                match self.get_type() {
                    NodeType::N4 => {
                        let node = self.as_n4_mut();
                        node.$method_name($($arg_n),*)
                    },
                    NodeType::N16 => {
                        let node = self.as_n16_mut();
                        node.$method_name($($arg_n),*)
                    },
                    NodeType::N48 => {
                        let node = self.as_n48_mut();
                        node.$method_name($($arg_n),*)
                    },
                    NodeType::N256 => {
                        let node = self.as_n256_mut();
                        node.$method_name($($arg_n),*)
                    },
                }
            }
        }
    };
}

gen_method!(get_child, (k: u8), Option<NodePtr>);
gen_method!(get_children, (start: u8, end: u8), NodeIter<'_>);

gen_method_mut!(change, (key: u8, val: NodePtr), NodePtr);
gen_method_mut!(remove, (key: u8), ());

impl BaseNode {
    pub(crate) fn new(n_type: NodeType, prefix: &[u8]) -> Self {
        let mut prefix_v: [u8; MAX_KEY_LEN] = [0; MAX_KEY_LEN];

        assert!(prefix.len() <= MAX_KEY_LEN);
        for (i, v) in prefix.iter().enumerate() {
            prefix_v[i] = *v;
        }

        let meta = NodeMeta {
            prefix_cnt: prefix.len() as u32,
            count: 0,
            prefix: prefix_v,
            node_type: n_type,
        };

        BaseNode {
            type_version_lock_obsolete: AtomicUsize::new(0),
            meta,
        }
    }

    pub(crate) fn make_node<'a, N: Node, A: Allocator>(
        prefix: &[u8],
        allocator: &'a A,
    ) -> Result<AllocatedNode<'a, N, A>, ArtError> {
        let layout = N::get_type().node_layout();
        let ptr = allocator
            .allocate_zeroed(layout)
            .map_err(|_e| ArtError::Oom)?;
        let base_ptr = ptr.as_ptr() as *mut BaseNode;
        let node = BaseNode::new(N::get_type(), prefix);
        unsafe {
            std::ptr::write(base_ptr, node);

            if matches!(N::get_type(), NodeType::N48) {
                let mem = base_ptr as *mut Node48;
                (*mem).init_empty();
            }

            Ok(AllocatedNode::new(
                NonNull::new(base_ptr as *mut N).unwrap(),
                allocator,
            ))
        }
    }

    /// Here we must get a clone of allocator because the drop_node might be called in epoch guard
    pub(crate) unsafe fn drop_node<A: Allocator>(node: NonNull<BaseNode>, allocator: A) {
        let layout = unsafe { node.as_ref() }.get_type().node_layout();
        let ptr = std::ptr::NonNull::new(node.as_ptr() as *mut u8).unwrap();
        unsafe {
            allocator.deallocate(ptr, layout);
        }
    }

    pub(crate) fn get_type(&self) -> NodeType {
        self.meta.node_type
    }

    pub(crate) fn as_n4(&self) -> &Node4 {
        debug_assert!(matches!(self.get_type(), NodeType::N4));
        unsafe { &*(self as *const BaseNode as *const Node4) }
    }

    pub(crate) fn as_n16(&self) -> &Node16 {
        debug_assert!(matches!(self.get_type(), NodeType::N16));
        unsafe { &*(self as *const BaseNode as *const Node16) }
    }

    pub(crate) fn as_n48(&self) -> &Node48 {
        debug_assert!(matches!(self.get_type(), NodeType::N48));
        unsafe { &*(self as *const BaseNode as *const Node48) }
    }

    pub(crate) fn as_n256(&self) -> &Node256 {
        debug_assert!(matches!(self.get_type(), NodeType::N256));
        unsafe { &*(self as *const BaseNode as *const Node256) }
    }

    pub(crate) fn as_n4_mut(&mut self) -> &mut Node4 {
        debug_assert!(matches!(self.get_type(), NodeType::N4));
        unsafe { &mut *(self as *mut BaseNode as *mut Node4) }
    }

    pub(crate) fn as_n16_mut(&mut self) -> &mut Node16 {
        debug_assert!(matches!(self.get_type(), NodeType::N16));
        unsafe { &mut *(self as *mut BaseNode as *mut Node16) }
    }

    pub(crate) fn as_n48_mut(&mut self) -> &mut Node48 {
        debug_assert!(matches!(self.get_type(), NodeType::N48));
        unsafe { &mut *(self as *mut BaseNode as *mut Node48) }
    }

    pub(crate) fn as_n256_mut(&mut self) -> &mut Node256 {
        debug_assert!(matches!(self.get_type(), NodeType::N256));
        unsafe { &mut *(self as *mut BaseNode as *mut Node256) }
    }

    fn read_lock_inner<'a>(node: NonNull<BaseNode>) -> Result<ReadGuard<'a>, ArtError> {
        let version = unsafe { node.as_ref() }
            .type_version_lock_obsolete
            .load(Ordering::Acquire);

        if Self::is_locked(version) || Self::is_obsolete(version) {
            #[cfg(all(feature = "shuttle", test))]
            shuttle::thread::yield_now();
            return Err(ArtError::Locked);
        }

        Ok(ReadGuard::new(version, node))
    }

    pub(crate) fn read_lock<'a>(node: NonNull<BaseNode>) -> Result<ReadGuard<'a>, ArtError> {
        Self::read_lock_inner(node)
    }

    pub(crate) fn value_count(&self) -> usize {
        self.meta.count as usize
    }

    fn is_obsolete(version: usize) -> bool {
        (version & 1) == 1
    }

    pub(crate) fn prefix(&self) -> &[u8] {
        unsafe {
            self.meta
                .prefix
                .get_unchecked(..self.meta.prefix_cnt as usize)
        }
    }

    pub(crate) fn insert_grow<CurT: Node, BiggerT: Node, A: Allocator + Send + Clone + 'static>(
        n: TypedReadGuard<CurT>,
        parent: Parent,
        val: (u8, NodePtr),
        allocator: &A,
        guard: &Guard,
    ) -> Result<(), ArtError> {
        if !n.as_ref().is_full() {
            if let Parent::Node(_, p) = parent {
                p.unlock()?;
            }

            let mut write_n = n.upgrade().map_err(|v| v.1)?;

            write_n.as_mut().insert(val.0, val.1);
            return Ok(());
        }

        let mut write_n = n.upgrade().map_err(|v| v.1)?;

        let mut n_big =
            BaseNode::make_node::<BiggerT, A>(write_n.as_ref().base().prefix(), allocator)?;
        write_n.as_ref().copy_to(n_big.as_mut());
        n_big.as_mut().insert(val.0, val.1);

        match parent {
            Parent::Node(parent_key, parent_guard) => {
                let mut write_p = parent_guard.upgrade().map_err(|v| v.1)?;
                write_p.as_mut().change(parent_key, n_big.into_note_ptr());
            }
            Parent::Root(root_atomic_ptr) => {
                // Update the root pointer atomically
                root_atomic_ptr.store(
                    n_big.into_non_null().cast::<BaseNode>().as_ptr(),
                    Ordering::Release,
                );
            }
        }

        write_n.mark_obsolete();
        let delete_n = write_n.as_mut() as *mut CurT as usize;
        std::mem::forget(write_n);
        let allocator: A = allocator.clone();
        guard.defer(move || unsafe {
            let delete_n = NonNull::new(delete_n as *mut BaseNode).unwrap();
            BaseNode::drop_node(delete_n, allocator);
        });
        Ok(())
    }

    pub(crate) fn insert_and_unlock<'a, A: Allocator + Send + Clone + 'static>(
        node: ReadGuard<'a>,
        parent: Parent<'a>,
        val: (u8, NodePtr),
        allocator: &'a A,
        guard: &Guard,
    ) -> Result<(), ArtError> {
        match node.as_ref().get_type() {
            NodeType::N4 => Self::insert_grow::<Node4, Node16, A>(
                node.into_typed(),
                parent,
                val,
                allocator,
                guard,
            ),
            NodeType::N16 => Self::insert_grow::<Node16, Node48, A>(
                node.into_typed(),
                parent,
                val,
                allocator,
                guard,
            ),
            NodeType::N48 => Self::insert_grow::<Node48, Node256, A>(
                node.into_typed(),
                parent,
                val,
                allocator,
                guard,
            ),
            NodeType::N256 => Self::insert_grow::<Node256, Node256, A>(
                node.into_typed(),
                parent,
                val,
                allocator,
                guard,
            ),
        }
    }

    fn is_locked(version: usize) -> bool {
        (version & 0b10) == 0b10
    }

    pub(crate) fn check_prefix(&self, key: &[u8], mut level: usize) -> Option<usize> {
        let node_prefix = self.prefix();

        for (n, k) in node_prefix.iter().zip(key).skip(level) {
            if n != k {
                return None;
            }
            level += 1;
        }
        Some(level)
    }
}
