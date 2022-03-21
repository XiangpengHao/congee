#[cfg(shuttle)]
use shuttle::sync::atomic::{AtomicUsize, Ordering};
use std::ops::Range;
#[cfg(not(shuttle))]
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_epoch::Guard;

use crate::{
    lock::{ConcreteReadGuard, ReadGuard},
    node_16::{Node16, Node16Iter},
    node_256::{Node256, Node256Iter},
    node_4::{Node4, Node4Iter},
    node_48::{Node48, Node48Iter},
    node_ptr::NodePtr,
    utils::ArtError,
};

pub(crate) const MAX_STORED_PREFIX_LEN: usize = 8;
pub(crate) type Prefix = [u8; MAX_STORED_PREFIX_LEN];

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
    fn base_mut(&mut self) -> &mut BaseNode;
    fn is_full(&self) -> bool;
    fn is_under_full(&self) -> bool;
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
pub(crate) struct BaseNode {
    // 2b type | 60b version | 1b lock | 1b obsolete
    pub(crate) type_version_lock_obsolete: AtomicUsize,
    pub(crate) meta: NodeMeta,
}

pub(crate) struct NodeMeta {
    prefix_cnt: u32,
    pub(crate) count: u16, // TODO: we only need u8
    node_type: NodeType,
    prefix: Prefix,
}

impl Drop for BaseNode {
    fn drop(&mut self) {
        let layout = self.get_type().node_layout();
        unsafe {
            std::alloc::dealloc(self as *mut BaseNode as *mut u8, layout);
        }
    }
}

macro_rules! gen_method {
    ($method_name:ident, ($($arg_n:ident : $args:ty),*), $return:ty) => {
        impl BaseNode {
            pub(crate) fn $method_name(&self, $($arg_n : $args),*) -> $return {
                match self.get_type() {
                    NodeType::N4 => {
                        let node = unsafe{&* (self as *const BaseNode as *const Node4)};
                        node.$method_name($($arg_n),*)
                    },
                    NodeType::N16 => {
                        let node = unsafe{&* (self as *const BaseNode as *const Node16)};
                        node.$method_name($($arg_n),*)
                    },
                    NodeType::N48 => {
                        let node = unsafe{&* (self as *const BaseNode as *const Node48)};
                        node.$method_name($($arg_n),*)
                    },
                    NodeType::N256 => {
                        let node = unsafe{&* (self as *const BaseNode as *const Node256)};
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
                        let node = unsafe{&mut * (self as *mut BaseNode as *mut Node4)};
                        node.$method_name($($arg_n),*)
                    },
                    NodeType::N16 => {
                        let node = unsafe{&mut * (self as *mut BaseNode as *mut Node16)};
                        node.$method_name($($arg_n),*)
                    },
                    NodeType::N48 => {
                        let node = unsafe{&mut * (self as *mut BaseNode as *mut Node48)};
                        node.$method_name($($arg_n),*)
                    },
                    NodeType::N256 => {
                        let node = unsafe{&mut * (self as *mut BaseNode as *mut Node256)};
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
        // let val = convert_type_to_version(n_type);
        let mut prefix_v: [u8; MAX_STORED_PREFIX_LEN] = [0; MAX_STORED_PREFIX_LEN];

        assert!(prefix.len() <= MAX_STORED_PREFIX_LEN);
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

    pub(crate) fn make_node<N: Node>(prefix: &[u8]) -> *mut N {
        let node = BaseNode::new(N::get_type(), prefix);
        let layout = N::get_type().node_layout();
        unsafe {
            let ptr = std::alloc::alloc_zeroed(layout) as *mut BaseNode;
            std::ptr::write(ptr, node);

            if matches!(N::get_type(), NodeType::N48) {
                let mem = ptr as *mut Node48;
                (&mut *mem).init_empty();
            }

            ptr as *mut N
        }
    }

    pub(crate) fn get_type(&self) -> NodeType {
        self.meta.node_type
    }

    pub(crate) fn set_prefix(&mut self, prefix: &[u8]) {
        let len = prefix.len();
        self.meta.prefix_cnt = len as u32;

        for (i, v) in prefix.iter().enumerate() {
            self.meta.prefix[i] = *v;
        }
    }

    pub(crate) fn read_lock(&self) -> Result<ReadGuard, ArtError> {
        let version = self.type_version_lock_obsolete.load(Ordering::Acquire);
        if Self::is_locked(version) || Self::is_obsolete(version) {
            return Err(ArtError::Locked(version));
        }

        Ok(ReadGuard::new(version, self))
    }

    fn is_locked(version: usize) -> bool {
        (version & 0b10) == 0b10
    }

    pub(crate) fn get_count(&self) -> usize {
        self.meta.count as usize
    }

    fn is_obsolete(version: usize) -> bool {
        (version & 1) == 1
    }

    pub(crate) fn prefix(&self) -> &[u8] {
        self.meta.prefix[..self.meta.prefix_cnt as usize].as_ref()
    }

    pub(crate) fn prefix_range(&self, range: Range<usize>) -> &[u8] {
        debug_assert!(range.end <= self.meta.prefix_cnt as usize);
        self.meta.prefix[range].as_ref()
    }

    pub(crate) fn insert_grow<CurT: Node, BiggerT: Node>(
        n: ConcreteReadGuard<CurT>,
        parent_node: Option<ReadGuard>,
        key_parent: u8,
        key: u8,
        val: NodePtr,
        guard: &Guard,
    ) -> Result<(), ArtError> {
        if !n.as_ref().is_full() {
            if let Some(p) = parent_node {
                p.unlock()?;
            }

            let mut write_n = n.upgrade().map_err(|v| v.1)?;

            write_n.as_mut().insert(key, val);
            return Ok(());
        }

        let p = parent_node.expect("parent node must present when current node is full");

        let mut write_p = p.upgrade().map_err(|v| v.1)?;

        let mut write_n = n.upgrade().map_err(|v| v.1)?;

        let n_big = BaseNode::make_node::<BiggerT>(write_n.as_ref().base().prefix());
        write_n.as_ref().copy_to(unsafe { &mut *n_big });
        unsafe { &mut *n_big }.insert(key, val);

        write_p
            .as_mut()
            .change(key_parent, NodePtr::from_node(n_big as *mut BaseNode));

        write_n.mark_obsolete();
        let delete_n = write_n.as_mut() as *mut CurT as usize;
        std::mem::forget(write_n);
        guard.defer(move || unsafe {
            std::ptr::drop_in_place(delete_n as *mut BaseNode);
        });
        Ok(())
    }

    pub(crate) fn insert_and_unlock(
        node: ReadGuard,
        parent: Option<ReadGuard>,
        key_parent: u8,
        key: u8,
        val: NodePtr,
        guard: &Guard,
    ) -> Result<(), ArtError> {
        match node.as_ref().get_type() {
            NodeType::N4 => Self::insert_grow::<Node4, Node16>(
                node.into_concrete(),
                parent,
                key_parent,
                key,
                val,
                guard,
            ),
            NodeType::N16 => Self::insert_grow::<Node16, Node48>(
                node.into_concrete(),
                parent,
                key_parent,
                key,
                val,
                guard,
            ),
            NodeType::N48 => Self::insert_grow::<Node48, Node256>(
                node.into_concrete(),
                parent,
                key_parent,
                key,
                val,
                guard,
            ),
            NodeType::N256 => Self::insert_grow::<Node256, Node256>(
                node.into_concrete(),
                parent,
                key_parent,
                key,
                val,
                guard,
            ),
        }
    }
}
