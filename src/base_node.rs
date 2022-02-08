use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_epoch::Guard;

use crate::{
    child_ptr::NodePtr,
    lock::{ConcreteReadGuard, ReadGuard, WriteGuard},
    node_16::Node16,
    node_256::Node256,
    node_4::Node4,
    node_48::Node48,
    utils::convert_type_to_version,
};

pub(crate) const MAX_STORED_PREFIX_LEN: usize = 10;
pub(crate) type Prefix = [u8; MAX_STORED_PREFIX_LEN];

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum NodeType {
    N4 = 0,
    N16 = 1,
    N48 = 2,
    N256 = 3,
}

pub(crate) trait Node: Send + Sync {
    fn new(prefix: &[u8]) -> Box<Self>;
    fn base(&self) -> &BaseNode;
    fn base_mut(&mut self) -> &mut BaseNode;
    fn is_full(&self) -> bool;
    fn is_under_full(&self) -> bool;
    fn insert(&mut self, key: u8, node: NodePtr);
    fn change(&mut self, key: u8, val: NodePtr);
    fn get_child(&self, key: u8) -> Option<NodePtr>;
    fn get_children(&self, start: u8, end: u8) -> Vec<(u8, NodePtr)>;
    fn remove(&mut self, k: u8);
    fn copy_to<N: Node>(&self, dst: &mut N);
    fn get_type() -> NodeType;
}

#[repr(C)]
pub(crate) struct BaseNode {
    // 2b type | 60b version | 1b lock | 1b obsolete
    pub(crate) type_version_lock_obsolete: AtomicUsize,
    pub(crate) prefix_cnt: u32,
    pub(crate) count: u16, // TODO: we only need u8
    pub(crate) prefix: Prefix,
}

impl Drop for BaseNode {
    fn drop(&mut self) {
        let layout = match self.get_type() {
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
        };
        unsafe {
            std::alloc::dealloc(self as *mut BaseNode as *mut u8, layout);
        }
    }
}

impl BaseNode {
    pub(crate) fn new(n_type: NodeType, prefix: &[u8]) -> Self {
        let val = convert_type_to_version(n_type);
        let mut prefix_v: [u8; MAX_STORED_PREFIX_LEN] = [0; MAX_STORED_PREFIX_LEN];

        assert!(prefix.len() <= MAX_STORED_PREFIX_LEN);
        for (i, v) in prefix.iter().enumerate() {
            prefix_v[i] = *v;
        }

        BaseNode {
            type_version_lock_obsolete: AtomicUsize::new(val),
            prefix_cnt: prefix.len() as u32,
            count: 0,
            prefix: prefix_v,
        }
    }

    #[allow(dead_code)]
    fn set_type(&self, n_type: NodeType) {
        let val = convert_type_to_version(n_type);
        self.type_version_lock_obsolete
            .fetch_add(val, Ordering::Release);
    }

    pub(crate) fn get_type(&self) -> NodeType {
        let val = self.type_version_lock_obsolete.load(Ordering::Relaxed);
        let val = val >> 62;
        debug_assert!(val < 4);
        unsafe { std::mem::transmute(val as u8) }
    }

    pub(crate) fn set_prefix(&mut self, prefix: *const u8, len: usize) {
        if len > 0 {
            let len = std::cmp::min(len, MAX_STORED_PREFIX_LEN);
            unsafe {
                std::ptr::copy_nonoverlapping(
                    prefix,
                    &mut self.prefix as *mut [u8; MAX_STORED_PREFIX_LEN] as *mut u8,
                    len,
                );
            }
            self.prefix_cnt = len as u32;
        } else {
            self.prefix_cnt = 0;
        }
    }

    pub(crate) fn read_lock(&self) -> Result<ReadGuard, usize> {
        let version = self.type_version_lock_obsolete.load(Ordering::Acquire);
        if Self::is_locked(version) || Self::is_obsolete(version) {
            return Err(version);
        }

        Ok(ReadGuard::new(version, self))
    }

    #[allow(dead_code)]
    pub(crate) fn write_lock_n(&self) -> Result<WriteGuard, usize> {
        let read = self.read_lock()?;
        read.upgrade().map_err(|v| v.1)
    }

    fn is_locked(version: usize) -> bool {
        (version & 0b10) == 0b10
    }

    pub(crate) fn get_count(&self) -> usize {
        self.count as usize
    }

    fn is_obsolete(version: usize) -> bool {
        (version & 1) == 1
    }

    pub(crate) fn has_prefix(&self) -> bool {
        self.prefix_cnt > 0
    }

    pub(crate) fn get_prefix_len(&self) -> u32 {
        self.prefix_cnt
    }

    pub(crate) fn get_prefix(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.prefix.as_ptr(), self.prefix_cnt as usize) }
    }

    pub(crate) fn get_child(&self, key: u8) -> Option<NodePtr> {
        match self.get_type() {
            NodeType::N4 => {
                let cur_n = unsafe { &*(self as *const BaseNode as *const Node4) };
                cur_n.get_child(key)
            }
            NodeType::N16 => {
                let cur_n = unsafe { &*(self as *const BaseNode as *const Node16) };
                cur_n.get_child(key)
            }
            NodeType::N48 => {
                let cur_n = unsafe { &*(self as *const BaseNode as *const Node48) };
                cur_n.get_child(key)
            }
            NodeType::N256 => {
                let cur_n = unsafe { &*(self as *const BaseNode as *const Node256) };
                cur_n.get_child(key)
            }
        }
    }

    pub(crate) fn change(&mut self, key: u8, val: NodePtr) {
        match self.get_type() {
            NodeType::N4 => {
                let n = self as *mut BaseNode as *mut Node4;
                unsafe { &mut *n }.change(key, val);
            }
            NodeType::N16 => {
                let n = self as *mut BaseNode as *mut Node16;
                unsafe { &mut *n }.change(key, val);
            }
            NodeType::N48 => {
                let n = self as *mut BaseNode as *mut Node48;
                unsafe { &mut *n }.change(key, val);
            }
            NodeType::N256 => {
                let n = self as *mut BaseNode as *mut Node256;
                unsafe { &mut *n }.change(key, val);
            }
        }
    }

    pub(crate) fn get_children(&self, start: u8, end: u8) -> Vec<(u8, NodePtr)> {
        let children = match self.get_type() {
            NodeType::N4 => {
                let n = self as *const BaseNode as *const Node4;
                unsafe { &*n }.get_children(start, end)
            }
            NodeType::N16 => {
                let n = self as *const BaseNode as *const Node16;
                unsafe { &*n }.get_children(start, end)
            }
            NodeType::N48 => {
                let n = self as *const BaseNode as *const Node48;
                unsafe { &*n }.get_children(start, end)
            }
            NodeType::N256 => {
                let n = self as *const BaseNode as *const Node256;
                unsafe { &*n }.get_children(start, end)
            }
        };
        children
    }

    pub(crate) fn insert_grow<CurT: Node, BiggerT: Node>(
        n: ConcreteReadGuard<CurT>,
        parent_node: Option<ReadGuard>,
        key_parent: u8,
        key: u8,
        val: NodePtr,
        guard: &Guard,
    ) -> Result<(), ()> {
        if !n.as_ref().is_full() {
            if let Some(p) = parent_node {
                p.unlock().map_err(|_| ())?;
            }

            let mut write_n = n.upgrade_to_write_lock().map_err(|_| ())?;

            write_n.as_mut().insert(key, val);
            return Ok(());
        }

        let p = parent_node.expect("parent node must present when current node is full");

        let mut write_p = p.upgrade().map_err(|_| ())?;

        let mut write_n = match n.upgrade_to_write_lock() {
            Ok(w) => w,
            Err(_) => {
                return Err(());
            }
        };

        let mut n_big = BiggerT::new(write_n.as_ref().base().get_prefix());
        write_n.as_ref().copy_to(n_big.as_mut());
        n_big.insert(key, val);

        write_p.as_mut().change(
            key_parent,
            NodePtr::from_node(Box::into_raw(n_big) as *mut BaseNode),
        );

        write_n.mark_obsolete();
        let delete_n = unsafe { &mut *(write_n.as_mut() as *mut CurT as *mut BaseNode) };
        std::mem::forget(write_n);
        guard.defer(move || unsafe {
            std::ptr::drop_in_place(delete_n);
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
    ) -> Result<(), ()> {
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

    pub(crate) fn remove(&mut self, key: u8) {
        match self.get_type() {
            NodeType::N4 => {
                let n = unsafe { &mut *(self as *mut BaseNode as *mut Node4) };
                n.remove(key);
            }
            NodeType::N16 => {
                let n = unsafe { &mut *(self as *mut BaseNode as *mut Node16) };
                n.remove(key);
            }
            NodeType::N48 => {
                let n = unsafe { &mut *(self as *mut BaseNode as *mut Node48) };
                n.remove(key);
            }
            NodeType::N256 => {
                let n = unsafe { &mut *(self as *mut BaseNode as *mut Node256) };
                n.remove(key);
            }
        }
    }
}
