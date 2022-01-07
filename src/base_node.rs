use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_epoch::Guard;

use crate::{
    node_16::Node16, node_256::Node256, node_4::Node4, node_48::Node48,
    utils::convert_type_to_version,
};

pub(crate) const MAX_STORED_PREFIX_LEN: usize = 10;
pub(crate) type Prefix = [u8; MAX_STORED_PREFIX_LEN];

#[repr(u8)]
#[derive(Clone, Copy)]
pub(crate) enum NodeType {
    N4 = 0,
    N16 = 1,
    N48 = 2,
    N256 = 3,
}

pub(crate) trait Node {
    fn new(prefix: *const u8, prefix_len: usize) -> *mut Self;
    unsafe fn destroy_node(node: *mut Self);
    fn base(&self) -> &BaseNode;
    fn base_mut(&mut self) -> &mut BaseNode;
    fn is_full(&self) -> bool;
    fn is_under_full(&self) -> bool;
    fn insert(&mut self, key: u8, node: *mut BaseNode);
    fn change(&mut self, key: u8, val: *mut BaseNode);
    fn get_child(&self, key: u8) -> Option<*mut BaseNode>;
    fn get_any_child(&self) -> *const BaseNode;
    fn get_children(
        &self,
        start: u8,
        end: u8,
        out_children: &mut [(u8, *mut BaseNode)],
    ) -> (usize, usize);

    fn copy_to<N: Node>(&self, dst: *mut N);
}

#[repr(C)]
pub(crate) struct BaseNode {
    // 2b type | 60b version | 1b lock | 1b obsolete
    pub(crate) type_version_lock_obsolete: AtomicUsize,
    pub(crate) prefix_cnt: u32,
    pub(crate) count: u16, // TODO: we only need u8
    pub(crate) prefix: Prefix,
}

impl BaseNode {
    pub(crate) fn new(n_type: NodeType, prefix: *const u8, len: usize) -> Self {
        let val = convert_type_to_version(n_type);
        let mut prefix_v: [u8; MAX_STORED_PREFIX_LEN] = [0; MAX_STORED_PREFIX_LEN];

        let prefix_cnt = if len > 0 {
            let len = std::cmp::min(len, MAX_STORED_PREFIX_LEN);
            unsafe {
                std::ptr::copy_nonoverlapping(
                    prefix,
                    &mut prefix_v as *mut [u8; MAX_STORED_PREFIX_LEN] as *mut u8,
                    len,
                );
            }
            len
        } else {
            0
        };

        BaseNode {
            type_version_lock_obsolete: AtomicUsize::new(val),
            prefix_cnt: prefix_cnt as u32,
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
        } else {
            self.prefix_cnt = 0;
        }
    }

    /// returns (version, need_restart)
    pub(crate) fn read_lock_or_restart(&self) -> Result<usize, usize> {
        let version = self.type_version_lock_obsolete.load(Ordering::Acquire);
        if Self::is_locked(version) || Self::is_obsolete(version) {
            return Err(version);
        }
        Ok(version)
    }

    pub(crate) fn check_or_restart(&self, start_read: usize) -> Result<usize, ()> {
        self.read_unlock_or_restart(start_read)
    }

    /// returns need restart
    pub(crate) fn read_unlock_or_restart(&self, start_read: usize) -> Result<usize, ()> {
        let version = self.type_version_lock_obsolete.load(Ordering::Acquire);
        if start_read != version {
            Err(())
        } else {
            Ok(version)
        }
    }

    #[allow(dead_code)]
    pub(crate) fn write_lock_or_restart(&self) -> bool {
        let mut version = if let Ok(v) = self.read_lock_or_restart() {
            v
        } else {
            return true;
        };

        self.upgrade_to_write_lock_or_restart(&mut version).is_err()
    }

    /// returns (version, need_restart)
    pub(crate) fn upgrade_to_write_lock_or_restart(&self, version: &mut usize) -> Result<(), ()> {
        match self.type_version_lock_obsolete.compare_exchange_weak(
            *version,
            *version + 0b10,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => {
                *version += 0b10;
                Ok(())
            }
            Err(_) => Err(()),
        }
    }

    pub(crate) fn write_unlock(&self) {
        self.type_version_lock_obsolete
            .fetch_add(0b10, Ordering::Release);
    }

    pub(crate) fn write_unlock_obsolete(&self) {
        self.type_version_lock_obsolete
            .fetch_add(0b11, Ordering::Release);
    }

    fn is_locked(version: usize) -> bool {
        (version & 0b10) == 0b10
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
        &self.prefix
    }

    pub(crate) fn get_child(key: u8, node_ptr: *const BaseNode) -> Option<*mut BaseNode> {
        let node = unsafe { &*node_ptr };
        match node.get_type() {
            NodeType::N4 => {
                let cur_n = unsafe { &*(node_ptr as *const Node4) };
                cur_n.get_child(key)
            }
            NodeType::N16 => {
                let cur_n = unsafe { &*(node_ptr as *const Node16) };
                cur_n.get_child(key)
            }
            NodeType::N48 => {
                let cur_n = unsafe { &*(node_ptr as *const Node48) };
                cur_n.get_child(key)
            }
            NodeType::N256 => {
                let cur_n = unsafe { &*(node_ptr as *const Node256) };
                cur_n.get_child(key)
            }
        }
    }

    pub(crate) fn get_any_child_tid(n: &BaseNode) -> Result<usize, ()> {
        let mut next_node = n;
        loop {
            let node = next_node;
            let v = if let Ok(v) = node.read_lock_or_restart() {
                v
            } else {
                return Err(());
            };

            let next_node_ptr = Self::get_any_child(node);
            node.read_unlock_or_restart(v)?;

            if BaseNode::is_leaf(next_node_ptr) {
                return Ok(BaseNode::get_leaf(next_node_ptr));
            } else {
                next_node = unsafe { &*next_node_ptr };
            }
        }
    }

    pub(crate) fn get_any_child(n: &BaseNode) -> *const BaseNode {
        match n.get_type() {
            NodeType::N4 => {
                let n = n as *const BaseNode as *const Node4;
                unsafe { &*n }.get_any_child()
            }
            NodeType::N16 => {
                let n = n as *const BaseNode as *const Node16;
                unsafe { &*n }.get_any_child()
            }
            NodeType::N48 => {
                let n = n as *const BaseNode as *const Node48;
                unsafe { &*n }.get_any_child()
            }
            NodeType::N256 => {
                let n = n as *const BaseNode as *const Node256;
                unsafe { &*n }.get_any_child()
            }
        }
    }

    pub(crate) fn change(node: *mut BaseNode, key: u8, val: *mut BaseNode) {
        match unsafe { &*node }.get_type() {
            NodeType::N4 => {
                let n = node as *mut Node4;
                unsafe { &mut *n }.change(key, val);
            }
            NodeType::N16 => {
                let n = node as *mut Node16;
                unsafe { &mut *n }.change(key, val);
            }
            NodeType::N48 => {
                let n = node as *mut Node48;
                unsafe { &mut *n }.change(key, val);
            }
            NodeType::N256 => {
                let n = node as *mut Node256;
                unsafe { &mut *n }.change(key, val);
            }
        }
    }
    pub(crate) fn get_children(
        node: &BaseNode,
        start: u8,
        end: u8,
        out_children: &mut [(u8, *mut BaseNode)],
    ) -> (usize, usize) {
        match node.get_type() {
            NodeType::N4 => {
                let n = node as *const BaseNode as *const Node4;
                unsafe { &*n }.get_children(start, end, out_children)
            }
            NodeType::N16 => {
                let n = node as *const BaseNode as *const Node16;
                unsafe { &*n }.get_children(start, end, out_children)
            }
            NodeType::N48 => {
                let n = node as *const BaseNode as *const Node48;
                unsafe { &*n }.get_children(start, end, out_children)
            }
            NodeType::N256 => {
                let n = node as *const BaseNode as *const Node256;
                unsafe { &*n }.get_children(start, end, out_children)
            }
        }
    }

    pub(crate) fn insert_grow<CurT: Node, BiggerT: Node>(
        n: *mut CurT,
        mut v: usize,
        parent_node: *mut BaseNode,
        mut parent_version: usize,
        key_parent: u8,
        key: u8,
        val: *mut BaseNode,
        guard: &Guard,
    ) -> Result<(), ()> {
        if !unsafe { &*n }.is_full() {
            if !parent_node.is_null() {
                unsafe { &*parent_node }.read_unlock_or_restart(parent_version)?;
            }

            unsafe { &mut *n }
                .base()
                .upgrade_to_write_lock_or_restart(&mut v)?;

            unsafe { &mut *n }.insert(key, val);
            unsafe { &*n }.base().write_unlock();
            return Ok(());
        }

        unsafe { &*parent_node }.upgrade_to_write_lock_or_restart(&mut parent_version)?;

        if unsafe { &*n }
            .base()
            .upgrade_to_write_lock_or_restart(&mut v)
            .is_err()
        {
            unsafe { &*parent_node }.write_unlock();
            return Err(());
        }

        let n_big = {
            let n_ref = unsafe { &*n };
            BiggerT::new(
                n_ref.base().get_prefix().as_ptr(),
                n_ref.base().get_prefix_len() as usize,
            )
        };
        unsafe { &mut *n }.copy_to(n_big);
        unsafe { &mut *n_big }.insert(key, val);

        BaseNode::change(parent_node, key_parent, n_big as *mut BaseNode);

        unsafe { &mut *n }.base().write_unlock_obsolete();
        let d_n = unsafe { &mut *(n as *mut BaseNode) };
        guard.defer(move || {
            BaseNode::destroy_node(d_n as *mut BaseNode);
        });
        unsafe { &*parent_node }.write_unlock();
        Ok(())
    }

    pub(crate) fn insert_and_unlock(
        node: *mut BaseNode,
        v: usize,
        parent: *mut BaseNode,
        parent_v: usize,
        key_parent: u8,
        key: u8,
        val: *mut BaseNode,
        guard: &Guard,
    ) -> Result<(), ()> {
        match unsafe { &*node }.get_type() {
            NodeType::N4 => {
                let n = node as *mut Node4;
                Self::insert_grow::<Node4, Node16>(
                    n, v, parent, parent_v, key_parent, key, val, guard,
                )
            }
            NodeType::N16 => {
                let n = node as *mut Node16;
                Self::insert_grow::<Node16, Node48>(
                    n, v, parent, parent_v, key_parent, key, val, guard,
                )
            }
            NodeType::N48 => {
                let n = node as *mut Node48;
                Self::insert_grow::<Node48, Node256>(
                    n, v, parent, parent_v, key_parent, key, val, guard,
                )
            }
            NodeType::N256 => {
                let n = node as *mut Node256;
                Self::insert_grow::<Node256, Node256>(
                    n, v, parent, parent_v, key_parent, key, val, guard,
                )
            }
        }
    }

    pub(crate) fn is_leaf(ptr: *const BaseNode) -> bool {
        debug_assert!(!ptr.is_null());
        (ptr as usize & (1 << 63)) == (1 << 63)
    }

    pub(crate) fn get_leaf(ptr: *const BaseNode) -> usize {
        ptr as usize & ((1 << 63) - 1)
    }

    pub(crate) fn set_leaf(tid: usize) -> *mut BaseNode {
        (tid | (1 << 63)) as *mut BaseNode
    }

    pub(crate) fn destroy_node(node: *mut BaseNode) {
        match unsafe { &*node }.get_type() {
            NodeType::N4 => unsafe {
                Node4::destroy_node(node as *mut Node4);
            },
            NodeType::N16 => unsafe {
                Node16::destroy_node(node as *mut Node16);
            },
            NodeType::N48 => unsafe {
                Node48::destroy_node(node as *mut Node48);
            },
            NodeType::N256 => unsafe {
                Node256::destroy_node(node as *mut Node256);
            },
        }
    }
}
