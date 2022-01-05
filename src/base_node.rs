use std::sync::atomic::{AtomicUsize, Ordering};

use crate::{
    node_16::Node16, node_256::Node256, node_4::Node4, node_48::Node48,
    utils::convert_type_to_version,
};

pub(crate) const MAX_STORED_PREFIX_LEN: usize = 11;
pub(crate) type Prefix = [u8; MAX_STORED_PREFIX_LEN];

#[repr(u8)]
#[derive(Clone, Copy)]
pub(crate) enum NodeType {
    N4 = 0,
    N16 = 1,
    N48 = 2,
    N256 = 3,
}

#[repr(C)]
pub(crate) struct BaseNode {
    // 2b type | 60b version | 1b lock | 1b obsolete
    pub(crate) type_version_lock_obsolete: AtomicUsize,
    pub(crate) prefix_cnt: u32,
    pub(crate) count: u8,
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

    fn set_type(&self, n_type: NodeType) {
        let val = convert_type_to_version(n_type);
        self.type_version_lock_obsolete
            .fetch_add(val, Ordering::Release);
    }

    fn get_type(&self) -> NodeType {
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
        return Ok(version);
    }

    pub(crate) fn check_or_restart(&self, start_read: usize) -> bool {
        self.read_unlock_or_restart(start_read)
    }

    /// returns need restart
    pub(crate) fn read_unlock_or_restart(&self, start_read: usize) -> bool {
        start_read != self.type_version_lock_obsolete.load(Ordering::Acquire)
    }

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
            let need_restart = node.read_unlock_or_restart(v);
            if need_restart {
                return Err(());
            }

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
                return unsafe { &*n }.get_any_child();
            }
            NodeType::N16 => {
                let n = n as *const BaseNode as *const Node16;
                return unsafe { &*n }.get_any_child();
            }
            NodeType::N48 => {
                let n = n as *const BaseNode as *const Node48;
                return unsafe { &*n }.get_any_child();
            }
            NodeType::N256 => {
                let n = n as *const BaseNode as *const Node256;
                return unsafe { &*n }.get_any_child();
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

    pub(crate) fn insert_and_unlock(
        node: *mut BaseNode,
        v: usize,
        parent: *mut BaseNode,
        parent_v: usize,
        key_parent: u8,
        key: u8,
        val: *mut BaseNode,
    ) -> bool {
        match unsafe { &*node }.get_type() {
            NodeType::N4 => {
                let n = node as *mut Node4;
            }
            NodeType::N16 => {}
            NodeType::N48 => {}
            NodeType::N256 => {}
        }
        todo!()
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
}
