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

    fn set_prefix(&mut self, prefix: *const u8, len: usize) {
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
    pub(crate) fn read_lock_or_restart(&self) -> (usize, bool) {
        let version = self.type_version_lock_obsolete.load(Ordering::Acquire);
        if Self::is_locked(version) || Self::is_obsolete(version) {
            return (version, true);
        }
        return (version, false);
    }

    pub(crate) fn check_or_restart(&self, start_read: usize) -> bool {
        self.read_unlock_or_restart(start_read)
    }

    /// returns need restart
    pub(crate) fn read_unlock_or_restart(&self, start_read: usize) -> bool {
        start_read != self.type_version_lock_obsolete.load(Ordering::Acquire)
    }

    fn write_lock_or_restart(&self) -> bool {
        let (version, need_restart) = self.read_lock_or_restart();
        if need_restart {
            return need_restart;
        }

        let (_version, need_restart) = self.upgrade_to_write_lock_or_restart(version);
        return need_restart;
    }

    /// returns (version, need_restart)
    fn upgrade_to_write_lock_or_restart(&self, version: usize) -> (usize, bool) {
        match self.type_version_lock_obsolete.compare_exchange_weak(
            version,
            version + 0b10,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => (version + 0b10, false),
            Err(_) => (version, true),
        }
    }

    fn write_unlock(&self) {
        self.type_version_lock_obsolete
            .fetch_add(0b10, Ordering::Release);
    }

    fn write_unlock_obsolete(&self) {
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

    pub(crate) fn get_any_child_tid(&self, n: *const BaseNode) -> Result<usize, ()> {
        let mut next_node = n;
        loop {
            let node = next_node;
            let (v, need_restart) = unsafe { &*node }.read_lock_or_restart();
            if need_restart {
                return Err(());
            }

            // next_node = self
        }
    }

    pub(crate) fn get_any_child(n: *const BaseNode) -> *const BaseNode {
        match unsafe { &*n }.get_type() {
            NodeType::N4 => {}
            NodeType::N16 => {}
            NodeType::N48 => {}
            NodeType::N256 => {}
        }
        todo!()
    }

    pub(crate) fn is_leaf(ptr: *const BaseNode) -> bool {
        (ptr as usize & (1 << 63)) == (1 << 63)
    }

    pub(crate) fn get_leaf(ptr: *const BaseNode) -> usize {
        ptr as usize & ((1 << 63) - 1)
    }
}
