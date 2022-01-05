use std::ptr::NonNull;

use crate::base_node::BaseNode;

const EMPTY_MARKER: u8 = 48;

#[repr(C)]
pub(crate) struct Node48 {
    base: BaseNode,

    child_idx: [u8; 256],
    children: [*mut BaseNode; 48],
}

impl Node48 {
    fn is_full(&self) -> bool {
        self.base.count == 48
    }

    fn is_under_full(&self) -> bool {
        self.base.count == 12
    }

    fn insert(&mut self, key: u8, node: *mut BaseNode) {
        let mut pos = self.base.count as usize;

        while !self.children[pos].is_null() {
            pos += 1;
        }
        debug_assert!(pos < 48);

        self.children[pos] = node;
        self.child_idx[key as usize] = pos as u8;
        self.base.count += 1;
    }

    pub(crate) fn change(&mut self, key: u8, val: *mut BaseNode) {
        self.children[self.child_idx[key as usize] as usize] = val;
    }

    pub(crate) fn get_child(&self, key: u8) -> Option<*mut BaseNode> {
        if self.child_idx[key as usize] == EMPTY_MARKER {
            return None;
        } else {
            return Some(self.children[self.child_idx[key as usize] as usize]);
        }
    }

    pub(crate) fn get_any_child(&self) -> *const BaseNode {
        let mut any_child = std::ptr::null();

        for i in 0..256 {
            if self.child_idx[i] != EMPTY_MARKER {
                let child = self.children[self.child_idx[i as usize] as usize];
                if BaseNode::is_leaf(child) {
                    return child;
                } else {
                    any_child = child;
                }
            }
        }
        return any_child;
    }
}
