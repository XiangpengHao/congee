use std::ops::Add;

use crate::base_node::BaseNode;

#[repr(C)]
pub(crate) struct Node4 {
    base: BaseNode,

    keys: [u8; 4],
    children: [*mut BaseNode; 4],
}

impl Node4 {
    fn is_full(&self) -> bool {
        self.base.count == 4
    }

    fn insert(&mut self, key: u8, node: *mut BaseNode) {
        let mut pos: usize = 0;

        while (pos as u8) < self.base.count {
            if self.keys[pos] < key {
                pos += 1;
                continue;
            } else {
                break;
            }
        }

        unsafe {
            std::ptr::copy(
                self.keys.as_ptr().add(pos),
                self.keys.as_mut_ptr().add(pos + 1),
                self.base.count as usize - pos,
            );

            std::ptr::copy(
                self.children.as_ptr().add(pos),
                self.children.as_mut_ptr().add(pos + 1),
                self.base.count as usize - pos,
            );
        }

        self.keys[pos] = key;
        self.children[pos] = node;
        self.base.count += 1;
    }

    fn change(&mut self, key: u8, val: *mut BaseNode) {
        for i in 0..self.base.count {
            if self.keys[i as usize] == key {
                self.children[i as usize] = val;
            }
        }
    }

    pub(crate) fn get_child(&self, key: u8) -> Option<*mut BaseNode> {
        for (i, k) in self.keys.iter().enumerate() {
            if *k == key {
                return Some(self.children[i]);
            }
        }
        return None;
    }

    pub(crate) fn get_any_child(&self) -> *const BaseNode {
        let mut any_child = std::ptr::null();

        for c in self.children.iter() {
            if BaseNode::is_leaf(*c as *const BaseNode) {
                return *c;
            } else {
                any_child = *c;
            }
        }
        return any_child;
    }
}
