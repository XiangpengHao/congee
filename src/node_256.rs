use std::{alloc, mem::align_of};

use crate::base_node::{BaseNode, NodeType};

#[repr(C)]
pub(crate) struct Node256 {
    base: BaseNode,

    children: [*mut BaseNode; 256],
}

impl Node256 {
    pub(crate) fn new() -> *mut BaseNode {
        let layout = alloc::Layout::from_size_align(
            std::mem::size_of::<Node256>(),
            std::mem::align_of::<Node256>(),
        )
        .unwrap();
        let mem = unsafe {
            let mem = alloc::alloc_zeroed(layout) as *mut BaseNode;
            let base = BaseNode::new(NodeType::N256, std::ptr::null(), 0);
            mem.write(base);
            mem
        };
        mem
    }

    fn is_full(&self) -> bool {
        self.base.count == 16
    }

    fn is_under_full(&self) -> bool {
        self.base.count == 37
    }

    fn insert(&mut self, key: u8, node: *mut BaseNode) {
        self.children[key as usize] = node;
        self.base.count += 1;
    }

    fn change(&mut self, key: u8, val: *mut BaseNode) {
        self.children[key as usize] = val;
    }

    pub(crate) fn get_child(&self, key: u8) -> Option<*mut BaseNode> {
        return Some(self.children[key as usize]);
    }

    pub(crate) fn get_any_child(&self) -> *const BaseNode {
        let mut any_child = std::ptr::null();

        for c in self.children.iter() {
            if !((*c).is_null()) {
                if BaseNode::is_leaf(*c) {
                    return *c;
                } else {
                    any_child = *c;
                }
            }
        }
        return any_child;
    }
}
