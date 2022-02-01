use std::alloc;

use crate::base_node::{BaseNode, Node, NodeType};

#[repr(C)]
pub(crate) struct Node256 {
    base: BaseNode,

    children: [*const BaseNode; 256],
}

unsafe impl Send for Node256 {}
unsafe impl Sync for Node256 {}

impl Node for Node256 {
    fn new(prefix: &[u8]) -> Box<Node256> {
        let layout = alloc::Layout::from_size_align(
            std::mem::size_of::<Node256>(),
            std::mem::align_of::<Node256>(),
        )
        .unwrap();
        unsafe {
            let mem = alloc::alloc_zeroed(layout) as *mut BaseNode;
            let base = BaseNode::new(NodeType::N256, prefix);
            mem.write(base);

            Box::from_raw(mem as *mut Node256)
        }
    }

    fn get_children(&self, start: u8, end: u8) -> Vec<(u8, *const BaseNode)> {
        let mut children = Vec::with_capacity(48);

        for i in start..=end {
            if !self.children[i as usize].is_null() {
                children.push((i, self.children[i as usize] as *const BaseNode));
            }
        }

        children
    }

    fn copy_to<N: Node>(&self, dst: &mut N) {
        for i in 0..256 {
            if !self.children[i].is_null() {
                dst.insert(i as u8, self.children[i]);
            }
        }
    }

    fn base(&self) -> &BaseNode {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseNode {
        &mut self.base
    }

    fn is_full(&self) -> bool {
        false
    }

    fn is_under_full(&self) -> bool {
        self.base.count == 37
    }

    fn insert(&mut self, key: u8, node: *const BaseNode) {
        self.children[key as usize] = node;
        self.base.count += 1;
    }

    fn change(&mut self, key: u8, val: *const BaseNode) {
        self.children[key as usize] = val;
    }

    fn remove(&mut self, k: u8) {
        self.children[k as usize] = std::ptr::null_mut();
        self.base.count -= 1;
    }

    fn get_child(&self, key: u8) -> Option<*const BaseNode> {
        let child = self.children[key as usize];
        if child.is_null() {
            None
        } else {
            Some(child)
        }
    }
}
