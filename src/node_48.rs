use crate::{
    base_node::{BaseNode, Node, NodeType},
    child_ptr::ChildPtr,
};
use std::alloc;

const EMPTY_MARKER: u8 = 48;

#[repr(C)]
pub(crate) struct Node48 {
    base: BaseNode,

    child_idx: [u8; 256],
    children: [ChildPtr; 48],
}

unsafe impl Send for Node48 {}
unsafe impl Sync for Node48 {}

impl Node for Node48 {
    fn new(prefix: &[u8]) -> Box<Self> {
        let layout = alloc::Layout::from_size_align(
            std::mem::size_of::<Node48>(),
            std::mem::align_of::<Node48>(),
        )
        .unwrap();
        let mut mem = unsafe {
            let mem = alloc::alloc_zeroed(layout) as *mut BaseNode;
            let base = BaseNode::new(NodeType::N48, prefix);
            mem.write(base);
            Box::from_raw(mem as *mut Node48)
        };
        for v in mem.child_idx.iter_mut() {
            *v = EMPTY_MARKER;
        }
        mem
    }

    fn get_type() -> NodeType {
        NodeType::N48
    }

    fn remove(&mut self, k: u8) {
        debug_assert!(self.child_idx[k as usize] != EMPTY_MARKER);
        self.children[self.child_idx[k as usize] as usize] =
            ChildPtr::from_raw(std::ptr::null_mut());
        self.child_idx[k as usize] = EMPTY_MARKER;
        self.base.count -= 1;
        debug_assert!(self.get_child(k).is_none());
    }

    fn get_children(&self, start: u8, end: u8) -> Vec<(u8, ChildPtr)> {
        let mut children = Vec::with_capacity(24);

        children.clear();

        for i in start..=end {
            if self.child_idx[i as usize] != EMPTY_MARKER {
                children.push((i, self.children[self.child_idx[i as usize] as usize]));
            }
        }

        children
    }

    fn copy_to<N: Node>(&self, dst: &mut N) {
        for i in 0..256 {
            if self.child_idx[i] != EMPTY_MARKER {
                dst.insert(i as u8, self.children[self.child_idx[i as usize] as usize]);
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
        self.base.count == 48
    }

    fn is_under_full(&self) -> bool {
        self.base.count == 12
    }

    fn insert(&mut self, key: u8, node: ChildPtr) {
        let mut pos = self.base.count as usize;

        if !self.children[pos].is_null() {
            pos = 0;
            while !self.children[pos].is_null() {
                pos += 1;
            }
        }
        debug_assert!(pos < 48);

        self.children[pos] = node;
        self.child_idx[key as usize] = pos as u8;
        self.base.count += 1;
    }

    fn change(&mut self, key: u8, val: ChildPtr) {
        self.children[self.child_idx[key as usize] as usize] = val;
    }

    fn get_child(&self, key: u8) -> Option<ChildPtr> {
        if self.child_idx[key as usize] == EMPTY_MARKER {
            None
        } else {
            Some(self.children[self.child_idx[key as usize] as usize])
        }
    }
}
