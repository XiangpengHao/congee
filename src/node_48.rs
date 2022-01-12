use crate::base_node::{BaseNode, Node, NodeType};
use std::alloc;

const EMPTY_MARKER: u8 = 48;

#[repr(C)]
pub(crate) struct Node48 {
    base: BaseNode,

    child_idx: [u8; 256],
    children: [*mut BaseNode; 48],
}

impl Node48 {}

impl Node for Node48 {
    fn new(prefix: *const u8, prefix_len: usize) -> *mut Self {
        let layout = alloc::Layout::from_size_align(
            std::mem::size_of::<Node48>(),
            std::mem::align_of::<Node48>(),
        )
        .unwrap();
        let mem = unsafe {
            let mem = alloc::alloc_zeroed(layout) as *mut BaseNode;
            let base = BaseNode::new(NodeType::N48, prefix, prefix_len);
            mem.write(base);
            mem as *mut Node48
        };
        {
            let mem_ref = unsafe { &mut *mem };
            for v in mem_ref.child_idx.iter_mut() {
                *v = EMPTY_MARKER;
            }
        }
        mem
    }

    unsafe fn destroy_node(node: *mut Self) {
        let layout = alloc::Layout::from_size_align(
            std::mem::size_of::<Self>(),
            std::mem::align_of::<Self>(),
        )
        .unwrap();
        alloc::dealloc(node as *mut u8, layout);
    }

    fn remove(&mut self, k: u8) {
        debug_assert!(self.child_idx[k as usize] != EMPTY_MARKER);
        self.children[self.child_idx[k as usize] as usize] = std::ptr::null_mut();
        self.child_idx[k as usize] = EMPTY_MARKER;
        self.base.count -= 1;
        debug_assert!(self.get_child(k).is_none());
    }

    fn get_children(&self, start: u8, end: u8) -> Result<(usize, Vec<(u8, *mut BaseNode)>), ()> {
        let mut children = Vec::with_capacity(24);
        let v = if let Ok(v) = self.base.read_lock() {
            v
        } else {
            return Err(());
        };

        children.clear();

        for i in start..=end {
            if self.child_idx[i as usize] != EMPTY_MARKER {
                children.push((i, self.children[self.child_idx[i as usize] as usize]));
            }
        }
        if self.base.read_unlock(v).is_err() {
            return Err(());
        };
        Ok((v, children))
    }

    fn copy_to<N: Node>(&self, dst: *mut N) {
        for i in 0..256 {
            if self.child_idx[i] != EMPTY_MARKER {
                unsafe { &mut *dst }
                    .insert(i as u8, self.children[self.child_idx[i as usize] as usize]);
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

    fn change(&mut self, key: u8, val: *mut BaseNode) {
        self.children[self.child_idx[key as usize] as usize] = val;
    }

    fn get_child(&self, key: u8) -> Option<*mut BaseNode> {
        if self.child_idx[key as usize] == EMPTY_MARKER {
            None
        } else {
            Some(self.children[self.child_idx[key as usize] as usize])
        }
    }

    fn get_any_child(&self) -> *const BaseNode {
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
        any_child
    }
}
