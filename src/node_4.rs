use std::alloc;

use crate::base_node::{BaseNode, Node, NodeType};

#[repr(C)]
pub(crate) struct Node4 {
    base: BaseNode,

    keys: [u8; 4],
    children: [*mut BaseNode; 4],
}

unsafe impl Send for Node4 {}
unsafe impl Sync for Node4 {}

impl Node for Node4 {
    fn new(prefix: &[u8]) -> *mut Node4 {
        let layout = alloc::Layout::from_size_align(
            std::mem::size_of::<Node4>(),
            std::mem::align_of::<Node4>(),
        )
        .unwrap();
        unsafe {
            let mem = alloc::alloc_zeroed(layout) as *mut BaseNode;
            let base = BaseNode::new(NodeType::N4, prefix);
            mem.write(base);
            mem as *mut Node4
        }
    }

    fn remove(&mut self, k: u8) {
        for i in 0..self.base.count {
            if self.keys[i as usize] == k {
                unsafe {
                    std::ptr::copy(
                        self.keys.as_ptr().add(i as usize + 1),
                        self.keys.as_mut_ptr().add(i as usize),
                        (self.base.count - i - 1) as usize,
                    );

                    std::ptr::copy(
                        self.children.as_ptr().add(i as usize + 1),
                        self.children.as_mut_ptr().add(i as usize),
                        (self.base.count - i - 1) as usize,
                    )
                }
                self.base.count -= 1;
                return;
            }
        }
    }

    fn get_children(&self, start: u8, end: u8) -> Result<(usize, Vec<(u8, *const BaseNode)>), ()> {
        let mut out_children = Vec::with_capacity(4);
        let version = if let Ok(v) = self.base.read_lock() {
            v
        } else {
            return Err(());
        };

        out_children.clear();
        for i in 0..self.base.count as usize {
            if self.keys[i] >= start && self.keys[i] <= end {
                out_children.push((self.keys[i], self.children[i] as *const BaseNode));
            }
        }

        if self.base.read_unlock(version).is_err() {
            return Err(());
        };

        Ok((version, out_children))
    }

    fn copy_to<N: Node>(&self, dst: &mut N) {
        for i in 0..self.base.count {
            dst.insert(self.keys[i as usize], self.children[i as usize]);
        }
    }

    fn base(&self) -> &BaseNode {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseNode {
        &mut self.base
    }

    fn is_full(&self) -> bool {
        self.base.count == 4
    }

    fn is_under_full(&self) -> bool {
        false
    }

    fn insert(&mut self, key: u8, node: *mut BaseNode) {
        let mut pos: usize = 0;

        while (pos as u16) < self.base.count {
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

    fn get_child(&self, key: u8) -> Option<*mut BaseNode> {
        for i in 0..self.base.count {
            if self.keys[i as usize] == key {
                return Some(self.children[i as usize]);
            }
        }
        None
    }

    fn get_any_child(&self) -> *const BaseNode {
        let mut any_child = std::ptr::null();

        for c in self.children.iter() {
            if BaseNode::is_leaf(*c as *const BaseNode) {
                return *c;
            } else {
                any_child = *c;
            }
        }
        any_child
    }
}
