use super::NodePtr;
use super::base_node::{BaseNode, Node, NodeIter, NodeType};

#[repr(C)]
#[repr(align(64))]
pub(crate) struct Node4 {
    base: BaseNode,
    keys: [u8; 4],
    children: [NodePtr; 4],
}

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(std::mem::size_of::<Node4>() == 64);

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(std::mem::align_of::<Node4>() == 64);

pub(crate) struct Node4Iter<'a> {
    start: u8,
    end: u8,
    idx: u8,
    cnt: u8,
    node: &'a Node4,
}

impl Iterator for Node4Iter<'_> {
    type Item = (u8, NodePtr);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.idx >= self.cnt {
                return None;
            }
            let cur = self.idx;
            self.idx += 1;

            let key = self.node.keys[cur as usize];
            if key >= self.start && key <= self.end {
                return Some((key, self.node.children[cur as usize]));
            }
        }
    }
}

impl Node for Node4 {
    fn get_type() -> NodeType {
        NodeType::N4
    }

    fn remove(&mut self, k: u8) {
        if let Some(pos) = self.keys.iter().position(|&key| key == k) {
            self.keys
                .copy_within(pos + 1..self.base.meta.count as usize, pos);
            self.children
                .copy_within(pos + 1..self.base.meta.count as usize, pos);

            self.base.meta.count -= 1;
        }
    }

    fn get_children(&self, start: u8, end: u8) -> NodeIter {
        NodeIter::N4(Node4Iter {
            start,
            end,
            idx: 0,
            cnt: self.base.meta.count as u8,
            node: self,
        })
    }

    fn copy_to<N: Node>(&self, dst: &mut N) {
        for i in 0..self.base.meta.count {
            dst.insert(self.keys[i as usize], self.children[i as usize]);
        }
    }

    fn base(&self) -> &BaseNode {
        &self.base
    }

    fn is_full(&self) -> bool {
        self.base.meta.count == 4
    }

    fn insert(&mut self, key: u8, node: NodePtr) {
        let mut pos: usize = 0;

        while (pos as u16) < self.base.meta.count {
            if self.keys[pos] < key {
                pos += 1;
                continue;
            } else {
                break;
            }
        }

        if pos < self.base.meta.count as usize {
            self.keys
                .copy_within(pos..self.base.meta.count as usize, pos + 1);
            self.children
                .copy_within(pos..self.base.meta.count as usize, pos + 1);
        }

        self.keys[pos] = key;
        self.children[pos] = node;
        self.base.meta.count += 1;
    }

    fn change(&mut self, key: u8, val: NodePtr) -> NodePtr {
        for i in 0..self.base.meta.count {
            if self.keys[i as usize] == key {
                let old = self.children[i as usize];
                self.children[i as usize] = val;
                return old;
            }
        }
        unreachable!("The key should always exist in the node");
    }

    #[inline]
    fn get_child(&self, key: u8) -> Option<NodePtr> {
        // Manually unrolled loop for better performance on small arrays
        let count = self.base.meta.count as usize;

        if count > 0 && self.keys[0] == key {
            return Some(self.children[0]);
        }
        if count > 1 && self.keys[1] == key {
            return Some(self.children[1]);
        }
        if count > 2 && self.keys[2] == key {
            return Some(self.children[2]);
        }
        if count > 3 && self.keys[3] == key {
            return Some(self.children[3]);
        }

        None
    }
}
