use crate::{
    base_node::{BaseNode, Node, NodeIter, NodeType},
    node_ptr::NodePtr,
};

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
        for i in 0..self.base.meta.count {
            if self.keys[i as usize] == k {
                unsafe {
                    std::ptr::copy(
                        self.keys.as_ptr().add(i as usize + 1),
                        self.keys.as_mut_ptr().add(i as usize),
                        (self.base.meta.count - i - 1) as usize,
                    );

                    std::ptr::copy(
                        self.children.as_ptr().add(i as usize + 1),
                        self.children.as_mut_ptr().add(i as usize),
                        (self.base.meta.count - i - 1) as usize,
                    )
                }
                self.base.meta.count -= 1;
                return;
            }
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

        unsafe {
            std::ptr::copy(
                self.keys.as_ptr().add(pos),
                self.keys.as_mut_ptr().add(pos + 1),
                self.base.meta.count as usize - pos,
            );

            std::ptr::copy(
                self.children.as_ptr().add(pos),
                self.children.as_mut_ptr().add(pos + 1),
                self.base.meta.count as usize - pos,
            );
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

    fn get_child(&self, key: u8) -> Option<NodePtr> {
        self.keys
            .iter()
            .zip(self.children.iter())
            .take(self.base.meta.count as usize)
            .find(|(k, _)| **k == key)
            .map(|(_, c)| *c)
    }

    #[cfg(feature = "db_extension")]
    fn get_random_child(&self, rng: &mut impl rand::Rng) -> Option<(u8, NodePtr)> {
        if self.base.meta.count == 0 {
            return None;
        }
        let idx = rng.gen_range(0..self.base.meta.count);
        Some((self.keys[idx as usize], self.children[idx as usize]))
    }
}
