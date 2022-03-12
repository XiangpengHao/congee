use crate::{
    base_node::{BaseNode, Node, NodeIter, NodeType},
    child_ptr::NodePtr,
};

#[repr(C)]
#[repr(align(64))]
pub(crate) struct Node256 {
    base: BaseNode,
    key_mask: [u8; 32],
    children: [NodePtr; 256],
}

impl Node256 {
    fn set_mask(&mut self, key: usize) {
        let idx = key / 8;
        let bit = key % 8;
        self.key_mask[idx] |= 1 << bit;
    }

    fn unset_mask(&mut self, key: usize) {
        let idx = key / 8;
        let bit = key % 8;
        self.key_mask[idx] &= !(1 << bit);
    }

    fn get_mask(&self, key: usize) -> bool {
        let idx = key / 8;
        let bit = key % 8;
        let key_mask = self.key_mask[idx];
        key_mask & (1 << bit) != 0
    }
}

pub(crate) struct Node256Iter<'a> {
    start: u8,
    end: u8,
    idx: u16,
    node: &'a Node256,
}

impl<'a> Iterator for Node256Iter<'a> {
    type Item = (u8, NodePtr);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let cur = self.idx + self.start as u16;

            if cur > self.end as u16 {
                return None;
            }

            self.idx += 1;

            if self.node.get_mask(cur as usize) {
                return Some((cur as u8, self.node.children[cur as usize]));
            } else {
                continue;
            }
        }
    }
}

impl Node for Node256 {
    fn get_type() -> NodeType {
        NodeType::N256
    }

    fn get_children(&self, start: u8, end: u8) -> NodeIter {
        NodeIter::N256(Node256Iter {
            start,
            end,
            idx: 0,
            node: self,
        })
    }

    fn copy_to<N: Node>(&self, dst: &mut N) {
        for (i, c) in self.children.iter().enumerate() {
            if self.get_mask(i) {
                dst.insert(i as u8, *c);
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
        self.base.meta.count == 37
    }

    fn insert(&mut self, key: u8, node: NodePtr) {
        self.children[key as usize] = node;
        self.set_mask(key as usize);
        self.base.meta.count += 1;
    }

    fn change(&mut self, key: u8, val: NodePtr) {
        self.children[key as usize] = val;
    }

    fn remove(&mut self, k: u8) {
        self.children[k as usize] = NodePtr::from_null();
        self.unset_mask(k as usize);
        self.base.meta.count -= 1;
    }

    fn get_child(&self, key: u8) -> Option<NodePtr> {
        if self.get_mask(key as usize) {
            Some(self.children[key as usize])
        } else {
            None
        }
    }
}
