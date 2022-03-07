use crate::{
    base_node::{BaseNode, Node, NodeType},
    child_ptr::NodePtr,
};

pub(crate) const EMPTY_MARKER: u8 = 48;

#[repr(C)]
pub(crate) struct Node48 {
    base: BaseNode,

    pub(crate) child_idx: [u8; 256],
    children: [NodePtr; 48],
}

impl Node for Node48 {
    fn get_type() -> NodeType {
        NodeType::N48
    }

    fn remove(&mut self, k: u8) {
        debug_assert!(self.child_idx[k as usize] != EMPTY_MARKER);
        self.children[self.child_idx[k as usize] as usize] = NodePtr::from_null();
        self.child_idx[k as usize] = EMPTY_MARKER;
        self.base.count -= 1;
        debug_assert!(self.get_child(k).is_none());
    }

    fn get_children(&self, start: u8, end: u8) -> Vec<(u8, NodePtr)> {
        let mut children = Vec::with_capacity(48);

        for (i, c) in self
            .child_idx
            .iter()
            .take(end as usize + 1)
            .skip(start as usize)
            .enumerate()
        {
            if *c != EMPTY_MARKER {
                children.push((i as u8 + start, self.children[*c as usize]));
            }
        }

        children
    }

    fn copy_to<N: Node>(&self, dst: &mut N) {
        for (i, c) in self.child_idx.iter().enumerate() {
            if *c != EMPTY_MARKER {
                dst.insert(i as u8, self.children[*c as usize]);
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

    fn insert(&mut self, key: u8, node: NodePtr) {
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

    fn change(&mut self, key: u8, val: NodePtr) {
        self.children[self.child_idx[key as usize] as usize] = val;
    }

    fn get_child(&self, key: u8) -> Option<NodePtr> {
        if self.child_idx[key as usize] == EMPTY_MARKER {
            None
        } else {
            Some(self.children[self.child_idx[key as usize] as usize])
        }
    }
}
