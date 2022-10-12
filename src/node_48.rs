use crate::{
    base_node::{BaseNode, Node, NodeIter, NodeType},
    node_ptr::NodePtr,
};

pub(crate) const EMPTY_MARKER: u8 = 48;

#[repr(C)]
#[repr(align(8))]
pub(crate) struct Node48 {
    base: BaseNode,

    pub(crate) child_idx: [u8; 256],
    next_empty: u8,
    children: [NodePtr; 48],
}

#[cfg(all(test, not(feature = "shuttle")))]
mod const_assert {
    use super::*;
    static_assertions::const_assert_eq!(std::mem::size_of::<Node48>(), 672);
    static_assertions::const_assert_eq!(std::mem::align_of::<Node48>(), 8);
}

impl Node48 {
    pub(crate) fn init_empty(&mut self) {
        for v in self.child_idx.iter_mut() {
            *v = EMPTY_MARKER;
        }
        self.next_empty = 0;
        for (i, child) in self.children.iter_mut().enumerate() {
            *child = NodePtr::from_tid(i + 1);
        }
    }
}

pub(crate) struct Node48Iter<'a> {
    start: u16,
    end: u16,
    node: &'a Node48,
}

impl<'a> Iterator for Node48Iter<'a> {
    type Item = (u8, NodePtr);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.start > self.end {
                return None;
            }

            let key = self.start as usize;
            self.start += 1;

            let child_loc = self.node.child_idx[key];
            if child_loc != EMPTY_MARKER {
                return Some((key as u8, self.node.children[child_loc as usize]));
            }
        }
    }
}

impl Node for Node48 {
    fn get_type() -> NodeType {
        NodeType::N48
    }

    fn remove(&mut self, k: u8) {
        debug_assert!(self.child_idx[k as usize] != EMPTY_MARKER);
        let pos = self.child_idx[k as usize];
        self.children[pos as usize] = NodePtr::from_tid(self.next_empty as usize);
        self.child_idx[k as usize] = EMPTY_MARKER;
        self.next_empty = pos;
        self.base.meta.count -= 1;
        debug_assert!(self.get_child(k).is_none());
    }

    fn get_children(&self, start: u8, end: u8) -> NodeIter {
        NodeIter::N48(Node48Iter {
            start: start as u16,
            end: end as u16,
            node: self,
        })
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
        self.base.meta.count == 48
    }

    fn insert(&mut self, key: u8, node: NodePtr) {
        let pos = self.next_empty as usize;
        self.next_empty = self.children[pos].as_tid() as u8;

        debug_assert!(pos < 48);

        self.children[pos] = node;
        self.child_idx[key as usize] = pos as u8;
        self.base.meta.count += 1;
    }

    fn change(&mut self, key: u8, val: NodePtr) -> NodePtr {
        let old = self.children[self.child_idx[key as usize] as usize];
        self.children[self.child_idx[key as usize] as usize] = val;
        old
    }

    fn get_child(&self, key: u8) -> Option<NodePtr> {
        if self.child_idx[key as usize] == EMPTY_MARKER {
            None
        } else {
            let child = self.children[self.child_idx[key as usize] as usize];

            #[cfg(all(target_feature = "sse2", not(miri)))]
            {
                let ptr = child.as_ptr();
                use core::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
                unsafe {
                    _mm_prefetch(ptr as *const i8, _MM_HINT_T0);
                }
            }

            Some(child)
        }
    }

    #[cfg(feature = "db_extension")]
    fn get_random_child(&self, rng: &mut impl rand::Rng) -> Option<(u8, NodePtr)> {
        if self.base.meta.count == 0 {
            return None;
        }

        let mut scan_cnt = rng.gen_range(1..=self.base.meta.count);
        let mut idx = 0;
        while scan_cnt > 0 {
            if self.child_idx[idx] != EMPTY_MARKER {
                scan_cnt -= 1;
            }
            idx += 1;
        }

        Some((
            (idx - 1) as u8,
            self.children[self.child_idx[idx - 1] as usize],
        ))
    }
}
