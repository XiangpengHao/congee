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
            self.keys.copy_within(pos + 1..self.base.meta.count(), pos);
            self.children
                .copy_within(pos + 1..self.base.meta.count(), pos);

            self.base.meta.dec_count();
        }
    }

    fn get_children(&self, start: u8, end: u8) -> NodeIter {
        NodeIter::N4(Node4Iter {
            start,
            end,
            idx: 0,
            cnt: self.base.meta.count() as u8,
            node: self,
        })
    }

    fn copy_to<N: Node>(&self, dst: &mut N) {
        for i in 0..self.base.meta.count() {
            dst.insert(self.keys[i], self.children[i]);
        }
    }

    fn base(&self) -> &BaseNode {
        &self.base
    }

    fn is_full(&self) -> bool {
        self.base.meta.count() == 4
    }

    fn insert(&mut self, key: u8, node: NodePtr) {
        let mut pos: usize = 0;

        while pos < self.base.meta.count() {
            if self.keys[pos] < key {
                pos += 1;
                continue;
            } else {
                break;
            }
        }

        if pos < self.base.meta.count() {
            self.keys.copy_within(pos..self.base.meta.count(), pos + 1);
            self.children
                .copy_within(pos..self.base.meta.count(), pos + 1);
        }

        self.keys[pos] = key;
        self.children[pos] = node;
        self.base.meta.inc_count();
    }

    fn change(&mut self, key: u8, val: NodePtr) -> NodePtr {
        for i in 0..self.base.meta.count() {
            if self.keys[i] == key {
                let old = self.children[i];
                self.children[i] = val;
                return old;
            }
        }
        unreachable!("The key should always exist in the node");
    }

    #[inline]
    fn get_child(&self, key: u8) -> Option<NodePtr> {
        // Manually unrolled loop for better performance on small arrays
        let count = self.base.meta.count();

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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node() -> Node4 {
        Node4 {
            base: BaseNode::new(NodeType::N4, &[]),
            keys: [0; 4],
            children: [NodePtr::from_payload(0); 4],
        }
    }

    #[test]
    fn test_node_operations() {
        let mut node = create_test_node();
        let ptr1 = NodePtr::from_payload(0x1000);
        let ptr2 = NodePtr::from_payload(0x2000);
        let ptr3 = NodePtr::from_payload(0x3000);

        assert_eq!(Node4::get_type(), NodeType::N4);
        assert!(!node.is_full());
        assert_eq!(node.base().meta.count(), 0);

        node.insert(20, ptr2);
        node.insert(10, ptr1);
        node.insert(30, ptr3);

        assert_eq!(node.base().meta.count(), 3);
        assert_eq!(node.keys[0], 10); // Should be sorted
        assert_eq!(node.keys[1], 20);
        assert_eq!(node.keys[2], 30);

        assert!(matches!(node.get_child(10), Some(_)));
        assert!(matches!(node.get_child(20), Some(_)));
        assert!(matches!(node.get_child(30), Some(_)));
        assert!(node.get_child(15).is_none());

        assert!(!node.is_full());
        node.insert(40, NodePtr::from_payload(0x4000));
        assert!(node.is_full());

        let new_ptr = NodePtr::from_payload(0x5000);
        let _old_ptr = node.change(10, new_ptr);
        assert!(matches!(node.get_child(10), Some(_)));
        assert_eq!(node.base().meta.count(), 4); // Count unchanged

        node.remove(20);
        assert_eq!(node.base().meta.count(), 3);
        assert!(node.get_child(20).is_none());
        assert_eq!(node.keys[1], 30); // Elements shifted
    }

    #[test]
    fn test_iterators_and_copy() {
        let mut src_node = create_test_node();
        let mut dst_node = create_test_node();
        let ptr1 = NodePtr::from_payload(0x1000);
        let ptr2 = NodePtr::from_payload(0x2000);

        src_node.insert(10, ptr1);
        src_node.insert(30, ptr2);

        // Test get_children (full range)
        let iter = src_node.get_children(0, 255);
        if let NodeIter::N4(mut n4_iter) = iter {
            let first = n4_iter.next();
            assert!(matches!(first, Some((10, _))));
            let second = n4_iter.next();
            assert!(matches!(second, Some((30, _))));
            assert!(n4_iter.next().is_none());
        } else {
            panic!("Expected N4 iterator");
        }

        // Test get_children (partial range)
        let iter = src_node.get_children(15, 35);
        if let NodeIter::N4(mut n4_iter) = iter {
            let first = n4_iter.next();
            assert!(matches!(first, Some((30, _))));
            assert!(n4_iter.next().is_none());
        }

        // Test copy_to
        src_node.copy_to(&mut dst_node);
        assert_eq!(dst_node.base().meta.count(), 2);
        assert!(dst_node.get_child(10).is_some());
        assert!(dst_node.get_child(30).is_some());
    }

    #[test]
    fn test_edge_cases() {
        let mut node = create_test_node();
        let ptr1 = NodePtr::from_payload(0x1000);

        node.insert(10, ptr1);
        let original_count = node.base().meta.count();
        node.remove(20);
        assert_eq!(node.base().meta.count(), original_count);

        let mut full_node = create_test_node();
        full_node.insert(10, NodePtr::from_payload(0x1000));
        full_node.insert(20, NodePtr::from_payload(0x2000));
        full_node.insert(30, NodePtr::from_payload(0x3000));
        full_node.insert(40, NodePtr::from_payload(0x4000));

        assert!(full_node.get_child(10).is_some());
        assert!(full_node.get_child(20).is_some());
        assert!(full_node.get_child(30).is_some());
        assert!(full_node.get_child(40).is_some());
        assert!(full_node.get_child(50).is_none());
    }
}
