use std::{fmt::Debug, ptr::NonNull};

use crate::{
    Allocator,
    nodes::{BaseNode, Node},
};

pub(crate) struct ChildIsPayload<'a> {
    _marker: std::marker::PhantomData<&'a ()>,
}

impl ChildIsPayload<'_> {
    pub(crate) fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

pub(crate) struct ChildIsSubNode<'a> {
    _marker: std::marker::PhantomData<&'a ()>,
}

impl ChildIsSubNode<'_> {
    pub(crate) fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

const HIGH_BIT_MASK: usize = 1 << (usize::BITS - 1);

/// Macro for pattern matching on NodePtr.
///
/// # Usage
///
/// ```ignore
/// use congee::cast_ptr;
///
/// let result = cast_ptr!(node_ptr => {
///     Payload(val) => {
///         println!("Found payload: {}", val);
///         val * 2
///     },
///     SubNode(ptr) => {
///         println!("Found subnode at: {:?}", ptr.as_ptr());
///         0
///     }
/// });
/// ```
#[macro_export]
macro_rules! cast_ptr {
    ($node_ptr:expr => {
        Payload($payload_var:pat) => $payload_expr:expr,
        SubNode($subnode_var:pat) => $subnode_expr:expr $(,)?
    }) => {{
        let __node_ptr = $node_ptr;
        if __node_ptr.is_payload() {
            let $payload_var = unsafe { __node_ptr.as_payload_unchecked() };
            $payload_expr
        } else {
            let $subnode_var = unsafe { __node_ptr.as_sub_node_unchecked() };
            $subnode_expr
        }
    }};

    ($node_ptr:expr => {
        SubNode($subnode_var:pat) => $subnode_expr:expr,
        Payload($payload_var:pat) => $payload_expr:expr $(,)?
    }) => {{
        let __node_ptr = $node_ptr;
        if __node_ptr.is_payload() {
            let $payload_var = unsafe { __node_ptr.as_payload_unchecked() };
            $payload_expr
        } else {
            let $subnode_var = unsafe { __node_ptr.as_sub_node_unchecked() };
            $subnode_expr
        }
    }};
}

/// A pointer to a node in the tree.
///
/// Use `cast_ptr!` macro to pattern match on it.
#[derive(Clone, Copy)]
pub(crate) struct NodePtr {
    val: usize,
}

impl Debug for NodePtr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        cast_ptr!(self => {
            Payload(val) => {
                write!(f, "Payload: {val:?}")
            },
            SubNode(ptr) => {
                write!(f, "SubNode: {ptr:?}")
            }
        })
    }
}

impl NodePtr {
    pub(crate) fn from_payload(val: usize) -> Self {
        Self { val }
    }

    pub(crate) fn from_node(ptr: NonNull<BaseNode>) -> Self {
        Self {
            val: ptr.as_ptr() as usize | HIGH_BIT_MASK,
        }
    }

    pub(crate) fn from_node_ref(ptr: &BaseNode) -> Self {
        Self {
            val: ptr as *const _ as usize | HIGH_BIT_MASK,
        }
    }

    pub(crate) fn is_payload(&self) -> bool {
        self.val & HIGH_BIT_MASK == 0
    }

    pub(crate) unsafe fn as_payload_unchecked(&self) -> usize {
        self.val
    }

    pub(crate) unsafe fn as_sub_node_unchecked(&self) -> NonNull<BaseNode> {
        unsafe { NonNull::new_unchecked((self.val & !HIGH_BIT_MASK) as *mut BaseNode) }
    }
}

/// This is a wrapper around a node that is allocated on the heap.
/// User need to explicitly call into_note_ptr to convert it to a NodePtr,
/// otherwise the node will be deallocated when the AllocatedNode is dropped.
///
/// This prevents the memory leak if the node is allocated but not used (e.g., due to concurrent insertions)
pub(crate) struct AllocatedNode<'a, N: Node, A: Allocator> {
    ptr: NonNull<N>,
    allocator: &'a A,
}

impl<'a, N: Node, A: Allocator> AllocatedNode<'a, N, A> {
    pub(crate) fn new(ptr: NonNull<N>, allocator: &'a A) -> Self {
        Self { ptr, allocator }
    }

    pub(crate) fn as_mut(&mut self) -> &mut N {
        unsafe { self.ptr.as_mut() }
    }

    pub(crate) fn into_note_ptr(self) -> NodePtr {
        let ptr = self.ptr;
        std::mem::forget(self);
        unsafe { NodePtr::from_node(std::mem::transmute::<NonNull<N>, NonNull<BaseNode>>(ptr)) }
    }

    pub(crate) fn into_non_null(self) -> NonNull<N> {
        let ptr = self.ptr;
        std::mem::forget(self);
        ptr
    }
}

impl<'a, N: Node, A: Allocator> Drop for AllocatedNode<'a, N, A> {
    fn drop(&mut self) {
        unsafe {
            std::ptr::drop_in_place(self.ptr.as_mut());
            let ptr = std::ptr::NonNull::new(self.ptr.as_ptr() as *mut u8).unwrap();
            let layout = N::get_type().node_layout();
            self.allocator.deallocate(ptr, layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cast_ptr;
    use crate::nodes::{BaseNode, Node, Node4};
    use crate::{Allocator, Congee, DefaultAllocator, MemoryStatsAllocator}; // Import the macro

    #[test]
    fn test_deallocator_called_on_drop() {
        let stats_allocator = MemoryStatsAllocator::new(DefaultAllocator {});
        let tree: Congee<usize, usize, _> = Congee::new(stats_allocator);

        let allocated_before = tree.allocated_bytes();
        let deallocated_before = tree.deallocated_bytes();

        // Create an AllocatedNode which should trigger allocation
        let allocated_node = {
            BaseNode::make_node::<Node4, _>(&[], tree.allocator()).expect("Failed to allocate node")
        };

        let allocated_after_creation = tree.allocated_bytes();
        let deallocated_after_creation = tree.deallocated_bytes();

        // Verify that memory was allocated but not yet deallocated
        assert!(
            allocated_after_creation > allocated_before,
            "Memory should have been allocated"
        );
        assert_eq!(
            deallocated_after_creation, deallocated_before,
            "Memory should not have been deallocated yet"
        );

        // Drop the AllocatedNode explicitly to trigger the deallocator
        drop(allocated_node);

        let allocated_after_drop = tree.allocated_bytes();
        let deallocated_after_drop = tree.deallocated_bytes();

        // Verify that the deallocator was called
        assert_eq!(
            allocated_after_drop, allocated_after_creation,
            "Allocated memory should not change after drop"
        );
        assert!(
            deallocated_after_drop > deallocated_after_creation,
            "Memory should have been deallocated"
        );

        // The amount deallocated should equal the amount allocated for this node
        let node_size = allocated_after_creation - allocated_before;
        let deallocated_size = deallocated_after_drop - deallocated_before;
        assert_eq!(
            node_size, deallocated_size,
            "Deallocated size should match allocated size"
        );
    }

    #[test]
    fn test_into_node_ptr_prevents_deallocation() {
        let stats_allocator = MemoryStatsAllocator::new(DefaultAllocator {});
        let tree: Congee<usize, usize, _> = Congee::new(stats_allocator);

        let deallocated_before = tree.deallocated_bytes();

        let allocated_node = BaseNode::make_node::<Node4, _>(&[], tree.allocator())
            .expect("Failed to allocate node");

        let node_ptr = allocated_node.into_note_ptr();

        let deallocated_after = tree.deallocated_bytes();

        // Verify that no deallocation occurred because into_note_ptr() calls std::mem::forget()
        assert_eq!(
            deallocated_after, deallocated_before,
            "No deallocation should occur when converting to NodePtr because std::mem::forget is called"
        );

        cast_ptr!(node_ptr => {
            Payload(_) => unreachable!(),
            SubNode(sub_node) => unsafe {
                tree.allocator().deallocate(
                    std::ptr::NonNull::new(sub_node.as_ptr() as *mut u8).unwrap(),
                    Node4::get_type().node_layout(),
                );
            }
        });
    }

    #[test]
    fn test_downcast_macro_with_payload() {
        let payload_value = 42usize;
        let node_ptr = super::NodePtr::from_payload(payload_value);

        let result = cast_ptr!(node_ptr => {
            Payload(val) => {
                format!("Payload: {}", val)
            },
            SubNode(ptr) => {
                format!("SubNode: {:?}", ptr)
            }
        });

        assert_eq!(result, "Payload: 42");
    }
}
