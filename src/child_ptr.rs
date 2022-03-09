use crate::base_node::BaseNode;

const TID_MASK: usize = 0x8000_0000_0000_0000;

// Tid: hightest bit set
// ChildNode: not set
#[derive(Clone, Copy)]
pub(crate) struct NodePtr {
    val: usize,
}

impl NodePtr {
    #[inline]
    pub(crate) fn from_null() -> Self {
        Self { val: 0 }
    }

    #[inline]
    pub(crate) fn from_node(ptr: *const BaseNode) -> Self {
        Self { val: ptr as usize }
    }

    #[inline]
    pub(crate) fn from_tid(tid: usize) -> Self {
        Self {
            val: tid | TID_MASK,
        }
    }

    #[inline]
    pub(crate) fn as_tid(&self) -> usize {
        self.val & !TID_MASK
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *const BaseNode {
        self.val as *const BaseNode
    }

    #[inline]
    pub(crate) fn is_null(&self) -> bool {
        self.val == 0
    }
}
