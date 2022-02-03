use crate::base_node::BaseNode;

const TID_MASK: usize = 0x8000_0000_0000_0000;

// Tid: hightest bit set
// ChildNode: not set
#[derive(Clone, Copy)]
pub(crate) struct ChildPtr {
    val: usize,
}

impl ChildPtr {
    pub(crate) fn from_ptr(ptr: *const BaseNode) -> Self {
        Self { val: ptr as usize }
    }

    pub(crate) fn from_tid(tid: usize) -> Self {
        assert!(tid & TID_MASK == 0);
        Self {
            val: tid | TID_MASK,
        }
    }

    pub(crate) fn is_tid(&self) -> bool {
        (self.val & TID_MASK) > 0
    }

    pub(crate) fn is_ptr(&self) -> bool {
        (self.val & TID_MASK) == 0
    }

    pub(crate) fn to_tid(&self) -> usize {
        debug_assert!(self.is_tid());
        self.val & !TID_MASK
    }

    pub(crate) fn to_ptr(&self) -> *const BaseNode {
        debug_assert!(self.is_ptr());
        self.val as *const BaseNode
    }

    /// For compatibility
    pub(crate) fn as_raw(&self) -> *const BaseNode {
        self.val as *const BaseNode
    }

    pub(crate) fn from_raw(ptr: *const BaseNode) -> Self {
        Self { val: ptr as usize }
    }

    pub(crate) fn is_null(&self) -> bool {
        self.val == 0
    }
}
