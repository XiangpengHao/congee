use std::cell::UnsafeCell;
use std::sync::atomic::AtomicUsize;

#[allow(dead_code)]
struct NodeLock<T> {
    pub(crate) type_version_lock_obsolete: AtomicUsize,
    node_data: UnsafeCell<T>,
}
