use crate::congee::Congee;
use crate::error::{ArtError, OOMError};
use crate::nodes::{BaseNode, NodePtr, PtrType};
use core::cell::Cell;
use core::fmt;
use std::sync::Arc;

const SPIN_LIMIT: u32 = 6;
const YIELD_LIMIT: u32 = 10;

/// Backoff implementation from the Crossbeam, added shuttle instrumentation
pub(crate) struct Backoff {
    step: Cell<u32>,
}

impl Backoff {
    #[inline]
    pub(crate) fn new() -> Self {
        Backoff { step: Cell::new(0) }
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn reset(&self) {
        self.step.set(0);
    }

    #[inline]
    pub(crate) fn spin(&self) {
        for _ in 0..1 << self.step.get().min(SPIN_LIMIT) {
            std::hint::spin_loop();
        }

        if self.step.get() <= SPIN_LIMIT {
            self.step.set(self.step.get() + 1);
        }
        #[cfg(all(feature = "shuttle", test))]
        shuttle::thread::yield_now();
    }

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn snooze(&self) {
        if self.step.get() <= SPIN_LIMIT {
            for _ in 0..1 << self.step.get() {
                std::hint::spin_loop();
            }
        } else {
            #[cfg(all(feature = "shuttle", test))]
            shuttle::thread::yield_now();

            #[cfg(not(all(feature = "shuttle", test)))]
            ::std::thread::yield_now();
        }

        if self.step.get() <= YIELD_LIMIT {
            self.step.set(self.step.get() + 1);
        }
    }

    #[inline]
    pub(crate) fn is_completed(&self) -> bool {
        self.step.get() > YIELD_LIMIT
    }
}

impl fmt::Debug for Backoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backoff")
            .field("step", &self.step)
            .field("is_completed", &self.is_completed())
            .finish()
    }
}

impl Default for Backoff {
    fn default() -> Backoff {
        Backoff::new()
    }
}

pub(crate) struct LastLevelKey<'a, const K_LEN: usize> {
    key: &'a KeyTracker<K_LEN>,
}

impl<const K_LEN: usize> LastLevelKey<'_, K_LEN> {
    pub(crate) fn key(&self) -> &[u8; K_LEN] {
        &self.key.data
    }
}

#[derive(Clone)]
pub(crate) struct KeyTracker<const K_LEN: usize> {
    len: usize,
    data: [u8; K_LEN],
}

impl<const K_LEN: usize> KeyTracker<K_LEN> {
    pub(crate) fn empty() -> Self {
        Self {
            len: 0,
            data: [0; K_LEN],
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, key: u8) {
        debug_assert!(self.len <= K_LEN);

        self.data[self.len] = key;
        self.len += 1;
    }

    #[inline]
    pub(crate) fn pop(&mut self) -> u8 {
        debug_assert!(self.len > 0);

        let v = self.data[self.len - 1];
        self.len -= 1;
        v
    }

    pub(crate) unsafe fn as_last_level_unchecked(&self) -> LastLevelKey<K_LEN> {
        LastLevelKey { key: self }
    }

    #[inline]
    pub(crate) fn append_prefix(
        node: NodePtr,
        key_tracker: &KeyTracker<K_LEN>,
    ) -> Result<KeyTracker<K_LEN>, ArtError> {
        match node.downcast_key_tracker::<K_LEN>(key_tracker) {
            PtrType::Payload(_payload) => Ok(key_tracker.clone()),
            PtrType::SubNode(sub_node) => {
                let node_ref = BaseNode::read_lock(sub_node)?;
                let n_prefix = node_ref.as_ref().prefix().iter().skip(key_tracker.len());
                let mut cur_key = key_tracker.clone();
                for i in n_prefix {
                    cur_key.push(*i);
                }
                Ok(cur_key)
            }
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

#[derive(Clone)]
pub struct DefaultAllocator {}

unsafe impl Send for DefaultAllocator {}
unsafe impl Sync for DefaultAllocator {}

/// We should use the `Allocator` trait in the std, but it is not stable yet.
/// https://github.com/rust-lang/rust/issues/32838
pub trait Allocator {
    fn allocate(&self, layout: std::alloc::Layout) -> Result<std::ptr::NonNull<[u8]>, OOMError>;
    fn allocate_zeroed(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, OOMError> {
        let ptr = self.allocate(layout)?;
        unsafe {
            std::ptr::write_bytes(ptr.as_ptr() as *mut u8, 0, layout.size());
        }
        Ok(ptr)
    }
    /// # Safety
    /// The caller must ensure that the pointer is valid and that the layout is correct.
    /// The pointer must allocated by this allocator.
    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout);
}

impl Allocator for DefaultAllocator {
    fn allocate(&self, layout: std::alloc::Layout) -> Result<std::ptr::NonNull<[u8]>, OOMError> {
        let ptr = unsafe { std::alloc::alloc(layout) };
        let ptr_slice = std::ptr::slice_from_raw_parts_mut(ptr, layout.size());
        Ok(std::ptr::NonNull::new(ptr_slice).unwrap())
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        unsafe {
            std::alloc::dealloc(ptr.as_ptr(), layout);
        }
    }
}

struct AllocStats {
    allocated: std::sync::atomic::AtomicUsize,
    deallocated: std::sync::atomic::AtomicUsize,
}

#[derive(Clone)]
pub struct MemoryStatsAllocator<A: Allocator + Clone + Send + 'static = DefaultAllocator> {
    inner: A,
    stats: Arc<AllocStats>,
}

impl<A: Allocator + Clone + Send + 'static> MemoryStatsAllocator<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner,
            stats: Arc::new(AllocStats {
                allocated: std::sync::atomic::AtomicUsize::new(0),
                deallocated: std::sync::atomic::AtomicUsize::new(0),
            }),
        }
    }
}

impl<A: Allocator + Clone + Send + 'static> Allocator for MemoryStatsAllocator<A> {
    fn allocate(&self, layout: std::alloc::Layout) -> Result<std::ptr::NonNull<[u8]>, OOMError> {
        let ptr = self.inner.allocate(layout)?;
        self.stats
            .allocated
            .fetch_add(layout.size(), std::sync::atomic::Ordering::Relaxed);
        Ok(ptr)
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        self.stats
            .deallocated
            .fetch_add(layout.size(), std::sync::atomic::Ordering::Relaxed);
        unsafe { self.inner.deallocate(ptr, layout) }
    }
}

impl<K, V, A: Allocator + Clone + Send + 'static> Congee<K, V, MemoryStatsAllocator<A>>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    pub fn allocated_memory(&self) -> usize {
        self.allocator()
            .stats
            .allocated
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn deallocated_memory(&self) -> usize {
        self.allocator()
            .stats
            .deallocated
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
pub(crate) mod leak_check {
    use super::*;

    use crate::error::OOMError;
    use crate::{Allocator, DefaultAllocator};
    use std::collections::HashSet;
    use std::ptr::NonNull;
    use std::sync::{Arc, Mutex};

    struct LeakCheckAllocatorInner {
        allocated: Mutex<HashSet<NonNull<BaseNode>>>,
        inner: DefaultAllocator,
    }

    unsafe impl Send for LeakCheckAllocatorInner {}
    unsafe impl Sync for LeakCheckAllocatorInner {}

    impl LeakCheckAllocatorInner {
        pub fn new() -> Self {
            Self {
                allocated: Mutex::new(HashSet::new()),
                inner: DefaultAllocator {},
            }
        }
    }

    impl Drop for LeakCheckAllocatorInner {
        fn drop(&mut self) {
            let allocated = self.allocated.lock().unwrap();

            if allocated.len() > 0 {
                println!("Memory leak detected, leaked: {:?}", allocated.len());
                for ptr in allocated.iter() {
                    let node = BaseNode::read_lock(ptr.clone()).unwrap();
                    println!("Ptr address: {:?}", ptr);
                    println!("{:?}", node.as_ref());
                    for (k, v) in node.as_ref().get_children(0, 255) {
                        println!("{:?} {:?}", k, v);
                    }
                }
                panic!("Memory leak detected, see above for details!");
            }
        }
    }

    #[derive(Clone)]
    pub(crate) struct LeakCheckAllocator {
        inner: Arc<LeakCheckAllocatorInner>,
    }

    impl LeakCheckAllocator {
        pub fn new() -> Self {
            Self {
                inner: Arc::new(LeakCheckAllocatorInner::new()),
            }
        }
    }

    impl Allocator for LeakCheckAllocator {
        fn allocate(
            &self,
            layout: std::alloc::Layout,
        ) -> Result<std::ptr::NonNull<[u8]>, OOMError> {
            let ptr = self.inner.inner.allocate(layout)?;
            self.inner
                .allocated
                .lock()
                .unwrap()
                .insert(NonNull::new(ptr.as_ptr() as *mut BaseNode).unwrap());
            Ok(ptr)
        }

        unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
            self.inner
                .allocated
                .lock()
                .unwrap()
                .remove(&NonNull::new(ptr.as_ptr() as *mut BaseNode).unwrap());
            unsafe {
                self.inner.inner.deallocate(ptr, layout);
            }
        }
    }
}
