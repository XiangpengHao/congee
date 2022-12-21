use std::{
    alloc::AllocError,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{node_256::Node256, node_4::Node4, Allocator, Art};

struct SmallAllocatorInner {
    max_size: AtomicUsize,
}

#[derive(Clone)]
struct SmallAllocator(Arc<SmallAllocatorInner>);

impl SmallAllocator {
    fn new(max_size: usize) -> Self {
        Self(Arc::new(SmallAllocatorInner {
            max_size: AtomicUsize::new(max_size),
        }))
    }
}

unsafe impl Allocator for SmallAllocator {
    fn allocate(&self, layout: std::alloc::Layout) -> Result<std::ptr::NonNull<[u8]>, AllocError> {
        let current_size = self.0.max_size.load(Ordering::Relaxed);
        if current_size >= layout.size() {
            self.0
                .max_size
                .store(current_size - layout.size(), Ordering::Relaxed);
            let ptr = unsafe { std::alloc::alloc(layout) };
            let ptr_slice = std::ptr::slice_from_raw_parts_mut(ptr, layout.size());
            Ok(std::ptr::NonNull::new(ptr_slice).unwrap())
        } else {
            return Err(AllocError);
        }
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        std::alloc::dealloc(ptr.as_ptr(), layout);
    }
}

#[should_panic]
#[test]
fn too_small_to_new() {
    let allocator = SmallAllocator::new(1024);
    let _art = Art::<usize, usize, SmallAllocator>::new(allocator.clone());
}

#[test]
fn init_but_no_insert() {
    let allocator = SmallAllocator::new(std::mem::size_of::<Node256>());
    let art = Art::<usize, usize, SmallAllocator>::new(allocator.clone());
    let guard = art.pin();
    let rv = art.insert(100, 100, &guard);
    assert!(rv.is_err());

    let rv = art.compute_or_insert(100, |_| 100, &guard);
    assert!(rv.is_err());
}

#[test]
fn insert_but_only_once() {
    let allocator =
        SmallAllocator::new(std::mem::size_of::<Node256>() + std::mem::size_of::<Node4>());
    let art = Art::<usize, usize, SmallAllocator>::new(allocator.clone());
    let guard = art.pin();
    let rv = art.insert(0, 100, &guard);
    assert!(rv.is_ok());

    let rv = art.insert(usize::MAX, 100, &guard);
    assert!(rv.is_err());
}
