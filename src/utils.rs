use crate::congee_raw::CongeeRaw;
use crate::error::{ArtError, OOMError};
use crate::nodes::{BaseNode, NodePtr};
use crate::{CongeeSet, cast_ptr};
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

    pub(crate) unsafe fn as_last_level_unchecked(&self) -> LastLevelKey<'_, K_LEN> {
        LastLevelKey { key: self }
    }

    #[inline]
    pub(crate) fn append_prefix(
        node: NodePtr,
        key_tracker: &KeyTracker<K_LEN>,
    ) -> Result<KeyTracker<K_LEN>, ArtError> {
        cast_ptr!(node => {
            Payload(_payload) => Ok(key_tracker.clone()),
            SubNode(sub_node) => {
                let node_ref = BaseNode::read_lock(sub_node)?;
                let n_prefix = node_ref.as_ref().prefix().iter();
                let mut cur_key = key_tracker.clone();
                for i in n_prefix {
                    cur_key.push(*i);
                }
                Ok(cur_key)
            }
        })
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Get the current key data as a slice
    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.data[..self.len]
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

impl<K, V, A: Allocator + Clone + Send + 'static> CongeeRaw<K, V, MemoryStatsAllocator<A>>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    pub fn allocated_bytes(&self) -> usize {
        self.allocator()
            .stats
            .allocated
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn deallocated_bytes(&self) -> usize {
        self.allocator()
            .stats
            .deallocated
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl<K, A: Allocator + Clone + Send + 'static> CongeeSet<K, MemoryStatsAllocator<A>>
where
    K: Copy + From<usize>,
    usize: From<K>,
{
    pub fn allocated_bytes(&self) -> usize {
        self.allocator()
            .stats
            .allocated
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn deallocated_bytes(&self) -> usize {
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

            if !allocated.is_empty() {
                println!("Memory leak detected, leaked: {:?}", allocated.len());
                for ptr in allocated.iter() {
                    let node = BaseNode::read_lock(*ptr).unwrap();
                    println!("Ptr address: {ptr:?}");
                    println!("{:?}", node.as_ref());
                    for (k, v) in node.as_ref().get_children(0, 255) {
                        println!("{k:?} {v:?}");
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

/// Compute precomputed popcount values for 64-bit boundaries in a 256-bit array
pub fn compute_precomputed_popcounts(bits: &[u8; 32]) -> [u8; 4] {
    let mut counts = [0u8; 4];

    counts[0] = bits[0..8].iter().map(|b| b.count_ones() as u8).sum::<u8>();
    counts[1] = bits[0..16].iter().map(|b| b.count_ones() as u8).sum::<u8>();
    counts[2] = bits[0..24].iter().map(|b| b.count_ones() as u8).sum::<u8>();
    counts[3] = bits.iter().map(|b| b.count_ones() as u8).sum::<u8>();

    counts
}

/// Count ones up to position using precomputed values for O(1) lookup
pub fn count_ones_up_to_precomputed(precomputed: &[u8; 4], bits: &[u8; 32], pos: u8) -> usize {
    if pos == 0 {
        return 0;
    }

    let pos = pos as usize;

    match pos {
        1..=64 => {
            let chunk_bytes = &bits[0..8];
            count_bits_in_range(chunk_bytes, 0, pos)
        }
        65..=128 => {
            let base_count = precomputed[0] as usize;
            let remaining_pos = pos - 64;
            let chunk_bytes = &bits[8..16];
            base_count + count_bits_in_range(chunk_bytes, 0, remaining_pos)
        }
        129..=192 => {
            let base_count = precomputed[1] as usize;
            let remaining_pos = pos - 128;
            let chunk_bytes = &bits[16..24];
            base_count + count_bits_in_range(chunk_bytes, 0, remaining_pos)
        }
        193..=256 => {
            let base_count = precomputed[2] as usize;
            let remaining_pos = pos - 192;
            let chunk_bytes = &bits[24..32];
            base_count + count_bits_in_range(chunk_bytes, 0, remaining_pos)
        }
        _ => 0,
    }
}

fn count_bits_in_range(bytes: &[u8], start_bit: usize, end_bit: usize) -> usize {
    if start_bit >= end_bit {
        return 0;
    }

    let mut count = 0;

    for (byte_idx, &byte) in bytes.iter().enumerate() {
        let byte_start_bit = byte_idx * 8;
        let byte_end_bit = byte_start_bit + 8;

        if byte_start_bit >= end_bit {
            break;
        }

        if byte_end_bit <= start_bit {
            continue;
        }

        let count_start = start_bit.saturating_sub(byte_start_bit);
        let count_end = (end_bit - byte_start_bit).min(8);

        if count_start == 0 && count_end == 8 {
            count += byte.count_ones() as usize;
        } else {
            let mask_bits = count_end - count_start;
            let mask = if mask_bits >= 8 {
                0xFF
            } else {
                (1u8 << mask_bits) - 1
            };
            let shifted_mask = mask << count_start;
            count += (byte & shifted_mask).count_ones() as usize;
        }
    }

    count
}

/// Set a bit at the given position in a 256-bit array
#[inline]
pub fn set_bit(bits: &mut [u8; 32], pos: u8) {
    let byte_idx = pos as usize / 8;
    let bit_idx = pos as usize % 8;
    if byte_idx < 32 {
        bits[byte_idx] |= 1u8 << bit_idx;
    }
}

/// Check if a bit is set at the given position in a 256-bit array
#[inline]
pub fn is_bit_set(bits: &[u8; 32], pos: u8) -> bool {
    let byte_idx = pos as usize / 8;
    let bit_idx = pos as usize % 8;
    if byte_idx < 32 {
        bits[byte_idx] & (1u8 << bit_idx) != 0
    } else {
        false
    }
}

#[cfg(test)]
mod bit_utils_tests {
    use super::*;

    #[test]
    fn test_bit_operations() {
        let mut bits = [0u8; 32]; // 256-bit array

        assert!(!is_bit_set(&bits, 10));
        set_bit(&mut bits, 10);
        assert!(is_bit_set(&bits, 10));

        set_bit(&mut bits, 0); // First bit
        set_bit(&mut bits, 255); // Last bit
        assert!(is_bit_set(&bits, 0));
        assert!(is_bit_set(&bits, 255));
    }

    #[test]
    fn test_precomputed_popcounts() {
        let mut bits = [0u8; 32];

        // Set bits across different 64-bit boundaries
        set_bit(&mut bits, 5); // First 64 bits
        set_bit(&mut bits, 10); // First 64 bits
        set_bit(&mut bits, 65); // Second 64 bits
        set_bit(&mut bits, 100); // Second 64 bits
        set_bit(&mut bits, 130); // Third 64 bits
        set_bit(&mut bits, 200); // Fourth 64 bits
        set_bit(&mut bits, 250); // Fourth 64 bits

        let precomputed = compute_precomputed_popcounts(&bits);

        // Check precomputed values
        assert_eq!(precomputed[0], 2); // bits 0..64: positions 5, 10
        assert_eq!(precomputed[1], 4); // bits 0..128: positions 5, 10, 65, 100
        assert_eq!(precomputed[2], 5); // bits 0..192: + position 130
        assert_eq!(precomputed[3], 7); // bits 0..256: + positions 200, 250
    }

    #[test]
    fn test_count_ones_up_to_precomputed() {
        let mut bits = [0u8; 32];

        set_bit(&mut bits, 5);
        set_bit(&mut bits, 10);
        set_bit(&mut bits, 65);
        set_bit(&mut bits, 100);
        set_bit(&mut bits, 130);
        set_bit(&mut bits, 200);
        set_bit(&mut bits, 250);

        let precomputed = compute_precomputed_popcounts(&bits);

        // Test various positions
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 0), 0);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 5), 0);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 6), 1);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 11), 2);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 64), 2);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 66), 3);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 128), 4);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 131), 5);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 192), 5);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 201), 6);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 251), 7);
    }

    #[test]
    fn test_precomputed_boundary_conditions() {
        let mut bits = [0u8; 32];

        set_bit(&mut bits, 63);
        set_bit(&mut bits, 64);
        set_bit(&mut bits, 127);
        set_bit(&mut bits, 128);
        set_bit(&mut bits, 191);
        set_bit(&mut bits, 192);

        let precomputed = compute_precomputed_popcounts(&bits);

        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 63), 0);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 64), 1);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 65), 2);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 127), 2);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 128), 3);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 129), 4);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 191), 4);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 192), 5);
        assert_eq!(count_ones_up_to_precomputed(&precomputed, &bits, 193), 6);
    }
}
