use std::{marker::PhantomData, sync::Arc};

use crate::{Allocator, DefaultAllocator, RawCongee, epoch, error::OOMError, stats};

/// A concurrent set-like data structure implemented using an adaptive radix tree.
pub struct CongeeSet<
    K: Copy + From<usize>,
    A: Allocator + Clone + Send + 'static = DefaultAllocator,
> where
    usize: From<K>,
{
    inner: RawCongee<8, A>,
    pt_key: PhantomData<K>,
}

impl<K: Copy + From<usize>> Default for CongeeSet<K>
where
    usize: From<K>,
{
    fn default() -> Self {
        Self::new(DefaultAllocator {})
    }
}

impl<K: Copy + From<usize>, A: Allocator + Clone + Send> CongeeSet<K, A>
where
    usize: From<K>,
{
    /// Creates a new empty CongeeSet.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeSet;
    /// let set = CongeeSet::<usize>::default();
    /// ```
    #[inline]
    pub fn new(allocator: A) -> Self {
        Self::new_with_drainer(allocator, |_k| {})
    }

    /// Creates a new empty CongeeSet with a drainer.
    ///
    /// The drainer is called on each key when the set is dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::{CongeeSet, DefaultAllocator};
    /// use std::sync::Arc;
    ///
    /// let deleted_key = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    /// let deleted_key_inner = deleted_key.clone();
    ///
    /// let drainer = move |k: usize| {
    ///     deleted_key_inner.store(k, std::sync::atomic::Ordering::Relaxed);
    /// };
    ///
    /// let set = CongeeSet::<usize>::new_with_drainer(DefaultAllocator {}, drainer);
    /// let pin = set.pin();
    /// set.insert(1, &pin).unwrap();
    /// drop(set);
    /// assert_eq!(deleted_key.load(std::sync::atomic::Ordering::Relaxed), 1);
    /// ```
    pub fn new_with_drainer(allocator: A, drainer: impl Fn(K) + 'static) -> Self {
        let drainer =
            Arc::new(move |k: [u8; 8], _v: usize| drainer(K::from(usize::from_be_bytes(k))));
        CongeeSet {
            inner: RawCongee::new(allocator, drainer),
            pt_key: PhantomData,
        }
    }

    /// Enters an epoch.
    /// Note: this can be expensive, try to reuse it.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeSet;
    /// let set = CongeeSet::<usize>::default();
    /// let guard = set.pin();
    /// ```
    #[inline]
    pub fn pin(&self) -> epoch::Guard {
        crossbeam_epoch::pin()
    }

    /// Returns true if the set is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeSet;
    /// let set = CongeeSet::default();
    /// let guard = set.pin();
    /// assert!(set.is_empty(&guard));
    /// set.insert(1, &guard).unwrap();
    /// assert!(!set.is_empty(&guard));
    /// ```
    pub fn is_empty(&self, guard: &epoch::Guard) -> bool {
        self.inner.is_empty(guard)
    }

    /// Checks if the set contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeSet;
    /// let set = CongeeSet::default();
    /// let guard = set.pin();
    ///
    /// set.insert(1, &guard).unwrap();
    /// assert!(set.contains(&1, &guard));
    /// assert!(!set.contains(&2, &guard));
    /// ```
    #[inline]
    pub fn contains(&self, key: &K, guard: &epoch::Guard) -> bool {
        let key = usize::from(*key);
        let key: [u8; 8] = key.to_be_bytes();
        self.inner.get(&key, guard).is_some()
    }

    /// Inserts a key into the set.
    /// Returns true if the key was newly inserted, false if it was already present.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeSet;
    /// let set = CongeeSet::default();
    /// let guard = set.pin();
    ///
    /// assert!(set.insert(1, &guard).unwrap());
    /// assert!(!set.insert(1, &guard).unwrap()); // Already present
    /// ```
    #[inline]
    pub fn insert(&self, k: K, guard: &epoch::Guard) -> Result<bool, OOMError> {
        let key = usize::from(k);
        let key: [u8; 8] = key.to_be_bytes();
        // Use a dummy value (1) since we only care about key presence
        let old = self.inner.insert(&key, 1, guard)?;
        Ok(old.is_none()) // true if newly inserted, false if already present
    }

    /// Removes a key from the set.
    /// Returns true if the key was present and removed, false if it was not present.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeSet;
    /// let set = CongeeSet::default();
    /// let guard = set.pin();
    ///
    /// set.insert(1, &guard).unwrap();
    /// assert!(set.remove(&1, &guard));
    /// assert!(!set.remove(&1, &guard)); // Not present anymore
    /// ```
    #[inline]
    pub fn remove(&self, k: &K, guard: &epoch::Guard) -> bool {
        let key = usize::from(*k);
        let key: [u8; 8] = key.to_be_bytes();
        let (old, new) = match self.inner.compute_if_present(&key, &mut |_v| None, guard) {
            Some(result) => result,
            None => return false, // Key not present
        };
        debug_assert!(new.is_none());
        old == 1 // Should always be 1 for sets, but check to be safe
    }

    /// Retrieves all keys from the set.
    /// Isolation level: read committed.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeSet;
    /// let set = CongeeSet::default();
    /// let guard = set.pin();
    /// set.insert(1, &guard).unwrap();
    /// set.insert(2, &guard).unwrap();
    ///
    /// let mut keys = set.keys();
    /// keys.sort();
    /// assert_eq!(keys, vec![1, 2]);
    /// ```
    pub fn keys(&self) -> Vec<K> {
        self.inner
            .keys()
            .into_iter()
            .map(|k| {
                let key = usize::from_be_bytes(k);
                K::from(key)
            })
            .collect()
    }

    /// Returns the number of keys in the set.
    /// Note: This operation is O(n) as it needs to count all keys.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeSet;
    /// let set = CongeeSet::default();
    /// let guard = set.pin();
    ///
    /// assert_eq!(set.len(&guard), 0);
    /// set.insert(1, &guard).unwrap();
    /// set.insert(2, &guard).unwrap();
    /// assert_eq!(set.len(&guard), 2);
    /// ```
    pub fn len(&self, guard: &epoch::Guard) -> usize {
        self.inner.value_count(guard)
    }

    /// Scans keys in the range [start, end] and writes them to the result buffer.
    /// Returns the number of keys scanned.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeSet;
    /// let set = CongeeSet::default();
    /// let guard = set.pin();
    ///
    /// set.insert(1, &guard).unwrap();
    /// set.insert(3, &guard).unwrap();
    /// set.insert(5, &guard).unwrap();
    ///
    /// let mut result = [0; 10];
    /// let scanned = set.range(&1, &4, &mut result, &guard);
    /// assert_eq!(scanned, 2);
    /// // result now contains [1, 3, 0, 0, ...]
    /// ```
    #[inline]
    pub fn range(&self, start: &K, end: &K, result: &mut [K], guard: &epoch::Guard) -> usize {
        let start_key = usize::from(*start);
        let end_key = usize::from(*end);
        let start_bytes: [u8; 8] = start_key.to_be_bytes();
        let end_bytes: [u8; 8] = end_key.to_be_bytes();

        // Create temporary buffer for (key, value) pairs
        let mut temp_result: Vec<([u8; 8], usize)> = vec![([0; 8], 0); result.len()];
        let scanned = self
            .inner
            .range(&start_bytes, &end_bytes, &mut temp_result, guard);

        // Convert the keys back to K type
        for (i, (key_bytes, _)) in temp_result.iter().enumerate().take(scanned) {
            result[i] = K::from(usize::from_be_bytes(*key_bytes));
        }

        scanned
    }

    /// Display the internal node statistics.
    pub fn stats(&self) -> stats::NodeStats {
        self.inner.stats()
    }

    /// Returns the allocator used by the set.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeSet;
    /// let set: CongeeSet<usize> = CongeeSet::default();
    /// let allocator = set.allocator();
    /// ```
    pub fn allocator(&self) -> &A {
        self.inner.allocator()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let set = CongeeSet::<usize>::default();
        let guard = set.pin();

        // Test empty set
        assert!(set.is_empty(&guard));
        assert_eq!(set.len(&guard), 0);
        assert!(!set.contains(&1, &guard));

        // Test insert
        assert!(set.insert(1, &guard).unwrap());
        assert!(!set.is_empty(&guard));
        assert_eq!(set.len(&guard), 1);
        assert!(set.contains(&1, &guard));

        // Test duplicate insert
        assert!(!set.insert(1, &guard).unwrap());
        assert_eq!(set.len(&guard), 1);

        // Test multiple inserts
        assert!(set.insert(2, &guard).unwrap());
        assert!(set.insert(3, &guard).unwrap());
        assert_eq!(set.len(&guard), 3);
        assert!(set.contains(&2, &guard));
        assert!(set.contains(&3, &guard));

        // Test remove
        assert!(set.remove(&2, &guard));
        assert_eq!(set.len(&guard), 2);
        assert!(!set.contains(&2, &guard));
        assert!(set.contains(&1, &guard));
        assert!(set.contains(&3, &guard));

        // Test remove non-existent
        assert!(!set.remove(&4, &guard));
        assert_eq!(set.len(&guard), 2);
    }

    #[test]
    fn test_keys() {
        let set = CongeeSet::<usize>::default();
        let guard = set.pin();

        // Empty set
        assert!(set.keys().is_empty());

        // Add some keys
        set.insert(3, &guard).unwrap();
        set.insert(1, &guard).unwrap();
        set.insert(2, &guard).unwrap();

        let mut keys = set.keys();
        keys.sort();
        assert_eq!(keys, vec![1, 2, 3]);
    }

    #[test]
    fn test_range() {
        let set = CongeeSet::<usize>::default();
        let guard = set.pin();

        // Insert some keys
        for i in [1, 3, 5, 7, 9] {
            set.insert(i, &guard).unwrap();
        }

        // Test range scan
        let mut result = [0; 10];
        let scanned = set.range(&2, &8, &mut result, &guard);
        assert_eq!(scanned, 3);

        let mut scanned_keys: Vec<usize> = result[..scanned].to_vec();
        scanned_keys.sort();
        assert_eq!(scanned_keys, vec![3, 5, 7]);
    }

    #[test]
    fn test_u64_congee_set() {
        let set = U64CongeeSet::default();
        let guard = set.pin();

        assert!(set.is_empty(&guard));
        assert!(set.insert(42, &guard).unwrap());
        assert!(set.contains(42, &guard));
        assert!(!set.insert(42, &guard).unwrap());

        assert_eq!(set.len(&guard), 1);
        let keys = set.keys();
        assert_eq!(keys, vec![42]);

        assert!(set.remove(42, &guard));
        assert!(set.is_empty(&guard));
        assert!(!set.remove(42, &guard));
    }

    #[test]
    fn test_drainer() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let deleted_key = Arc::new(AtomicUsize::new(0));
        let deleted_key_inner = deleted_key.clone();

        let drainer = move |k: usize| {
            deleted_key_inner.store(k, Ordering::Relaxed);
        };

        {
            let set = CongeeSet::<usize>::new_with_drainer(DefaultAllocator {}, drainer);
            let guard = set.pin();
            set.insert(42, &guard).unwrap();
        } // Set is dropped here

        // Give a moment for the drainer to run
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert_eq!(deleted_key.load(Ordering::Relaxed), 42);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let set = Arc::new(CongeeSet::<usize>::default());
        let mut handles = vec![];

        // Spawn multiple threads to insert values concurrently
        for i in 0..10 {
            let set_clone = set.clone();
            handles.push(thread::spawn(move || {
                let guard = set_clone.pin();
                for j in 0..10 {
                    let key = i * 10 + j;
                    set_clone.insert(key, &guard).unwrap();
                }
            }));
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all keys were inserted
        let guard = set.pin();
        assert_eq!(set.len(&guard), 100);

        for i in 0..100 {
            assert!(set.contains(&i, &guard));
        }
    }
}
