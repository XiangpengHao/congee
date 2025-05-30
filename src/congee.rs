use std::{marker::PhantomData, sync::Arc};

use crate::{Allocator, DefaultAllocator, RawCongee, epoch, error::OOMError, stats};

pub struct U64Congee<
    V: Clone + From<usize> + Into<usize>,
    A: Allocator + Clone + Send + 'static = DefaultAllocator,
> {
    inner: RawCongee<8, A>,
    pt_val: PhantomData<V>,
}

impl<V: Clone + From<usize> + Into<usize>> Default for U64Congee<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Clone + From<usize> + Into<usize>> U64Congee<V> {
    pub fn new() -> Self {
        Self {
            inner: RawCongee::new(DefaultAllocator {}, Arc::new(|_k, _v| {})),
            pt_val: PhantomData,
        }
    }

    pub fn get(&self, key: u64, guard: &epoch::Guard) -> Option<V> {
        let key: [u8; 8] = key.to_be_bytes();
        let v = self.inner.get(&key, guard)?;
        Some(V::from(v))
    }

    pub fn insert(&self, key: u64, val: V, guard: &epoch::Guard) -> Result<Option<V>, OOMError> {
        let key: [u8; 8] = key.to_be_bytes();
        let val = val.into();
        self.inner
            .insert(&key, val, guard)
            .map(|v| v.map(|v| V::from(v)))
    }

    pub fn remove(&self, key: u64, guard: &epoch::Guard) -> Option<V> {
        let key: [u8; 8] = key.to_be_bytes();
        let (old, new) = self.inner.compute_if_present(&key, &mut |_v| None, guard)?;
        debug_assert!(new.is_none());
        Some(V::from(old))
    }

    pub fn range(
        &self,
        start: u64,
        end: u64,
        result: &mut [([u8; 8], usize)],
        guard: &epoch::Guard,
    ) -> usize {
        let start: [u8; 8] = start.to_be_bytes();
        let end: [u8; 8] = end.to_be_bytes();
        self.inner.range(&start, &end, result, guard)
    }
}

/// The adaptive radix tree.
pub struct Congee<
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    A: Allocator + Clone + Send + 'static = DefaultAllocator,
> where
    usize: From<K>,
    usize: From<V>,
{
    inner: RawCongee<8, A>,
    pt_key: PhantomData<K>,
    pt_val: PhantomData<V>,
}

impl<K: Copy + From<usize>, V: Copy + From<usize>> Default for Congee<K, V>
where
    usize: From<K>,
    usize: From<V>,
{
    fn default() -> Self {
        Self::new(DefaultAllocator {})
    }
}

impl<K: Copy + From<usize>, V: Copy + From<usize>, A: Allocator + Clone + Send> Congee<K, V, A>
where
    usize: From<K>,
    usize: From<V>,
{
    /// Returns a copy of the value corresponding to the key.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// let tree = Congee::default();
    /// let guard = tree.pin();
    ///
    /// tree.insert(1, 42, &guard);
    /// assert_eq!(tree.get(&1, &guard).unwrap(), 42);
    /// ```
    #[inline]
    pub fn get(&self, key: &K, guard: &epoch::Guard) -> Option<V> {
        let key = usize::from(*key);
        let key: [u8; 8] = key.to_be_bytes();
        let v = self.inner.get(&key, guard)?;
        Some(V::from(v))
    }

    /// Enters an epoch.
    /// Note: this can be expensive, try to reuse it.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// let tree = Congee::<usize, usize>::default();
    /// let guard = tree.pin();
    /// ```
    #[inline]
    pub fn pin(&self) -> epoch::Guard {
        crossbeam_epoch::pin()
    }

    /// Create an empty [Art] tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// let tree = Congee::<usize, usize>::default();
    /// ```
    #[inline]
    pub fn new(allocator: A) -> Self {
        Self::new_with_drainer(allocator, |_k, _v| {})
    }

    /// Create an empty [Art] tree with a drainer.
    ///
    /// The drainer is called on each of the value when the tree is dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::{Congee, DefaultAllocator};
    /// use std::sync::Arc;
    ///
    /// let deleted_key = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    /// let deleted_value = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    /// let deleted_key_inner = deleted_key.clone();
    /// let deleted_value_inner = deleted_value.clone();
    ///
    /// let drainer = move |k: usize, v: usize| {
    ///     deleted_key_inner.store(k, std::sync::atomic::Ordering::Relaxed);
    ///     deleted_value_inner.store(v, std::sync::atomic::Ordering::Relaxed);
    /// };
    ///
    /// let tree = Congee::<usize, usize>::new_with_drainer(DefaultAllocator {}, drainer);
    /// let pin = tree.pin();
    /// tree.insert(1, 42, &pin).unwrap();
    /// drop(tree);
    /// assert_eq!(deleted_key.load(std::sync::atomic::Ordering::Relaxed), 1);
    /// assert_eq!(deleted_value.load(std::sync::atomic::Ordering::Relaxed), 42);
    /// ```
    pub fn new_with_drainer(allocator: A, drainer: impl Fn(K, V) + 'static) -> Self {
        let drainer = Arc::new(move |k: [u8; 8], v: usize| {
            drainer(K::from(usize::from_be_bytes(k)), V::from(v))
        });
        Congee {
            inner: RawCongee::new(allocator, drainer),
            pt_key: PhantomData,
            pt_val: PhantomData,
        }
    }

    /// Returns if the tree is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// let tree = Congee::default();
    /// let guard = tree.pin();
    /// assert!(tree.is_empty(&guard));
    /// tree.insert(1, 42, &guard);
    /// assert!(!tree.is_empty(&guard));
    /// ```
    pub fn is_empty(&self, guard: &epoch::Guard) -> bool {
        self.inner.is_empty(guard)
    }

    /// Removes key-value pair from the tree, returns the value if the key was found.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// let tree = Congee::default();
    /// let guard = tree.pin();
    ///
    /// tree.insert(1, 42, &guard);
    /// let removed = tree.remove(&1, &guard);
    /// assert_eq!(removed, Some(42));
    /// assert!(tree.get(&1, &guard).is_none());
    /// ```
    #[inline]
    pub fn remove(&self, k: &K, guard: &epoch::Guard) -> Option<V> {
        let key = usize::from(*k);
        let key: [u8; 8] = key.to_be_bytes();
        let (old, new) = self.inner.compute_if_present(&key, &mut |_v| None, guard)?;
        debug_assert!(new.is_none());
        Some(V::from(old))
    }

    /// Insert a key-value pair to the tree, returns the previous value if the key was already present.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// let tree = Congee::default();
    /// let guard = tree.pin();
    ///
    /// tree.insert(1, 42, &guard);
    /// assert_eq!(tree.get(&1, &guard).unwrap(), 42);
    /// let old = tree.insert(1, 43, &guard).unwrap();
    /// assert_eq!(old, Some(42));
    /// ```
    #[inline]
    pub fn insert(&self, k: K, v: V, guard: &epoch::Guard) -> Result<Option<V>, OOMError> {
        let key = usize::from(k);
        let key: [u8; 8] = key.to_be_bytes();
        let val = self.inner.insert(&key, usize::from(v), guard);
        val.map(|inner| inner.map(|v| V::from(v)))
    }

    /// Scan the tree with the range of [start, end], write the result to the
    /// `result` buffer.
    /// It scans the length of `result` or the number of the keys within the range, whichever is smaller;
    /// returns the number of the keys scanned.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// let tree = Congee::default();
    /// let guard = tree.pin();
    ///
    /// tree.insert(1, 42, &guard);
    ///
    /// let low_key = 1;
    /// let high_key = 2;
    /// let mut result = [(0, 0); 2];
    /// let scanned = tree.range(&low_key, &high_key, &mut result, &guard);
    /// assert_eq!(scanned, 1);
    /// assert_eq!(result, [(1, 42), (0, 0)]);
    /// ```
    #[inline]
    pub fn range(
        &self,
        start: &K,
        end: &K,
        result: &mut [(usize, usize)],
        guard: &epoch::Guard,
    ) -> usize {
        let start = usize::from(*start);
        let end = usize::from(*end);
        let start: [u8; 8] = start.to_be_bytes();
        let end: [u8; 8] = end.to_be_bytes();
        let result_ref = unsafe {
            std::slice::from_raw_parts_mut(
                result.as_mut_ptr() as *mut ([u8; 8], usize),
                result.len(),
            )
        };
        let v = self.inner.range(&start, &end, result_ref, guard);
        for i in 0..v {
            result[i].0 = usize::from_be_bytes(result_ref[i].0);
        }
        v
    }

    /// Compute and update the value if the key presents in the tree.
    /// Returns the (old, new) value
    ///
    /// Note that the function `f` is a FnMut and it must be safe to execute multiple times.
    /// The `f` is expected to be short and fast as it will hold a exclusive lock on the leaf node.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// let tree = Congee::default();
    /// let guard = tree.pin();
    ///
    /// tree.insert(1, 42, &guard);
    /// let old = tree.compute_if_present(&1, |v| Some(v+1), &guard).unwrap();
    /// assert_eq!(old, (42, Some(43)));
    /// let val = tree.get(&1, &guard).unwrap();
    /// assert_eq!(val, 43);
    /// ```
    #[inline]
    pub fn compute_if_present<F>(
        &self,
        key: &K,
        mut f: F,
        guard: &epoch::Guard,
    ) -> Option<(usize, Option<usize>)>
    where
        F: FnMut(usize) -> Option<usize>,
    {
        let key = usize::from(*key);
        let key: [u8; 8] = key.to_be_bytes();
        self.inner.compute_if_present(&key, &mut f, guard)
    }

    /// Compute or insert the value if the key is not in the tree.
    /// Returns the Option(old) value
    ///
    /// Note that the function `f` is a FnMut and it must be safe to execute multiple times.
    /// The `f` is expected to be short and fast as it will hold a exclusive lock on the leaf node.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// let tree = Congee::default();
    /// let guard = tree.pin();
    ///
    /// tree.insert(1, 42, &guard);
    /// let old = tree.compute_or_insert(1, |v| v.unwrap() + 1, &guard).unwrap().unwrap();
    /// assert_eq!(old, 42);
    /// let val = tree.get(&1, &guard).unwrap();
    /// assert_eq!(val, 43);
    ///
    /// let old = tree.compute_or_insert(2, |v| {
    ///     assert!(v.is_none());
    ///     2
    /// }, &guard).unwrap();
    /// assert!(old.is_none());
    /// let val = tree.get(&2, &guard).unwrap();
    /// assert_eq!(val, 2);
    /// ```
    pub fn compute_or_insert<F>(
        &self,
        key: K,
        mut f: F,
        guard: &epoch::Guard,
    ) -> Result<Option<V>, OOMError>
    where
        F: FnMut(Option<usize>) -> usize,
    {
        let key = usize::from(key);
        let key: [u8; 8] = key.to_be_bytes();
        let u_val = self.inner.compute_or_insert(&key, &mut f, guard)?;
        Ok(u_val.map(|v| V::from(v)))
    }

    /// Display the internal node statistics
    pub fn stats(&self) -> stats::NodeStats {
        self.inner.stats()
    }

    /// Update the value if the old value matches with the new one.
    /// Returns the current value.
    ///
    /// # Examples:
    /// ```
    /// use congee::Congee;
    /// let tree = Congee::default();
    /// let guard = tree.pin();
    /// tree.insert(1, 42, &guard);
    ///
    ///
    /// let v = tree.compare_exchange(&1, &42, Some(43), &guard).unwrap();
    /// assert_eq!(v, Some(43));
    /// ```
    pub fn compare_exchange(
        &self,
        key: &K,
        old: &V,
        new: Option<V>,
        guard: &epoch::Guard,
    ) -> Result<Option<V>, Option<V>> {
        let key = usize::from(*key);
        let key: [u8; 8] = key.to_be_bytes();
        let new_v = new.map(|v| usize::from(v));
        let mut fc = |v: usize| -> Option<usize> {
            if v == usize::from(*old) {
                new_v
            } else {
                Some(v)
            }
        };
        let v = self.inner.compute_if_present(&key, &mut fc, guard);
        match v {
            Some((actual_old, actual_new)) => {
                if actual_old == usize::from(*old) && actual_new == new_v {
                    Ok(new)
                } else {
                    Err(actual_new.map(|v| V::from(v)))
                }
            }
            None => Err(None),
        }
    }

    /// Retrieve all keys from ART.
    /// Isolation level: read committed.
    ///
    /// # Examples:
    /// ```
    /// use congee::Congee;
    /// let tree = Congee::default();
    /// let guard = tree.pin();
    /// tree.insert(1, 42, &guard);
    /// tree.insert(2, 43, &guard);
    ///
    /// let keys = tree.keys();
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

    /// Returns the allocator used by the tree.
    ///
    /// # Examples:
    /// ```
    /// use congee::Congee;
    /// let tree: Congee<usize, usize> = Congee::default();
    /// let allocator = tree.allocator();
    /// ```
    pub fn allocator(&self) -> &A {
        self.inner.allocator()
    }
}
