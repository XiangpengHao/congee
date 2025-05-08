use std::{marker::PhantomData, ptr::with_exposed_provenance, sync::Arc};

use crate::{DefaultAllocator, RawCongee, epoch, error::OOMError};

/// A concurrent map-like data structure that uses Arc for reference counting of values.
///
/// CongeeArc provides a way to store Arc-wrapped values in a concurrent tree structure.
/// It automatically manages reference counting when inserting, retrieving, and removing values.
pub struct CongeeArc<K: From<usize> + Copy, V>
where
    usize: From<K>,
{
    inner: Arc<RawCongee<8, DefaultAllocator>>,
    pt_val: PhantomData<V>,
    pt_key: PhantomData<K>,
}

unsafe fn arc_from_usize<V>(v: usize) -> Arc<V> {
    // # Safety
    // The pointer was previously inserted with expose_provenance
    let ptr: *const V = with_exposed_provenance(v);
    unsafe { Arc::from_raw(ptr) }
}

impl<K: From<usize> + Copy, V> Default for CongeeArc<K, V>
where
    usize: From<K>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K: From<usize> + Copy, V> CongeeArc<K, V>
where
    usize: From<K>,
{
    /// Creates a new empty CongeeArc instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeArc;
    /// use std::sync::Arc;
    ///
    /// let tree: CongeeArc<usize, String> = CongeeArc::new();
    /// ```
    pub fn new() -> Self {
        let drainer = |_k: [u8; 8], v: usize| {
            // Safety
            // The pointer was previously inserted with expose_provenance
            let owned = unsafe { arc_from_usize::<V>(v) };
            drop(owned);
        };
        Self {
            inner: Arc::new(RawCongee::new(DefaultAllocator {}, Arc::new(drainer))),
            pt_val: PhantomData,
            pt_key: PhantomData,
        }
    }

    /// Enters an epoch.
    ///
    /// This is necessary before performing operations on the tree.
    /// Note: this can be expensive, try to reuse it.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeArc;
    /// use std::sync::Arc;
    ///
    /// let tree: CongeeArc<usize, String> = CongeeArc::new();
    /// let guard = tree.pin();
    /// ```
    pub fn pin(&self) -> epoch::Guard {
        crossbeam_epoch::pin()
    }

    /// Returns true if the tree is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeArc;
    /// use std::sync::Arc;
    ///
    /// let tree: CongeeArc<usize, String> = CongeeArc::new();
    /// let guard = tree.pin();
    ///
    /// assert!(tree.is_empty(&guard));
    ///
    /// let value = Arc::new(String::from("value"));
    /// tree.insert(1, value, &guard).unwrap();
    /// assert!(!tree.is_empty(&guard));
    /// ```
    pub fn is_empty(&self, guard: &epoch::Guard) -> bool {
        self.inner.is_empty(guard)
    }

    /// Removes a key-value pair from the tree and returns the removed value (if present).
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeArc;
    /// use std::sync::Arc;
    ///
    /// let tree: CongeeArc<usize, String> = CongeeArc::new();
    /// let guard = tree.pin();
    ///
    /// let value = Arc::new(String::from("hello"));
    /// tree.insert(1, value, &guard).unwrap();
    ///
    /// let removed = tree.remove(1, &guard).unwrap();
    /// assert_eq!(removed.as_ref(), "hello");
    /// assert!(tree.is_empty(&guard));
    /// ```
    pub fn remove(&self, key: K, guard: &epoch::Guard) -> Option<Arc<V>> {
        let usize_key: usize = usize::from(key);
        let key: [u8; 8] = usize_key.to_be_bytes();
        let (old, new) = self.inner.compute_if_present(&key, &mut |_v| None, guard)?;
        debug_assert!(new.is_none());

        // Safety
        // The pointer was previously inserted with expose_provenance
        let rt = unsafe { arc_from_usize::<V>(old) };
        Some(rt)
    }

    /// Retrieves a value from the tree without removing it.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeArc;
    /// use std::sync::Arc;
    ///
    /// let tree: CongeeArc<usize, String> = CongeeArc::new();
    /// let guard = tree.pin();
    ///
    /// let value = Arc::new(String::from("hello"));
    /// tree.insert(1, value.clone(), &guard).unwrap();
    ///
    /// let retrieved = tree.get(1, &guard).unwrap();
    /// assert_eq!(retrieved.as_ref(), "hello");
    /// ```
    pub fn get(&self, key: K, guard: &epoch::Guard) -> Option<Arc<V>> {
        let usize_key: usize = usize::from(key);
        let key: [u8; 8] = usize_key.to_be_bytes();
        let v = self.inner.get(&key, guard)?;

        // Get
        // 1. construct the owned Arc from the pointer
        // 2. clone the Arc for return value
        // 2. leak the Arc so that we still hold the reference
        //
        // # Safety
        // The pointer was previously inserted with expose_provenance
        let owned = unsafe { arc_from_usize::<V>(v) };
        let rt = owned.clone();
        _ = Arc::into_raw(owned);
        Some(rt)
    }

    /// Inserts a key-value pair into the tree.
    ///
    /// If the key already exists, the old value is replaced and returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeArc;
    /// use std::sync::Arc;
    ///
    /// let tree: CongeeArc<usize, String> = CongeeArc::new();
    /// let guard = tree.pin();
    ///
    /// let value1 = Arc::new(String::from("hello"));
    /// assert!(tree.insert(1, value1, &guard).unwrap().is_none());
    ///
    /// let value2 = Arc::new(String::from("world"));
    /// let old = tree.insert(1, value2, &guard).unwrap().unwrap();
    /// assert_eq!(old.as_ref(), "hello");
    /// ```
    pub fn insert(
        &self,
        key: K,
        val: Arc<V>,
        guard: &epoch::Guard,
    ) -> Result<Option<Arc<V>>, OOMError> {
        let usize_key: usize = usize::from(key);
        let key: [u8; 8] = usize_key.to_be_bytes();

        // Insertion
        // 1. Get the pointer of the value, consume the Arc
        // 2. Insert the pointer into the tree
        // 3. If replaced an old value, construct an Arc from the old value and return it
        let ptr_v = Arc::into_raw(val);
        let ptr_usize = ptr_v.expose_provenance();
        let old = self.inner.insert(&key, ptr_usize, guard)?;
        if let Some(v) = old {
            // Safety
            // The pointer was previously inserted with expose_provenance
            let owned = unsafe { arc_from_usize::<V>(v) };
            Ok(Some(owned))
        } else {
            Ok(None)
        }
    }

    /// Computes a new value for a key if it exists in the tree.
    ///
    /// The function `f` is called with the current value and should return an optional new value.
    /// If `f` returns `None`, the key-value pair is removed from the tree.
    /// If `f` returns `Some(new_value)`, the key is updated with the new value.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeArc;
    /// use std::sync::Arc;
    ///
    /// let tree: CongeeArc<usize, String> = CongeeArc::new();
    /// let guard = tree.pin();
    ///
    /// let value = Arc::new(String::from("hello"));
    /// tree.insert(1, value, &guard).unwrap();
    ///
    /// // Update an existing value
    /// let old = tree.compute_if_present(
    ///     1,
    ///     |current| Some(Arc::new(format!("{} world", current))),
    ///     &guard
    /// ).unwrap();
    /// assert_eq!(old.as_ref(), "hello");
    ///
    /// let updated = tree.get(1, &guard).unwrap();
    /// assert_eq!(updated.as_ref(), "hello world");
    ///
    /// // Remove a value by returning None
    /// tree.compute_if_present(1, |_| None, &guard);
    /// assert!(tree.get(1, &guard).is_none());
    /// ```
    pub fn compute_if_present<F>(&self, key: K, mut f: F, guard: &epoch::Guard) -> Option<Arc<V>>
    where
        F: FnMut(Arc<V>) -> Option<Arc<V>>,
    {
        let usize_key: usize = usize::from(key);
        let key: [u8; 8] = usize_key.to_be_bytes();
        let mut inner_f = |v: usize| {
            // Safety
            // The pointer was previously inserted with expose_provenance
            let owned = unsafe { arc_from_usize::<V>(v) };
            let owned_clone = owned.clone();
            let rt = f(owned_clone);
            _ = Arc::into_raw(owned);
            if let Some(new) = rt {
                let new_v = Arc::into_raw(new);
                Some(new_v.expose_provenance())
            } else {
                None
            }
        };
        let (old, _new) = self.inner.compute_if_present(&key, &mut inner_f, guard)?;
        let old_owned = unsafe { arc_from_usize::<V>(old) };
        Some(old_owned)
    }

    /// Retrieves all keys from the tree.
    ///
    /// Isolation level: read committed.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::CongeeArc;
    /// use std::sync::Arc;
    ///
    /// let tree: CongeeArc<usize, String> = CongeeArc::new();
    /// let guard = tree.pin();
    ///
    /// let value1 = Arc::new(String::from("value1"));
    /// let value2 = Arc::new(String::from("value2"));
    /// tree.insert(1, value1, &guard).unwrap();
    /// tree.insert(2, value2, &guard).unwrap();
    ///
    /// let keys = tree.keys();
    /// assert!(keys.contains(&1));
    /// assert!(keys.contains(&2));
    /// assert_eq!(keys.len(), 2);
    /// ```
    pub fn keys(&self) -> Vec<K> {
        self.inner
            .keys()
            .into_iter()
            .map(|k| K::from(usize::from_be_bytes(k)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_new() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();
        assert!(tree.is_empty(&guard));
    }

    #[test]
    fn test_pin() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let _guard = tree.pin();
    }

    #[test]
    fn test_is_empty() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        assert!(tree.is_empty(&guard));

        let value = Arc::new(String::from("test"));
        tree.insert(1, value, &guard).unwrap();
        assert!(!tree.is_empty(&guard));

        tree.remove(1, &guard);
        assert!(tree.is_empty(&guard));
    }

    #[test]
    fn test_insert_basic() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        let value = Arc::new(String::from("test"));
        let result = tree.insert(1, value.clone(), &guard).unwrap();
        assert!(result.is_none());

        assert_eq!(Arc::strong_count(&value), 2);
    }

    #[test]
    fn test_insert_overwrite() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        let value1 = Arc::new(String::from("test1"));
        let value1_clone = value1.clone();
        tree.insert(1, value1, &guard).unwrap();

        let value2 = Arc::new(String::from("test2"));
        let old = tree.insert(1, value2.clone(), &guard).unwrap().unwrap();

        assert_eq!(old.as_ref(), "test1");
        assert_eq!(old.as_ref(), value1_clone.as_ref());

        let retrieved = tree.get(1, &guard).unwrap();
        assert_eq!(retrieved.as_ref(), "test2");
    }

    #[test]
    fn test_insert_multiple() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        let values: Vec<_> = (0..100)
            .map(|i| (i, Arc::new(format!("value-{}", i))))
            .collect();

        for (k, v) in &values {
            tree.insert(*k, v.clone(), &guard).unwrap();
        }

        for (k, v) in &values {
            let retrieved = tree.get(*k, &guard).unwrap();
            assert_eq!(retrieved.as_ref(), v.as_ref());
        }
    }

    #[test]
    fn test_get_nonexistent() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        assert!(tree.get(1, &guard).is_none());

        tree.insert(2, Arc::new(String::from("test")), &guard)
            .unwrap();
        assert!(tree.get(1, &guard).is_none());
    }

    #[test]
    fn test_get_basic() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        let value = Arc::new(String::from("test"));
        tree.insert(1, value.clone(), &guard).unwrap();

        let retrieved = tree.get(1, &guard).unwrap();
        assert_eq!(retrieved.as_ref(), "test");
        assert_eq!(retrieved.as_ref(), value.as_ref());

        assert_eq!(Arc::strong_count(&value), 3); // original + tree + retrieved
    }

    #[test]
    fn test_get_multiple() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        let value = Arc::new(String::from("test"));
        tree.insert(1, value.clone(), &guard).unwrap();

        let r1 = tree.get(1, &guard).unwrap();
        let r2 = tree.get(1, &guard).unwrap();
        let r3 = tree.get(1, &guard).unwrap();

        assert_eq!(r1.as_ref(), "test");
        assert_eq!(r2.as_ref(), "test");
        assert_eq!(r3.as_ref(), "test");

        assert_eq!(Arc::strong_count(&value), 5);
    }

    #[test]
    fn test_remove_nonexistent() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        assert!(tree.remove(1, &guard).is_none());

        tree.insert(2, Arc::new(String::from("test")), &guard)
            .unwrap();
        assert!(tree.remove(1, &guard).is_none());
    }

    #[test]
    fn test_remove_basic() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        let value = Arc::new(String::from("test"));
        tree.insert(1, value.clone(), &guard).unwrap();

        assert_eq!(Arc::strong_count(&value), 2);

        let removed = tree.remove(1, &guard).unwrap();
        assert_eq!(removed.as_ref(), "test");

        assert_eq!(Arc::strong_count(&value), 2); // original + removed

        assert!(tree.get(1, &guard).is_none());
    }

    #[test]
    fn test_remove_multiple() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        for i in 0..10 {
            tree.insert(i, Arc::new(format!("value-{}", i)), &guard)
                .unwrap();
        }

        for i in 0..5 {
            let removed = tree.remove(i, &guard).unwrap();
            assert_eq!(removed.as_ref(), &format!("value-{}", i));
        }

        for i in 0..10 {
            let result = tree.get(i, &guard);
            if i < 5 {
                assert!(result.is_none());
            } else {
                assert!(result.is_some());
                assert_eq!(result.unwrap().as_ref(), &format!("value-{}", i));
            }
        }
    }

    #[test]
    fn test_reference_counting() {
        let counter = Arc::new(AtomicUsize::new(0));

        struct Tracked {
            counter: Arc<AtomicUsize>,
        }

        impl Drop for Tracked {
            fn drop(&mut self) {
                self.counter.fetch_add(1, Ordering::SeqCst);
            }
        }

        let tree: CongeeArc<usize, Tracked> = CongeeArc::new();
        let guard = tree.pin();

        let tracked = Arc::new(Tracked {
            counter: counter.clone(),
        });
        tree.insert(1, tracked, &guard).unwrap();

        {
            let _retrieved = tree.get(1, &guard).unwrap();
            assert_eq!(counter.load(Ordering::SeqCst), 0);
        }
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        let tracked2 = Arc::new(Tracked {
            counter: counter.clone(),
        });
        let old = tree.insert(1, tracked2, &guard).unwrap().unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        drop(old);
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        let removed = tree.remove(1, &guard).unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        drop(removed);
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_compute_if_present_update() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        let value = Arc::new(String::from("hello"));
        tree.insert(1, value, &guard).unwrap();

        let old = tree
            .compute_if_present(
                1,
                |current| Some(Arc::new(format!("{} world", current))),
                &guard,
            )
            .unwrap();

        assert_eq!(old.as_ref(), "hello");

        let updated = tree.get(1, &guard).unwrap();
        assert_eq!(updated.as_ref(), "hello world");
    }

    #[test]
    fn test_compute_if_present_remove() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        let value = Arc::new(String::from("hello"));
        tree.insert(1, value, &guard).unwrap();

        let old = tree.compute_if_present(1, |_| None, &guard).unwrap();
        assert_eq!(old.as_ref(), "hello");

        assert!(tree.get(1, &guard).is_none());
    }

    #[test]
    fn test_compute_if_present_nonexistent() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        let result = tree.compute_if_present(1, |_| Some(Arc::new(String::from("new"))), &guard);
        assert!(result.is_none());
        assert!(tree.get(1, &guard).is_none());
    }

    #[test]
    fn test_keys_empty() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();

        let keys = tree.keys();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_keys_populated() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        for i in 0..5 {
            tree.insert(i, Arc::new(format!("value-{}", i)), &guard)
                .unwrap();
        }

        let mut keys = tree.keys();
        keys.sort();
        assert_eq!(keys, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_concurrency_simple() {
        use std::thread;

        let tree: Arc<CongeeArc<usize, String>> = Arc::new(CongeeArc::new());

        let mut handles = vec![];

        for i in 0..10 {
            let tree_clone = tree.clone();
            handles.push(thread::spawn(move || {
                let guard = tree_clone.pin();

                let value = Arc::new(format!("thread-{}", i));
                tree_clone.insert(i, value.clone(), &guard).unwrap();

                for j in 0..10 {
                    if j < i {
                        if let Some(val) = tree_clone.get(j, &guard) {
                            assert_eq!(val.as_ref(), &format!("thread-{}", j));
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let guard = tree.pin();
        for i in 0..10 {
            let value = tree.get(i, &guard).unwrap();
            assert_eq!(value.as_ref(), &format!("thread-{}", i));
        }
    }

    #[test]
    fn test_insert_get_remove_lifecycle() {
        let tree: CongeeArc<usize, String> = CongeeArc::new();
        let guard = tree.pin();

        assert!(tree.is_empty(&guard));
        let value = Arc::new(String::from("test"));
        tree.insert(1, value.clone(), &guard).unwrap();
        assert!(!tree.is_empty(&guard));
        let retrieved = tree.get(1, &guard).unwrap();
        assert_eq!(retrieved.as_ref(), "test");
        let removed = tree.remove(1, &guard).unwrap();
        assert_eq!(removed.as_ref(), "test");
        assert!(tree.is_empty(&guard));
        assert!(tree.get(1, &guard).is_none());
    }
}
