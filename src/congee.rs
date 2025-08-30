use std::{marker::PhantomData, ptr::with_exposed_provenance, sync::Arc};

use crate::{CongeeInner, DefaultAllocator, epoch, error::OOMError};

/// A concurrent map-like data structure that uses Arc for reference counting of values.
///
/// CongeeArc provides a way to store Arc-wrapped values in a concurrent tree structure.
/// It automatically manages reference counting when inserting, retrieving, and removing values.
pub struct Congee<K: From<usize> + Copy, V: Sync + Send + 'static>
where
    usize: From<K>,
{
    inner: Arc<CongeeInner<8, DefaultAllocator>>,
    pt_val: PhantomData<V>,
    pt_key: PhantomData<K>,
}

unsafe fn arc_from_usize<V>(v: usize) -> Arc<V> {
    // # Safety
    // The pointer was previously inserted with expose_provenance
    let ptr: *const V = with_exposed_provenance(v);
    unsafe { Arc::from_raw(ptr) }
}

impl<K: From<usize> + Copy, V: Sync + Send + 'static> Default for Congee<K, V>
where
    usize: From<K>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K: From<usize> + Copy, V: Sync + Send + 'static> Congee<K, V>
where
    usize: From<K>,
{
    /// Creates a new empty CongeeArc instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// use std::sync::Arc;
    ///
    /// let tree: Congee<usize, String> = Congee::new();
    /// ```
    pub fn new() -> Self {
        let drainer = |_k: [u8; 8], v: usize| {
            // Safety
            // The pointer was previously inserted with expose_provenance
            let owned = unsafe { arc_from_usize::<V>(v) };
            drop(owned);
        };
        Self {
            inner: Arc::new(CongeeInner::new(DefaultAllocator {}, Arc::new(drainer))),
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
    /// use congee::Congee;
    /// use std::sync::Arc;
    ///
    /// let tree: Congee<usize, String> = Congee::new();
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
    /// use congee::Congee;
    /// use std::sync::Arc;
    ///
    /// let tree: Congee<usize, String> = Congee::new();
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
    /// Note: Congee holds a reference to the removed value until the guard is flushed.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// use std::sync::Arc;
    ///
    /// let tree: Congee<usize, String> = Congee::new();
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
        let delayed_v = rt.clone();
        guard.defer(move || {
            drop(delayed_v);
        });
        Some(rt)
    }

    /// Retrieves a value from the tree without removing it.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// use std::sync::Arc;
    ///
    /// let tree: Congee<usize, String> = Congee::new();
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
    /// Note: Congee holds a reference to the returned old value until the guard is flushed.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// use std::sync::Arc;
    ///
    /// let tree: Congee<usize, String> = Congee::new();
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

            let delayed_v = owned.clone();
            guard.defer(move || {
                drop(delayed_v);
            });
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
    /// Note: Congee holds a reference to the returned old value until the guard is flushed.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// use std::sync::Arc;
    ///
    /// let tree: Congee<usize, String> = Congee::new();
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
        let delayed_v = old_owned.clone();
        guard.defer(move || {
            drop(delayed_v);
        });
        Some(old_owned)
    }

    /// Retrieves all keys from the tree.
    ///
    /// Isolation level: read committed.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// use std::sync::Arc;
    ///
    /// let tree: Congee<usize, String> = Congee::new();
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

    /// Scan the tree with the range of [start, end], write the result to the
    /// `result` buffer.
    /// It scans the length of `result` or the number of the keys within the range, whichever is smaller;
    /// returns the number of the keys scanned.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// use std::sync::Arc;
    ///
    /// let tree: Congee<usize, String> = Congee::new();
    /// let guard = tree.pin();
    ///
    /// let value1 = Arc::new(String::from("value1"));
    /// let value2 = Arc::new(String::from("value2"));
    /// tree.insert(1, value1, &guard).unwrap();
    /// tree.insert(2, value2, &guard).unwrap();
    ///
    /// let mut result = vec![(0usize, None::<Arc<String>>); 10];
    /// let scanned = tree.range(&1, &3, &mut result, &guard);
    /// assert_eq!(scanned, 2);
    /// assert_eq!(result[0].0, 1);
    /// assert_eq!(result[1].0, 2);
    /// assert!(result[0].1.is_some());
    /// assert!(result[1].1.is_some());
    /// ```
    pub fn range(
        &self,
        start: &K,
        end: &K,
        result: &mut [(K, Option<Arc<V>>)],
        guard: &epoch::Guard,
    ) -> usize {
        let start_usize = usize::from(*start);
        let end_usize = usize::from(*end);
        let start_bytes: [u8; 8] = start_usize.to_be_bytes();
        let end_bytes: [u8; 8] = end_usize.to_be_bytes();

        // Create a temporary buffer for the raw results
        let mut raw_result: Vec<([u8; 8], usize)> = vec![([0; 8], 0); result.len()];

        let scanned = self
            .inner
            .range(&start_bytes, &end_bytes, &mut raw_result, guard);

        // Convert the raw results to the expected format
        for i in 0..scanned {
            let key_bytes = raw_result[i].0;
            let val_ptr = raw_result[i].1;

            let key = K::from(usize::from_be_bytes(key_bytes));

            // Convert the usize pointer back to Arc<V>
            // Safety: The pointer was previously inserted with expose_provenance
            let owned = unsafe { arc_from_usize::<V>(val_ptr) };
            let arc_value = owned.clone();
            _ = Arc::into_raw(owned); // Leak to maintain reference in tree

            result[i] = (key, Some(arc_value));
        }

        scanned
    }

    /// Compute or insert the value if the key is not in the tree, or update if it exists.
    /// Returns the old value if the key existed, None if it was inserted.
    ///
    /// Note that the function `f` is a FnMut and it must be safe to execute multiple times.
    /// The `f` is expected to be short and fast as it will hold an exclusive lock on the leaf node.
    ///
    /// # Examples
    ///
    /// ```
    /// use congee::Congee;
    /// use std::sync::Arc;
    ///
    /// let tree: Congee<usize, String> = Congee::new();
    /// let guard = tree.pin();
    ///
    /// // Insert new value
    /// let result = tree.compute_or_insert(
    ///     1,
    ///     |existing| {
    ///         assert!(existing.is_none());
    ///         Arc::new(String::from("new_value"))
    ///     },
    ///     &guard
    /// ).unwrap();
    /// assert!(result.is_none());
    ///
    /// // Update existing value
    /// let old = tree.compute_or_insert(
    ///     1,
    ///     |existing| {
    ///         assert!(existing.is_some());
    ///         Arc::new(format!("updated_{}", existing.unwrap()))
    ///     },
    ///     &guard
    /// ).unwrap();
    /// assert!(old.is_some());
    /// assert_eq!(old.unwrap().as_ref(), "new_value");
    /// ```
    pub fn compute_or_insert<F>(
        &self,
        key: K,
        mut f: F,
        guard: &epoch::Guard,
    ) -> Result<Option<Arc<V>>, OOMError>
    where
        F: FnMut(Option<Arc<V>>) -> Arc<V>,
    {
        let usize_key = usize::from(key);
        let key_bytes: [u8; 8] = usize_key.to_be_bytes();

        let mut inner_f = |existing_ptr: Option<usize>| -> usize {
            let existing_arc = if let Some(ptr) = existing_ptr {
                // Safety: The pointer was previously inserted with expose_provenance
                let owned = unsafe { arc_from_usize::<V>(ptr) };
                let arc_clone = owned.clone();
                _ = Arc::into_raw(owned); // Leak to maintain reference in tree
                Some(arc_clone)
            } else {
                None
            };

            let new_arc = f(existing_arc);
            let new_ptr = Arc::into_raw(new_arc);
            new_ptr.expose_provenance()
        };

        let old_ptr = self
            .inner
            .compute_or_insert(&key_bytes, &mut inner_f, guard)?;

        if let Some(ptr) = old_ptr {
            // There was an old value, return it
            let old_owned = unsafe { arc_from_usize::<V>(ptr) };
            let delayed_v = old_owned.clone();
            guard.defer(move || {
                drop(delayed_v);
            });
            Ok(Some(old_owned))
        } else {
            // No old value, this was an insert
            Ok(None)
        }
    }

    /// Display the internal node statistics
    pub fn stats(&self) -> crate::stats::NodeStats {
        self.inner.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    #[cfg(all(feature = "shuttle", test))]
    use shuttle::thread;

    #[cfg(not(all(feature = "shuttle", test)))]
    use std::thread;

    #[test]
    fn test_new() {
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();
        assert!(tree.is_empty(&guard));
    }

    #[test]
    fn test_pin() {
        let tree: Congee<usize, String> = Congee::new();
        let _guard = tree.pin();
    }

    #[test]
    fn test_is_empty() {
        let tree: Congee<usize, String> = Congee::new();
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
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        let value = Arc::new(String::from("test"));
        let result = tree.insert(1, value.clone(), &guard).unwrap();
        assert!(result.is_none());

        assert_eq!(Arc::strong_count(&value), 2);
    }

    #[test]
    fn test_insert_overwrite() {
        let tree: Congee<usize, String> = Congee::new();
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
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        let values: Vec<_> = (0..100)
            .map(|i| (i, Arc::new(format!("value-{i}"))))
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
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        assert!(tree.get(1, &guard).is_none());

        tree.insert(2, Arc::new(String::from("test")), &guard)
            .unwrap();
        assert!(tree.get(1, &guard).is_none());
    }

    #[test]
    fn test_get_basic() {
        let tree: Congee<usize, String> = Congee::new();
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
        let tree: Congee<usize, String> = Congee::new();
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
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        assert!(tree.remove(1, &guard).is_none());

        tree.insert(2, Arc::new(String::from("test")), &guard)
            .unwrap();
        assert!(tree.remove(1, &guard).is_none());
    }

    #[test]
    fn test_remove_basic() {
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        let value = Arc::new(String::from("test"));
        tree.insert(1, value.clone(), &guard).unwrap();

        assert_eq!(Arc::strong_count(&value), 2);

        let removed = tree.remove(1, &guard).unwrap();
        assert_eq!(removed.as_ref(), "test");

        assert_eq!(Arc::strong_count(&value), 3); // original + removed + guard

        assert!(tree.get(1, &guard).is_none());
    }

    #[test]
    fn test_remove_multiple() {
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        for i in 0..10 {
            tree.insert(i, Arc::new(format!("value-{i}")), &guard)
                .unwrap();
        }

        for i in 0..5 {
            let removed = tree.remove(i, &guard).unwrap();
            assert_eq!(removed.as_ref(), &format!("value-{i}"));
        }

        for i in 0..10 {
            let result = tree.get(i, &guard);
            if i < 5 {
                assert!(result.is_none());
            } else {
                assert!(result.is_some());
                assert_eq!(result.unwrap().as_ref(), &format!("value-{i}"));
            }
        }
    }

    #[test]
    fn test_reference_counting() {
        let counter = Arc::new(AtomicUsize::new(0)); // 1

        let tree: Congee<usize, AtomicUsize> = Congee::new();
        let guard = tree.pin();

        tree.insert(1, counter.clone(), &guard).unwrap(); // 2

        {
            let _retrieved = tree.get(1, &guard).unwrap(); // 3
            assert_eq!(Arc::strong_count(&counter), 3);
        }

        let old = tree.insert(1, counter.clone(), &guard).unwrap().unwrap(); // 2
        assert_eq!(Arc::strong_count(&counter), 4); // one in guard.
        drop(old);
        assert_eq!(Arc::strong_count(&counter), 3);

        let removed = tree.remove(1, &guard).unwrap();
        assert_eq!(Arc::strong_count(&counter), 4);
        drop(removed);
        assert_eq!(Arc::strong_count(&counter), 3); // two in guard.
    }

    #[test]
    fn test_compute_if_present_update() {
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        let value = Arc::new(String::from("hello"));
        tree.insert(1, value, &guard).unwrap();

        let old = tree
            .compute_if_present(
                1,
                |current| Some(Arc::new(format!("{current} world"))),
                &guard,
            )
            .unwrap();

        assert_eq!(old.as_ref(), "hello");

        let updated = tree.get(1, &guard).unwrap();
        assert_eq!(updated.as_ref(), "hello world");
    }

    #[test]
    fn test_compute_if_present_remove() {
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        let value = Arc::new(String::from("hello"));
        tree.insert(1, value, &guard).unwrap();

        let old = tree.compute_if_present(1, |_| None, &guard).unwrap();
        assert_eq!(old.as_ref(), "hello");

        assert!(tree.get(1, &guard).is_none());
    }

    #[test]
    fn test_compute_if_present_nonexistent() {
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        let result = tree.compute_if_present(1, |_| Some(Arc::new(String::from("new"))), &guard);
        assert!(result.is_none());
        assert!(tree.get(1, &guard).is_none());
    }

    #[test]
    fn test_keys_empty() {
        let tree: Congee<usize, String> = Congee::new();

        let keys = tree.keys();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_keys_populated() {
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        for i in 0..5 {
            tree.insert(i, Arc::new(format!("value-{i}")), &guard)
                .unwrap();
        }

        let mut keys = tree.keys();
        keys.sort();
        assert_eq!(keys, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_concurrency_simple() {
        use std::thread;

        let tree: Arc<Congee<usize, String>> = Arc::new(Congee::new());

        let mut handles = vec![];

        for i in 0..10 {
            let tree_clone = tree.clone();
            handles.push(thread::spawn(move || {
                let guard = tree_clone.pin();

                let value = Arc::new(format!("thread-{i}"));
                tree_clone.insert(i, value.clone(), &guard).unwrap();

                for j in 0..10 {
                    if j < i
                        && let Some(val) = tree_clone.get(j, &guard)
                    {
                        assert_eq!(val.as_ref(), &format!("thread-{j}"));
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
            assert_eq!(value.as_ref(), &format!("thread-{i}"));
        }
    }

    #[test]
    fn test_insert_get_remove_lifecycle() {
        let tree: Congee<usize, String> = Congee::new();
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

    #[test]
    fn test_concurrent_get_insert_race_condition() {
        let tree: Arc<Congee<usize, String>> = Arc::new(Congee::new());

        // Insert initial value
        {
            let guard = tree.pin();
            tree.insert(42, Arc::new(String::from("initial")), &guard)
                .unwrap();
        }

        let tree1 = tree.clone();
        let tree2 = tree.clone();

        // Thread 1: Repeatedly reads the same key
        let reader_handle = thread::spawn(move || {
            for _i in 0..10 {
                let guard = tree1.pin();
                let _value = tree1.get(42, &guard);
            }
        });

        // Thread 2: Repeatedly replaces the value for the same key
        let writer_handle = thread::spawn(move || {
            for i in 0..10 {
                let guard = tree2.pin();
                let new_value = Arc::new(format!("value-{i}"));
                let _ = tree2.insert(42, new_value, &guard);
            }
        });

        reader_handle.join().unwrap();
        writer_handle.join().unwrap();

        // Verify the tree is still functional
        let guard = tree.pin();
        assert!(tree.get(42, &guard).is_some());
    }

    #[cfg(all(feature = "shuttle", test))]
    #[test]
    fn shuttle_get_insert_race() {
        tracing_subscriber::fmt()
            .with_ansi(true)
            .with_thread_names(false)
            .without_time()
            .with_target(false)
            .init();
        let config = shuttle::Config::default();
        let mut runner = shuttle::PortfolioRunner::new(true, config);
        runner.add(shuttle::scheduler::PctScheduler::new(3, 2_000));
        runner.add(shuttle::scheduler::PctScheduler::new(15, 2_000));
        runner.add(shuttle::scheduler::PctScheduler::new(40, 2_000));

        runner.run(test_concurrent_get_insert_race_condition);
    }

    #[test]
    fn test_range_basic() {
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        // Insert some values
        for i in 0..10 {
            let value = Arc::new(format!("value-{i}"));
            tree.insert(i, value, &guard).unwrap();
        }

        // Test range scan
        let mut result: Vec<(usize, Option<Arc<String>>)> = vec![(0, None); 5];
        let scanned = tree.range(&2, &7, &mut result, &guard);

        assert_eq!(scanned, 5); // Should scan keys 2, 3, 4, 5, 6
        for (i, r) in result.iter().enumerate().take(scanned) {
            assert_eq!(r.0, i + 2);
            assert!(r.1.is_some());
            assert_eq!(r.1.as_ref().unwrap().as_ref(), &format!("value-{}", i + 2));
        }
    }

    #[test]
    fn test_range_empty() {
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        let mut result: Vec<(usize, Option<Arc<String>>)> = vec![(0, None); 5];
        let scanned = tree.range(&1, &5, &mut result, &guard);

        assert_eq!(scanned, 0);
    }

    #[test]
    fn test_range_partial() {
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        // Insert only some values
        tree.insert(1, Arc::new(String::from("one")), &guard)
            .unwrap();
        tree.insert(5, Arc::new(String::from("five")), &guard)
            .unwrap();
        tree.insert(8, Arc::new(String::from("eight")), &guard)
            .unwrap();

        let mut result: Vec<(usize, Option<Arc<String>>)> = vec![(0, None); 10];
        let scanned = tree.range(&0, &10, &mut result, &guard);

        assert_eq!(scanned, 3);
        assert_eq!(result[0].0, 1);
        assert_eq!(result[0].1.as_ref().unwrap().as_ref(), "one");
        assert_eq!(result[1].0, 5);
        assert_eq!(result[1].1.as_ref().unwrap().as_ref(), "five");
        assert_eq!(result[2].0, 8);
        assert_eq!(result[2].1.as_ref().unwrap().as_ref(), "eight");
    }

    #[test]
    fn test_compute_or_insert_new() {
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        let result = tree
            .compute_or_insert(
                1,
                |existing| {
                    assert!(existing.is_none());
                    Arc::new(String::from("new_value"))
                },
                &guard,
            )
            .unwrap();

        assert!(result.is_none()); // No old value

        let stored = tree.get(1, &guard).unwrap();
        assert_eq!(stored.as_ref(), "new_value");
    }

    #[test]
    fn test_compute_or_insert_update() {
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        let initial_value = Arc::new(String::from("initial"));
        tree.insert(1, initial_value.clone(), &guard).unwrap();

        let old = tree
            .compute_or_insert(
                1,
                |existing| {
                    assert!(existing.is_some());
                    let old_val = existing.unwrap();
                    Arc::new(format!("updated_{old_val}"))
                },
                &guard,
            )
            .unwrap();

        assert!(old.is_some());
        assert_eq!(old.unwrap().as_ref(), "initial");

        let stored = tree.get(1, &guard).unwrap();
        assert_eq!(stored.as_ref(), "updated_initial");
    }

    #[test]
    fn test_compute_or_insert_multiple() {
        let tree: Congee<usize, String> = Congee::new();
        let guard = tree.pin();

        // Insert multiple values using compute_or_insert
        for i in 0..5 {
            let result = tree
                .compute_or_insert(
                    i,
                    |existing| {
                        assert!(existing.is_none());
                        Arc::new(format!("value-{i}"))
                    },
                    &guard,
                )
                .unwrap();
            assert!(result.is_none());
        }

        // Verify all values are there
        for i in 0..5 {
            let value = tree.get(i, &guard).unwrap();
            assert_eq!(value.as_ref(), &format!("value-{i}"));
        }

        // Update all values
        for i in 0..5 {
            let old = tree
                .compute_or_insert(
                    i,
                    |existing| {
                        let old_val = existing.unwrap();
                        Arc::new(format!("updated_{old_val}"))
                    },
                    &guard,
                )
                .unwrap();
            assert!(old.is_some());
            assert_eq!(old.unwrap().as_ref(), &format!("value-{i}"));
        }

        // Verify all values are updated
        for i in 0..5 {
            let value = tree.get(i, &guard).unwrap();
            assert_eq!(value.as_ref(), &format!("updated_value-{i}"));
        }
    }
}
