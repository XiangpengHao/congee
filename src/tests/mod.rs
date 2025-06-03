use std::sync::Arc;

use crate::DefaultAllocator;

mod scan;
mod tree;

mod alloc;

#[test]
fn drop_with_drainer() {
    let deleted_key = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let deleted_value = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let deleted_key_inner = deleted_key.clone();
    let deleted_value_inner = deleted_value.clone();
    let drain_function = move |k: usize, v: usize| {
        deleted_key_inner.store(k, std::sync::atomic::Ordering::Relaxed);
        deleted_value_inner.store(v, std::sync::atomic::Ordering::Relaxed);
    };

    let tree = crate::CongeeRaw::<usize, usize>::new_with_drainer(DefaultAllocator {}, drain_function);
    let pin = tree.pin();
    tree.insert(1, 42, &pin).unwrap();
    drop(tree);
    assert_eq!(deleted_key.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(deleted_value.load(std::sync::atomic::Ordering::Relaxed), 42);
}
