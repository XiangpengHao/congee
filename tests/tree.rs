use std::sync::Arc;
use std::thread;

use con_art_rust::{tree::Tree, GeneralKey};
use con_art_rust::{Key, UsizeKey};

#[test]
fn test_simple() {
    let tree = Tree::new();
    let key_cnt = 1000;

    let guard = tree.pin();
    for i in 0..key_cnt {
        tree.insert(GeneralKey::key_from(i), i, &guard);
    }

    for i in 0..key_cnt {
        let v = tree.get(&GeneralKey::key_from(i), &guard).unwrap();
        assert_eq!(v, i);
    }
}

#[test]
fn test_remove() {
    let key_cnt = 100_000;
    let tree = Tree::new();

    let guard = tree.pin();
    for i in 0..key_cnt {
        tree.insert(GeneralKey::key_from(i), i, &guard);
    }

    let delete_cnt = key_cnt / 2;

    for i in 0..delete_cnt {
        tree.remove(&GeneralKey::key_from(i), &guard);
    }

    for i in 0..delete_cnt {
        let v = tree.get(&GeneralKey::key_from(i), &guard);
        assert!(v.is_none());
    }

    for i in delete_cnt..key_cnt {
        let v = tree.get(&GeneralKey::key_from(i), &guard).unwrap();
        assert_eq!(v, i);
    }
}

#[test]
fn test_insert_read_back() {
    let key_cnt = 1000000;
    let tree = Tree::new();

    let guard = tree.pin();
    for i in 0..key_cnt {
        tree.insert(GeneralKey::key_from(i), i, &guard);
    }

    for i in 0..key_cnt {
        let v = tree.get(&GeneralKey::key_from(i), &guard).unwrap();
        assert_eq!(v, i);
    }

    for i in key_cnt..2 * key_cnt {
        let v = tree.get(&GeneralKey::key_from(i), &guard);
        assert!(v.is_none());
    }
}

#[test]
fn test_rng_insert_read_back() {
    let key_cnt = 30000;
    let mut key_space = Vec::with_capacity(key_cnt);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }

    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let tree = Tree::new();

    let guard = tree.pin();
    for v in key_space.iter() {
        tree.insert(UsizeKey::key_from(*v), *v, &guard);
    }

    let guard = crossbeam_epoch::pin();
    for i in 0..key_cnt {
        let v = tree.get(&UsizeKey::key_from(i), &guard).unwrap();
        assert_eq!(v, i);
    }

    for i in key_cnt..2 * key_cnt {
        let v = tree.get(&UsizeKey::key_from(i), &guard);
        assert!(v.is_none());
    }
}

use rand::prelude::StdRng;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng, SeedableRng};

#[test]
fn test_concurrent_insert() {
    let key_cnt_per_thread = 100000;
    let n_thread = 6;
    let mut key_space = Vec::with_capacity(key_cnt_per_thread * n_thread);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }
    key_space.shuffle(&mut thread_rng());

    let key_space = Arc::new(key_space);

    let tree = Arc::new(Tree::new());

    let mut handlers = Vec::new();
    for t in 0..n_thread {
        let key_space = key_space.clone();
        let tree = tree.clone();

        handlers.push(thread::spawn(move || {
            let guard = tree.pin();
            for i in 0..key_cnt_per_thread {
                let idx = t * key_cnt_per_thread + i;
                let val = key_space[idx];
                tree.insert(GeneralKey::key_from(val), val, &guard);
            }
        }));
    }

    for h in handlers.into_iter() {
        h.join().unwrap();
    }

    let guard = crossbeam_epoch::pin();
    for v in key_space.iter() {
        let val = tree.get(&GeneralKey::key_from(*v), &guard).unwrap();
        assert_eq!(val, *v);
    }
}

#[test]
fn test_concurrent_insert_read() {
    let key_cnt_per_thread = 200000;
    let w_thread = 3;
    let mut key_space = Vec::with_capacity(key_cnt_per_thread * w_thread);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }

    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let key_space = Arc::new(key_space);

    let tree = Arc::new(Tree::new());

    let mut handlers = Vec::new();
    for t in 0..w_thread {
        let key_space = key_space.clone();
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            let guard = tree.pin();
            for i in 0..key_cnt_per_thread {
                let idx = t * key_cnt_per_thread + i;
                let val = key_space[idx];
                tree.insert(GeneralKey::key_from(val), val, &guard);
            }
        }));
    }

    let r_thread = 3;
    for t in 0..r_thread {
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            let mut r = StdRng::seed_from_u64(10 + t);
            let guard = crossbeam_epoch::pin();
            for _i in 0..key_cnt_per_thread {
                let val = r.gen_range(0..(key_cnt_per_thread * w_thread));
                if let Some(v) = tree.get(&GeneralKey::key_from(val), &guard) {
                    assert_eq!(v, val);
                }
            }
        }));
    }

    for h in handlers.into_iter() {
        h.join().unwrap();
    }

    let guard = crossbeam_epoch::pin();
    for v in key_space.iter() {
        let val = tree.get(&GeneralKey::key_from(*v), &guard).unwrap();
        assert_eq!(val, *v);
    }
}

#[test]
fn fuzz_0() {
    let key = 4294967295;

    let tree = Tree::new();

    let guard = tree.pin();
    tree.insert(UsizeKey::key_from(key), key, &guard);
    tree.insert(UsizeKey::key_from(key), key, &guard);
    let rv = tree.get(&UsizeKey::key_from(key), &guard).unwrap();
    assert_eq!(rv, key);
}

#[test]
fn fuzz_1() {
    let keys = [4294967295, 4294967039, 30];

    let tree = Tree::new();
    let guard = tree.pin();

    for k in keys {
        tree.insert(GeneralKey::key_from(k), k, &guard);
    }

    let rv = tree
        .get(&GeneralKey::key_from(4294967295 as usize), &guard)
        .unwrap();
    assert_eq!(rv, 4294967295 as usize);
}
