use std::sync::Arc;
use std::thread;

use con_art_rust::Key;
use con_art_rust::{tree::Tree, GeneralKey};

use rand::prelude::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

#[test]
fn small_scan() {
    let tree = Tree::new();
    let key_cnt = 1000;

    let guard = tree.pin();
    for i in 0..key_cnt {
        tree.insert(GeneralKey::key_from(i), i, &guard);
    }

    let scan_cnt = 10;
    let low_v = 200;
    let low_key = GeneralKey::key_from(low_v);
    let high_key = GeneralKey::key_from(low_v + scan_cnt);

    let mut results = [0; 20];
    let scan_r = tree
        .look_up_range(&low_key, &high_key, &mut results)
        .unwrap();

    assert_eq!(scan_r, scan_cnt);
    for i in 0..scan_r {
        assert_eq!(results[i], low_v + i);
    }
}

#[test]
fn large_scan() {
    let tree = Tree::new();
    let key_cnt = 1000000;
    let mut key_space = Vec::with_capacity(key_cnt);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }

    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let guard = tree.pin();
    for v in key_space.iter() {
        tree.insert(GeneralKey::key_from(*v), *v, &guard);
    }

    let scan_counts = [3, 13, 65, 257, 513];

    // positive scan
    for _r in 0..10 {
        let scan_cnt = scan_counts.choose(&mut r).unwrap();
        let low_key_v = r.gen_range(0..(key_cnt - scan_cnt));

        let low_key = GeneralKey::key_from(low_key_v);
        let high_key = GeneralKey::key_from(low_key_v + scan_cnt);

        let mut scan_results = Vec::with_capacity(*scan_cnt);
        for _i in 0..*scan_cnt {
            scan_results.push(0);
        }
        let r_found = tree
            .look_up_range(&low_key, &high_key, &mut scan_results)
            .unwrap();
        assert_eq!(r_found, *scan_cnt);

        for (i, v) in scan_results.iter().enumerate() {
            assert_eq!(*v, low_key_v + i);
        }
    }

    // negative scan
    for _r in 0..10 {
        let scan_cnt = scan_counts.choose(&mut r).unwrap();
        let low_key_v = r.gen_range(key_cnt..2 * key_cnt);

        let low_key = GeneralKey::key_from(low_key_v);
        let high_key = GeneralKey::key_from(low_key_v + scan_cnt);

        let mut scan_results = Vec::with_capacity(*scan_cnt);
        for _i in 0..*scan_cnt {
            scan_results.push(0);
        }
        let r_found = tree.look_up_range(&low_key, &high_key, &mut scan_results);
        assert!(r_found.is_none());
    }
}

#[test]
fn test_insert_and_scan() {
    let insert_thread = 2;
    let scan_thread = 4;

    let key_cnt_per_thread = 500000;
    let total_key = key_cnt_per_thread * insert_thread;
    let mut key_space = Vec::with_capacity(total_key);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }

    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let key_space = Arc::new(key_space);
    let tree = Arc::new(Tree::new());

    let mut handlers = vec![];

    for t in 0..scan_thread {
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            let mut r = StdRng::seed_from_u64(42 + t);
            let scan_counts = [3, 13, 65, 257, 513];
            let scan_cnt = scan_counts.choose(&mut r).unwrap();
            let low_key_v = r.gen_range(0..(total_key - scan_cnt));

            let low_key = GeneralKey::key_from(low_key_v);
            let high_key = GeneralKey::key_from(low_key_v + scan_cnt);

            let mut scan_results = Vec::with_capacity(*scan_cnt);
            for _i in 0..*scan_cnt {
                scan_results.push(0);
            }

            let _v = tree.look_up_range(&low_key, &high_key, &mut scan_results);
        }));
    }

    for t in 0..insert_thread {
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
fn fuzz_0() {
    let tree = Tree::new();
    let guard = tree.pin();

    tree.insert(GeneralKey::key_from(54227), 54227, &guard);

    let low_key = GeneralKey::key_from(0);
    let high_key = GeneralKey::key_from(0);

    let mut results = vec![0; 255];
    let scanned = tree.look_up_range(&low_key, &high_key, &mut results);
    assert_eq!(scanned.unwrap_or(0), 0);
}

#[test]
fn fuzz_1() {
    let tree = Tree::new();
    let guard = tree.pin();

    let key = 4294967179;
    tree.insert(GeneralKey::key_from(key), key, &guard);

    let scan_key = 1895772415;
    let low_key = GeneralKey::key_from(scan_key);
    let high_key = GeneralKey::key_from(scan_key + 255);

    let mut results = vec![0; 256];
    let scanned = tree.look_up_range(&low_key, &high_key, &mut results);
    assert_eq!(scanned.unwrap_or(0), 0);

    let low_key = GeneralKey::key_from(key);
    let high_key = GeneralKey::key_from(key + 255);
    let scanned = tree.look_up_range(&low_key, &high_key, &mut results);
    assert_eq!(scanned.unwrap_or(0), 1);
}

#[test]
fn fuzz_2() {
    let tree = Tree::new();
    let guard = tree.pin();

    tree.insert(GeneralKey::key_from(4261390591), 4261390591, &guard);
    tree.insert(GeneralKey::key_from(4294944959), 4294944959, &guard);

    let scan_key = 4261412863;
    let low_key = GeneralKey::key_from(scan_key);
    let high_key = GeneralKey::key_from(scan_key + 253);

    let mut results = vec![0; 256];
    let scanned = tree.look_up_range(&low_key, &high_key, &mut results);
    assert_eq!(scanned.unwrap_or(0), 0);
}

#[test]
fn fuzz_3() {
    let tree = Tree::new();
    let guard = tree.pin();

    tree.insert(GeneralKey::key_from(4294967295), 4294967295, &guard);
    tree.insert(GeneralKey::key_from(4294967247), 4294967247, &guard);

    let scan_key = 4294967066;
    let low_key = GeneralKey::key_from(scan_key);
    let high_key = GeneralKey::key_from(scan_key + 253);

    let mut results = vec![0; 256];
    let scanned = tree.look_up_range(&low_key, &high_key, &mut results);
    assert_eq!(scanned.unwrap_or(0), 2);

    let scan_key = 4294967000;
    let low_key = GeneralKey::key_from(scan_key);
    let high_key = GeneralKey::key_from(scan_key + 253);

    let scanned = tree.look_up_range(&low_key, &high_key, &mut results);
    assert_eq!(scanned.unwrap_or(0), 1);
}

#[test]
fn fuzz_4() {
    let tree = Tree::new();
    let guard = tree.pin();

    tree.insert(GeneralKey::key_from(219021065), 219021065, &guard);
    tree.insert(GeneralKey::key_from(4279959551), 4279959551, &guard);

    let scan_key = 4294967295;
    let low_key = GeneralKey::key_from(scan_key);
    let high_key = GeneralKey::key_from(scan_key + 253);

    let mut results = vec![0; 256];
    let scanned = tree.look_up_range(&low_key, &high_key, &mut results);
    assert_eq!(scanned.unwrap_or(0), 0);
}

#[test]
fn fuzz_5() {
    let tree = Tree::new();
    let guard = tree.pin();

    tree.insert(GeneralKey::key_from(4294967128), 429496, &guard);
    tree.insert(GeneralKey::key_from(4294940824), 40824, &guard);

    let scan_key = 4294967039;
    let low_key = GeneralKey::key_from(scan_key);
    let high_key = GeneralKey::key_from(scan_key + 255);

    let mut results = vec![0; 256];
    let scanned = tree.look_up_range(&low_key, &high_key, &mut results);
    assert_eq!(scanned.unwrap_or(0), 1);
}

#[test]
fn fuzz_6() {
    let tree = Tree::new();
    let guard = tree.pin();

    tree.insert(GeneralKey::key_from(4278190080), 2734686207, &guard);
    tree.insert(GeneralKey::key_from(4278189917), 3638099967, &guard);

    let scan_key = 4278190079;
    let low_key = GeneralKey::key_from(scan_key);
    let high_key = GeneralKey::key_from(scan_key + 255);

    let mut results = vec![0; 256];
    let scanned = tree.look_up_range(&low_key, &high_key, &mut results);
    assert_eq!(scanned.unwrap_or(0), 1);
}
