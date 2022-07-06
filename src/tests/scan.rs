use std::sync::Arc;
use std::thread;

use crate::{key::GeneralKey, tree::RawTree, RawKey};

use rand::prelude::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

#[test]
fn small_scan() {
    let tree = RawTree::new();
    let key_cnt = 1000;

    let guard = crossbeam_epoch::pin();
    for i in 0..key_cnt {
        tree.insert(GeneralKey::key_from(i), i, &guard);
    }

    let scan_cnt = 10;
    let low_v = 200;
    let low_key = GeneralKey::key_from(low_v);
    let high_key = GeneralKey::key_from(low_v + scan_cnt);

    let mut results = [(0, 0); 20];
    let scan_r = tree.range(&low_key, &high_key, &mut results, &guard);

    assert_eq!(scan_r, scan_cnt);
    for i in 0..scan_r {
        assert_eq!(results[i].1, low_v + i);
    }
}

#[test]
fn large_scan() {
    let tree = RawTree::new();
    let key_cnt = 500_000;
    let mut key_space = Vec::with_capacity(key_cnt);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }

    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let guard = crossbeam_epoch::pin();
    for v in key_space.iter() {
        tree.insert(GeneralKey::key_from(*v), *v, &guard);
    }

    let scan_counts = [3, 13, 65];

    // positive scan
    for _r in 0..10 {
        let scan_cnt = scan_counts.choose(&mut r).unwrap();
        let low_key_v = r.gen_range(0..(key_cnt - scan_cnt));

        let low_key = GeneralKey::key_from(low_key_v);
        let high_key = GeneralKey::key_from(low_key_v + scan_cnt);

        let mut scan_results = vec![(0, 0); *scan_cnt];

        let r_found = tree.range(&low_key, &high_key, &mut scan_results, &guard);
        assert_eq!(r_found, *scan_cnt);

        for (i, v) in scan_results.iter().enumerate() {
            assert_eq!(v.1, low_key_v + i);
        }
    }

    // negative scan
    for _r in 0..10 {
        let scan_cnt = scan_counts.choose(&mut r).unwrap();
        let low_key_v = r.gen_range(key_cnt..2 * key_cnt);

        let low_key = GeneralKey::key_from(low_key_v);
        let high_key = GeneralKey::key_from(low_key_v + scan_cnt);

        let mut scan_results = vec![(0, 0); *scan_cnt];
        let r_found = tree.range(&low_key, &high_key, &mut scan_results, &guard);
        assert_eq!(r_found, 0);
    }
}

#[test]
fn large_scan_small_buffer() {
    let tree = RawTree::new();
    let key_cnt = 500_000;
    let mut key_space = Vec::with_capacity(key_cnt);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }

    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let guard = crossbeam_epoch::pin();
    for v in key_space.iter() {
        tree.insert(GeneralKey::key_from(*v), *v, &guard);
    }

    let scan_counts = [3, 13, 65];

    // Scan with smaller buffer
    for _r in 0..16 {
        let scan_cnt = scan_counts.choose(&mut r).unwrap();
        let low_key_v = r.gen_range(0..(key_cnt - scan_cnt));

        let low_key = GeneralKey::key_from(low_key_v);
        let high_key = GeneralKey::key_from(low_key_v + scan_cnt * 5);

        let mut scan_results = vec![(0, 0); (*scan_cnt) / 2];

        let r_found = tree.range(&low_key, &high_key, &mut scan_results, &guard);
        assert_eq!(r_found, *scan_cnt / 2);

        for (i, v) in scan_results.iter().enumerate() {
            assert_eq!(v.1, low_key_v + i);
        }
    }
}

#[test]
fn test_insert_and_scan() {
    let insert_thread = 2;
    let scan_thread = 4;

    let key_cnt_per_thread = 500_000;
    let total_key = key_cnt_per_thread * insert_thread;
    let mut key_space = Vec::with_capacity(total_key);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }

    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let key_space = Arc::new(key_space);
    let tree = Arc::new(RawTree::new());

    let mut handlers = vec![];

    for t in 0..scan_thread {
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            let guard = crossbeam_epoch::pin();
            let mut r = StdRng::seed_from_u64(42 + t);
            let scan_counts = [3, 13, 65, 257, 513];
            let scan_cnt = scan_counts.choose(&mut r).unwrap();
            let low_key_v = r.gen_range(0..(total_key - scan_cnt));

            let low_key = GeneralKey::key_from(low_key_v);
            let high_key = GeneralKey::key_from(low_key_v + scan_cnt);

            let mut scan_results = vec![(0, 0); *scan_cnt];
            let _v = tree.range(&low_key, &high_key, &mut scan_results, &guard);
        }));
    }

    for t in 0..insert_thread {
        let key_space = key_space.clone();
        let tree = tree.clone();

        handlers.push(thread::spawn(move || {
            let guard = crossbeam_epoch::pin();
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
    let tree = RawTree::new();
    let guard = crossbeam_epoch::pin();

    tree.insert(GeneralKey::key_from(54227), 54227, &guard);

    let low_key = GeneralKey::key_from(0);
    let high_key = GeneralKey::key_from(0);

    let mut results = vec![(0, 0); 255];
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 0);
}

#[test]
fn fuzz_1() {
    let tree = RawTree::new();
    let guard = crossbeam_epoch::pin();

    let key = 4294967179;
    tree.insert(GeneralKey::key_from(key), key, &guard);

    let scan_key = 1895772415;
    let low_key = GeneralKey::key_from(scan_key);
    let high_key = GeneralKey::key_from(scan_key + 255);

    let mut results = vec![(0, 0); 256];
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 0);

    let low_key = GeneralKey::key_from(key);
    let high_key = GeneralKey::key_from(key + 255);
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 1);
}

#[test]
fn fuzz_2() {
    let tree = RawTree::new();
    let guard = crossbeam_epoch::pin();

    tree.insert(GeneralKey::key_from(4261390591), 4261390591, &guard);
    tree.insert(GeneralKey::key_from(4294944959), 4294944959, &guard);

    let scan_key = 4261412863;
    let low_key = GeneralKey::key_from(scan_key);
    let high_key = GeneralKey::key_from(scan_key + 253);

    let mut results = vec![(0, 0); 256];
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 0);
}

#[test]
fn fuzz_3() {
    let tree = RawTree::new();
    let guard = crossbeam_epoch::pin();

    tree.insert(GeneralKey::key_from(4294967295), 4294967295, &guard);
    tree.insert(GeneralKey::key_from(4294967247), 4294967247, &guard);

    let scan_key = 4294967066;
    let low_key = GeneralKey::key_from(scan_key);
    let high_key = GeneralKey::key_from(scan_key + 253);

    let mut results = vec![(0, 0); 256];
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 2);

    let scan_key = 4294967000;
    let low_key = GeneralKey::key_from(scan_key);
    let high_key = GeneralKey::key_from(scan_key + 253);

    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 1);
}

#[test]
fn fuzz_4() {
    let tree = RawTree::new();
    let guard = crossbeam_epoch::pin();

    tree.insert(GeneralKey::key_from(219021065), 219021065, &guard);
    tree.insert(GeneralKey::key_from(4279959551), 4279959551, &guard);

    let scan_key = 4294967295;
    let low_key = GeneralKey::key_from(scan_key);
    let high_key = GeneralKey::key_from(scan_key + 253);

    let mut results = vec![(0, 0); 256];
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 0);
}

#[test]
fn fuzz_5() {
    let tree = RawTree::new();
    let guard = crossbeam_epoch::pin();

    tree.insert(GeneralKey::key_from(4294967128), 429496, &guard);
    tree.insert(GeneralKey::key_from(4294940824), 40824, &guard);

    let scan_key = 4294967039;
    let low_key = GeneralKey::key_from(scan_key);
    let high_key = GeneralKey::key_from(scan_key + 255);

    let mut results = vec![(0, 0); 256];
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 1);
}

#[test]
fn fuzz_6() {
    let tree = RawTree::new();
    let guard = crossbeam_epoch::pin();

    tree.insert(GeneralKey::key_from(4278190080), 2734686207, &guard);
    tree.insert(GeneralKey::key_from(4278189917), 3638099967, &guard);

    let scan_key = 4278190079;
    let low_key = GeneralKey::key_from(scan_key);
    let high_key = GeneralKey::key_from(scan_key + 255);

    let mut results = vec![(0, 0); 256];
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 1);
}
