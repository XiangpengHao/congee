use std::sync::Arc;
use std::thread;

use crate::congee_inner::CongeeInner;

use rand::prelude::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

#[test]
fn small_scan() {
    let tree = CongeeInner::default();
    let key_cnt: usize = 1000;

    let guard = crossbeam_epoch::pin();
    for i in 0..key_cnt {
        let key: [u8; 8] = i.to_be_bytes();
        tree.insert(&key, i, &guard).unwrap();
    }

    let scan_cnt = 10usize;
    let low_v = 200usize;
    let low_key: [u8; 8] = low_v.to_be_bytes();
    let high_key: [u8; 8] = (low_v + scan_cnt).to_be_bytes();

    let mut results = [([0; 8], 0); 20];
    let scan_r = tree.range(&low_key, &high_key, &mut results, &guard);

    assert_eq!(scan_r, scan_cnt);
    for i in 0..scan_r {
        assert_eq!(results[i].1, low_v + i);
    }
}

#[test]
fn large_scan() {
    let tree = CongeeInner::default();
    let key_cnt = 500_000;
    let mut key_space = Vec::with_capacity(key_cnt);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }

    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let guard = crossbeam_epoch::pin();
    for v in key_space.iter() {
        let key: [u8; 8] = v.to_be_bytes();
        tree.insert(&key, *v, &guard).unwrap();
    }

    let scan_counts = [3, 13, 65];

    // positive scan
    for _r in 0..10 {
        let scan_cnt = scan_counts.choose(&mut r).unwrap();
        let low_key_v = r.gen_range(0..(key_cnt - scan_cnt));

        let low_key: [u8; 8] = low_key_v.to_be_bytes();
        let high_key: [u8; 8] = (low_key_v + scan_cnt).to_be_bytes();

        let mut scan_results = vec![([0; 8], 0); *scan_cnt];

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

        let low_key: [u8; 8] = low_key_v.to_be_bytes();
        let high_key: [u8; 8] = (low_key_v + scan_cnt).to_be_bytes();

        let mut scan_results = vec![([0; 8], 0); *scan_cnt];
        let r_found = tree.range(&low_key, &high_key, &mut scan_results, &guard);
        assert_eq!(r_found, 0);
    }
}

#[test]
fn large_scan_small_buffer() {
    let tree = CongeeInner::default();
    let key_cnt = 500_000;
    let mut key_space = Vec::with_capacity(key_cnt);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }

    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let guard = crossbeam_epoch::pin();
    for v in key_space.iter() {
        let key: [u8; 8] = v.to_be_bytes();
        tree.insert(&key, *v, &guard).unwrap();
    }

    let scan_counts = [3, 13, 65];

    // Scan with smaller buffer
    for _r in 0..16 {
        let scan_cnt = scan_counts.choose(&mut r).unwrap();
        let low_key_v = r.gen_range(0..(key_cnt - scan_cnt));

        let low_key: [u8; 8] = low_key_v.to_be_bytes();
        let high_key: [u8; 8] = (low_key_v + scan_cnt * 5).to_be_bytes();

        let mut scan_results = vec![([0; 8], 0); (*scan_cnt) / 2];

        let r_found = tree.range(&low_key, &high_key, &mut scan_results, &guard);
        assert_eq!(r_found, *scan_cnt / 2);

        for (i, v) in scan_results.iter().enumerate() {
            assert_eq!(v.1, low_key_v + i);
        }
    }

    for _r in 0..16 {
        let scan_cnt = scan_counts.choose(&mut r).unwrap();
        let low_key: [u8; 8] = 0x6_0000usize.to_be_bytes();
        let high_key: [u8; 8] = (0x6_ffff as usize).to_be_bytes();
        let mut scan_results = vec![([0; 8], 0); *scan_cnt];

        let r_found = tree.range(&low_key, &high_key, &mut scan_results, &guard);
        assert_eq!(r_found, *scan_cnt);

        for (i, v) in scan_results.iter().enumerate() {
            assert_eq!(v.1, 0x6_0000 + i);
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
    let tree = Arc::new(CongeeInner::default());

    let mut handlers = vec![];

    for t in 0..scan_thread {
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            let guard = crossbeam_epoch::pin();
            let mut r = StdRng::seed_from_u64(42 + t);
            let scan_counts = [3, 13, 65, 257, 513];
            let scan_cnt = scan_counts.choose(&mut r).unwrap();
            let low_key_v = r.gen_range(0..(total_key - scan_cnt));

            let low_key: [u8; 8] = low_key_v.to_be_bytes();
            let high_key: [u8; 8] = (low_key_v + scan_cnt).to_be_bytes();

            let mut scan_results = vec![([0; 8], 0); *scan_cnt];
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
                let key: [u8; 8] = val.to_be_bytes();
                tree.insert(&key, val, &guard).unwrap();
            }
        }));
    }

    for h in handlers.into_iter() {
        h.join().unwrap();
    }

    let guard = crossbeam_epoch::pin();
    for v in key_space.iter() {
        let key: [u8; 8] = v.to_be_bytes();
        let val = tree.get(&key, &guard).unwrap();
        assert_eq!(val, *v);
    }
}

#[test]
fn fuzz_0() {
    let tree = CongeeInner::default();
    let guard = crossbeam_epoch::pin();

    let key: [u8; 8] = 54227usize.to_be_bytes();
    tree.insert(&key, 54227, &guard).unwrap();

    let low_key: [u8; 8] = 0usize.to_be_bytes();
    let high_key: [u8; 8] = 0usize.to_be_bytes();

    let mut results = vec![([0; 8], 0); 255];
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 0);
}

#[test]
fn fuzz_1() {
    let tree = CongeeInner::default();
    let guard = crossbeam_epoch::pin();

    let value = 4294967179usize;
    let key: [u8; 8] = value.to_be_bytes();
    tree.insert(&key, value, &guard).unwrap();

    let scan_key = 1895772415usize;
    let low_key: [u8; 8] = scan_key.to_be_bytes();
    let high_key: [u8; 8] = (scan_key + 255).to_be_bytes();

    let mut results = vec![([0; 8], 0); 256];
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 0);

    let low_key: [u8; 8] = value.to_be_bytes();
    let high_key: [u8; 8] = (value + 255).to_be_bytes();
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 1);
}

#[test]
fn fuzz_2() {
    let tree = CongeeInner::default();
    let guard = crossbeam_epoch::pin();

    let value = 4261390591usize;
    let key: [u8; 8] = value.to_be_bytes();
    tree.insert(&key, value, &guard).unwrap();

    let value = 4294944959usize;
    let key: [u8; 8] = value.to_be_bytes();
    tree.insert(&key, value, &guard).unwrap();

    let scan_key = 4261412863usize;
    let low_key: [u8; 8] = scan_key.to_be_bytes();
    let high_key: [u8; 8] = (scan_key + 253).to_be_bytes();

    let mut results = vec![([0; 8], 0); 256];
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 0);
}

#[test]
fn fuzz_3() {
    let tree = CongeeInner::default();
    let guard = crossbeam_epoch::pin();

    let value = 4294967295usize;
    let key: [u8; 8] = value.to_be_bytes();
    tree.insert(&key, value, &guard).unwrap();

    let value = 4294967247usize;
    let key: [u8; 8] = value.to_be_bytes();
    tree.insert(&key, value, &guard).unwrap();

    let scan_key = 4294967066usize;
    let low_key: [u8; 8] = scan_key.to_be_bytes();
    let high_key: [u8; 8] = (scan_key + 253).to_be_bytes();

    let mut results = vec![([0; 8], 0); 256];
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 2);

    let scan_key = 4294967000usize;
    let low_key: [u8; 8] = scan_key.to_be_bytes();
    let high_key: [u8; 8] = (scan_key + 253).to_be_bytes();

    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 1);
}

#[test]
fn fuzz_4() {
    let tree = CongeeInner::default();
    let guard = crossbeam_epoch::pin();

    let value = 219021065usize;
    let key: [u8; 8] = value.to_be_bytes();
    tree.insert(&key, value, &guard).unwrap();

    let value = 4279959551usize;
    let key: [u8; 8] = value.to_be_bytes();
    tree.insert(&key, value, &guard).unwrap();

    let scan_key = 4294967295usize;
    let low_key: [u8; 8] = scan_key.to_be_bytes();
    let high_key: [u8; 8] = (scan_key + 253).to_be_bytes();

    let mut results = vec![([0; 8], 0); 256];
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 0);
}

#[test]
fn fuzz_5() {
    let tree = CongeeInner::default();
    let guard = crossbeam_epoch::pin();

    let value = 4294967128usize;
    let key: [u8; 8] = value.to_be_bytes();
    tree.insert(&key, value, &guard).unwrap();

    let value = 4294940824usize;
    let key: [u8; 8] = value.to_be_bytes();
    tree.insert(&key, value, &guard).unwrap();

    let scan_key = 4294967039usize;
    let low_key: [u8; 8] = scan_key.to_be_bytes();
    let high_key: [u8; 8] = (scan_key + 255).to_be_bytes();

    let mut results = vec![([0; 8], 0); 256];
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 1);
}

#[test]
fn fuzz_6() {
    let tree = CongeeInner::default();
    let guard = crossbeam_epoch::pin();

    let value = 4278190080usize;
    let key: [u8; 8] = value.to_be_bytes();
    tree.insert(&key, value, &guard).unwrap();

    let value = 4278189917usize;
    let key: [u8; 8] = value.to_be_bytes();
    tree.insert(&key, value, &guard).unwrap();

    let scan_key = 4278190079usize;
    let low_key: [u8; 8] = scan_key.to_be_bytes();
    let high_key: [u8; 8] = (scan_key + 255).to_be_bytes();

    let mut results = vec![([0; 8], 0); 256];
    let scanned = tree.range(&low_key, &high_key, &mut results, &guard);
    assert_eq!(scanned, 1);
}
