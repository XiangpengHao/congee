use con_art_rust::{tree::Tree, Key};

use rand::prelude::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

#[test]
fn small_scan() {
    let tree = Tree::new();
    let key_cnt = 1000;

    for i in 0..key_cnt {
        tree.insert(Key::from(i), i);
    }

    let scan_cnt = 10;
    let low_v = 200;
    let low_key = Key::from(low_v);
    let high_key = Key::from(low_v + scan_cnt);

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

    for v in key_space.iter() {
        tree.insert(Key::from(*v), *v);
    }

    let scan_counts = [3, 13, 65, 257, 513];

    for _r in 0..10 {
        let scan_cnt = scan_counts.choose(&mut r).unwrap();
        let low_key_v = r.gen_range(0..(key_cnt - scan_cnt));

        let low_key = Key::from(low_key_v);
        let high_key = Key::from(low_key_v + scan_cnt);

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
}
