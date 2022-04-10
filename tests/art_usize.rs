use std::{collections::BTreeMap, vec};

use congee::{Art, ArtUsize};
use rand::{
    prelude::{SliceRandom, StdRng},
    SeedableRng,
};

enum TreeOp {
    Get { key: usize },
    Insert { key: usize, val: usize },
    Range { low_v: usize, cnt: u8 },
    Delete { key: usize },
}

fn test_runner(ops: &[TreeOp]) {
    let art_usize = ArtUsize::new();
    let art = Art::new();
    let mut bt_map = BTreeMap::new();

    let mut au_scan_buffer = vec![(0, 0); 512];
    let mut art_scan_buffer = vec![(0, 0); 512];

    for m_c in ops.chunks(1024) {
        let guard = art_usize.pin();
        for m in m_c {
            match m {
                TreeOp::Get { key } => {
                    let art_u = art_usize.get(key, &guard);
                    let bt = bt_map.get(key).cloned();
                    let art = art.get(key, &guard);
                    assert_eq!(art_u, bt);
                    assert_eq!(art, bt);
                }
                TreeOp::Insert { key, val } => {
                    let a_insert = art.insert(*key, *val, &guard);
                    let au_insert = art_usize.insert(*key, *val, &guard);
                    let btree_insert = bt_map.insert(*key, *val);
                    assert_eq!(a_insert, btree_insert);
                    assert_eq!(au_insert, btree_insert);
                }
                TreeOp::Delete { key } => {
                    let bt_remove = bt_map.remove(key);
                    let au_remove = art_usize.remove(key, &guard);
                    let art_remove = art.remove(key, &guard);
                    assert_eq!(bt_remove, au_remove);
                    assert_eq!(bt_remove, art_remove);
                }
                TreeOp::Range { low_v, cnt } => {
                    let cnt = *cnt as usize;

                    // prevent integer overflow
                    let high_key = if (usize::MAX - low_v) <= cnt {
                        usize::MAX
                    } else {
                        low_v + cnt
                    };

                    let au_range = art_usize.range(low_v, &high_key, &mut au_scan_buffer, &guard);
                    let art_range = art.range(low_v, &high_key, &mut art_scan_buffer, &guard);
                    let bt_range: Vec<(&usize, &usize)> = bt_map.range(*low_v..high_key).collect();

                    assert_eq!(bt_range.len(), art_range);
                    assert_eq!(bt_range.len(), au_range);

                    for (i, v) in au_scan_buffer.iter().take(au_range).enumerate() {
                        assert_eq!(v.1, *bt_range[i].1);
                        assert_eq!(v.0, *bt_range[i].0);
                    }

                    for (i, v) in art_scan_buffer.iter().take(art_range).enumerate() {
                        assert_eq!(v.1, *bt_range[i].1);
                        assert_eq!(v.0, *bt_range[i].0);
                    }
                }
            }
        }
    }

    let guard = art_usize.pin();
    for (k, v) in bt_map.iter() {
        assert_eq!(art_usize.get(k, &guard).unwrap(), *v);
    }
}

#[test]
fn insert() {
    let key_cnt = 1_000;
    let mut ops = vec![];
    for i in 0..key_cnt {
        ops.push(TreeOp::Insert { key: i, val: i });
    }
    test_runner(&ops);
}

#[test]
fn large_insert() {
    let key_cnt = 100_000;
    let mut ops: Vec<TreeOp> = (0..key_cnt)
        .map(|i| TreeOp::Insert { key: i, val: i })
        .collect();
    for i in 0..key_cnt {
        ops.push(TreeOp::Get { key: i });
    }
    for i in key_cnt..2 * key_cnt {
        ops.push(TreeOp::Get { key: i });
    }

    test_runner(&ops);
}

#[test]
fn rng_insert() {
    let key_cnt = 30_000;
    let mut key_space = Vec::with_capacity(key_cnt);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }

    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let mut ops = vec![];
    for i in 0..key_cnt {
        ops.push(TreeOp::Insert {
            key: key_space[i],
            val: key_space[i],
        });
    }

    for i in 0..key_cnt {
        ops.push(TreeOp::Get { key: key_space[i] });
    }

    for i in key_cnt..2 * key_cnt {
        ops.push(TreeOp::Get { key: i });
    }
    test_runner(&ops);
}

#[test]
fn delete_on_n48() {
    // Test the bug found in n48
    let mut ops = vec![];

    for i in 0..24 {
        ops.push(TreeOp::Insert { key: i, val: 0 });
    }
    for i in 24..48 {
        ops.push(TreeOp::Insert { key: i, val: 1 });
    }
    for i in 24..30 {
        ops.push(TreeOp::Delete { key: i });
    }
    for i in 24..30 {
        ops.push(TreeOp::Insert { key: i, val: 1 });
    }
    for i in 0..24 {
        ops.push(TreeOp::Get { key: i });
    }
    test_runner(&ops);
}

#[test]
fn remove() {
    let key_cnt = 100_000;
    let mut ops = vec![];
    for i in 0..key_cnt {
        ops.push(TreeOp::Insert { key: i, val: i });
    }
    for i in 0..key_cnt / 2 {
        ops.push(TreeOp::Delete { key: i });
    }
    test_runner(&ops);
}

#[test]
fn small_scan() {
    let mut ops = vec![];
    for i in 0..1000 {
        ops.push(TreeOp::Insert { key: i, val: i });
    }
    for i in 0..100 {
        ops.push(TreeOp::Range { low_v: i, cnt: 3 });
    }
    test_runner(&ops);
}

#[test]
fn fuzz_0() {
    let key: usize = 4294967295;
    let ops = vec![
        TreeOp::Insert { key: key, val: key },
        TreeOp::Insert { key: key, val: key },
        TreeOp::Get { key: key },
    ];

    test_runner(&ops);
}

#[cfg(feature = "db_extension")]
#[test]
fn compute_if_present() {
    let tree = ArtUsize::new();
    let guard = tree.pin();
    tree.insert(1, 42, &guard);
    let (old_v, new_v) = tree
        .compute_if_present(
            &1,
            |v| {
                assert_eq!(v, 42);
                v + 1
            },
            &guard,
        )
        .unwrap();
    assert_eq!(old_v, 42);
    assert_eq!(new_v, 43);

    let mut tmp_v = 0;
    let (old_v, new_v) = tree
        .compute_if_present(
            &1,
            |v| {
                assert_eq!(v, 43);
                tmp_v = v;
                v + 1
            },
            &guard,
        )
        .unwrap();
    assert_eq!(old_v, 43);
    assert_eq!(new_v, 44);
    assert_eq!(tmp_v, 43);
}

#[cfg(feature = "db_extension")]
#[test]
fn random_value() {
    let tree = ArtUsize::new();
    let guard = tree.pin();
    tree.insert(1, 42, &guard);
    let mut rng = rand::thread_rng();
    let (key, old_v, new_v) = tree
        .compute_on_random(
            &mut rng,
            |k, v| {
                assert_eq!(k, 1);
                assert_eq!(v, 42);
                v + 1
            },
            &guard,
        )
        .unwrap();
    assert_eq!(key, 1);
    assert_eq!(old_v, 42);
    assert_eq!(new_v, 43);

    let mut tmp_v = 0;
    let (key, old_v, new_v) = tree
        .compute_on_random(
            &mut rng,
            |k, v| {
                assert_eq!(k, 1);
                assert_eq!(v, 43);
                tmp_v = v;
                v + 1
            },
            &guard,
        )
        .unwrap();
    assert_eq!(key, 1);
    assert_eq!(old_v, 43);
    assert_eq!(new_v, 44);
    assert_eq!(tmp_v, 43);
}

#[cfg(feature = "db_extension")]
#[test]
fn compare_exchange() {
    let tree = ArtUsize::new();
    let guard = tree.pin();
    tree.insert(1, 42, &guard);

    let v = tree.compare_exchange(&1, &42, 43, &guard).unwrap();
    assert_eq!(v, 43);

    if let Err(v) = tree.compare_exchange(&1, &42, 45, &guard) {
        assert_eq!(v.unwrap(), 43);
    } else {
        panic!("should have failed");
    }

    if let Err(v) = tree.compare_exchange(&0, &43, 42, &guard) {
        assert!(v.is_none());
    } else {
        panic!("should have failed");
    }
}
