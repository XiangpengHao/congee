#![no_main]
use arbitrary::Arbitrary;
use congee::Congee;
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

/// Follow the tutorial from this post: https://tiemoko.com/blog/diff-fuzz/
#[derive(Arbitrary, Debug)]
enum MapMethod {
    Get { key: usize },
    Insert { key: usize, val: usize },
    Update { key: usize, val: usize },
    Range { low_v: usize, cnt: u8 },
    Delete { key: usize },
}

fuzz_target!(|methods: Vec<MapMethod>| {
    let capacity = 10_000_000;
    let art = Congee::<usize, usize>::default();
    let mut bt_map = BTreeMap::new();

    let mut art_scan_buffer = vec![(0, 0); 128];

    for m_c in methods.chunks(1024) {
        let guard = art.pin();
        for m in m_c {
            match m {
                MapMethod::Get { key } => {
                    let art_v = art.get(key, &guard);
                    let bt_v = bt_map.get(key).map(|v| *v);
                    let art_v2 = art
                        .compute_if_present(key, |v| Some(v), &guard)
                        .map(|(v, _)| v);
                    assert_eq!(art_v, bt_v);
                    assert_eq!(art_v2, bt_v);
                }
                MapMethod::Insert { key, val } => {
                    if bt_map.len() < capacity {
                        let btree_insert = bt_map.insert(*key, *val);
                        if key % 2 == 0 {
                            let a_insert = art.insert(*key, *val, &guard).unwrap();
                            assert_eq!(a_insert, btree_insert);
                        } else {
                            let a_insert =
                                art.compute_or_insert(*key, |_old| *val, &guard).unwrap();
                            assert_eq!(a_insert, btree_insert);
                        }
                    }
                }
                MapMethod::Update { key, val } => {
                    let old_bt = bt_map.get_mut(key);
                    let old_art = art.compute_if_present(key, |_v| Some(*val), &guard);
                    if let Some(old_bt) = old_bt {
                        assert_eq!(old_art, Some((*old_bt, Some(*val))));
                        *old_bt = *val;
                    } else {
                        assert_eq!(old_art, None);
                    }
                }
                MapMethod::Delete { key } => {
                    bt_map.remove(key);
                    art.remove(key, &guard);
                }
                MapMethod::Range { low_v, cnt } => {
                    let cnt = *cnt as usize;

                    // prevent integer overflow
                    let high_key = if (usize::MAX - low_v) <= cnt {
                        usize::MAX
                    } else {
                        low_v + cnt
                    };

                    let art_range = art.range(low_v, &high_key, &mut art_scan_buffer, &guard);
                    let bt_range: Vec<(&usize, &usize)> = bt_map.range(*low_v..high_key).collect();

                    assert_eq!(bt_range.len(), art_range);

                    for (i, v) in art_scan_buffer.iter().take(art_range).enumerate() {
                        assert_eq!(v.1, *bt_range[i].1);
                        assert_eq!(v.0, *bt_range[i].0);
                    }
                }
            }
        }
    }

    let guard = art.pin();
    for (k, v) in bt_map.iter() {
        assert_eq!(art.get(k, &guard).unwrap(), *v);
    }
});
