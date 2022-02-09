#![no_main]
use arbitrary::Arbitrary;
use con_art_rust::{Key, Tree, UsizeKey};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

/// Follow the tutorial from this post: https://tiemoko.com/blog/diff-fuzz/
#[derive(Arbitrary, Debug)]
enum MapMethod {
    Get { key: u32 },
    Insert { key: u32, val: u32 },
    Range { low_v: u32, cnt: u8 },
    Delete { key: u32 },
}

fuzz_target!(|methods: Vec<MapMethod>| {
    let capacity = 10_000_000;
    let art = Tree::new();
    let mut bt_map = BTreeMap::new();

    let mut art_scan_buffer = vec![0; 128];

    for m_c in methods.chunks(1024) {
        let guard = art.pin();
        for m in m_c {
            match m {
                MapMethod::Get { key } => {
                    let key = *key as usize;
                    assert_eq!(
                        art.get(&UsizeKey::key_from(key), &guard),
                        bt_map.get(&key).map(|v| { *v })
                    );
                }
                MapMethod::Insert { key, val } => {
                    let key = *key as usize;
                    if bt_map.len() < capacity {
                        art.insert(UsizeKey::key_from(key), *val as usize, &guard);
                        bt_map.insert(key, *val as usize);
                    }
                }
                MapMethod::Delete { key } => {
                    let key = *key as usize;
                    bt_map.remove(&key);
                    art.remove(&UsizeKey::key_from(key), &guard);
                }
                MapMethod::Range { low_v, cnt } => {
                    let low_v = *low_v as usize;
                    let cnt = *cnt as usize;

                    let low_key = UsizeKey::key_from(low_v);
                    let high_key = UsizeKey::key_from(low_v + cnt);
                    let art_range = art.range(&low_key, &high_key, &mut art_scan_buffer, &guard);
                    let bt_range: Vec<(&usize, &usize)> =
                        bt_map.range(low_v..(low_v + cnt)).collect();

                    assert_eq!(bt_range.len(), art_range.unwrap_or(0));

                    for (i, v) in art_scan_buffer
                        .iter()
                        .take(art_range.unwrap_or(0))
                        .enumerate()
                    {
                        assert_eq!(v, bt_range[i].1)
                    }
                }
            }
        }
    }

    let guard = art.pin();
    for (k, v) in bt_map.iter() {
        assert_eq!(art.get(&UsizeKey::key_from(*k), &guard).unwrap(), *v);
    }
});
