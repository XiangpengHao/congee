#![no_main]
use arbitrary::Arbitrary;
use con_art_rust::{Key, UsizeKey, Tree};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

#[derive(Arbitrary, Debug)]
enum MapMethod {
    Get { key: u32 },
    Insert { key: u32 },
    Range { low_v: u32, cnt: u8 },
}

fuzz_target!(|methods: Vec<MapMethod>| {
    let capacity = 100_000;
    let art = Tree::new();
    let mut bt_map = BTreeMap::new();

    let guard = art.pin();

    let mut art_scan_buffer = vec![0; 256];

    for m in methods {
        match m {
            MapMethod::Get { key } => {
                let key = key as usize;
                assert_eq!(
                    art.get(&UsizeKey::key_from(key), &guard),
                    bt_map.get(&key).map(|v| { *v })
                );
            }
            MapMethod::Insert { key } => {
                let key = key as usize;
                if bt_map.len() < capacity {
                    art.insert(usize::key_from(key), key, &guard);
                    bt_map.insert(key, key);
                }
            }
            MapMethod::Range { low_v, cnt } => {
                let low_v = low_v as usize;
                let cnt = cnt as usize;

                let low_key = usize::key_from(low_v);
                let high_key = usize::key_from(low_v + cnt);
                let art_range = art.look_up_range(&low_key, &high_key, &mut art_scan_buffer);
                let bt_range: Vec<(&usize, &usize)> = bt_map.range(low_v..(low_v + cnt)).collect();

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
});
