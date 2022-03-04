#![no_main]
use arbitrary::Arbitrary;
use congee::Art;
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

/// Follow the tutorial from this post: https://tiemoko.com/blog/diff-fuzz/
#[derive(Arbitrary, Debug)]
enum MapMethod {
    Get { key: usize },
    Insert { key: usize, val: usize },
    Range { low_v: usize, cnt: u8 },
    Delete { key: usize },
}

fuzz_target!(|methods: Vec<MapMethod>| {
    let capacity = 10_000_000;
    let art = Art::new();
    let mut bt_map = BTreeMap::new();

    let mut art_scan_buffer = vec![(0, 0); 128];

    for m_c in methods.chunks(1024) {
        let guard = art.pin();
        for m in m_c {
            match m {
                MapMethod::Get { key } => {
                    assert_eq!(art.get(key, &guard), bt_map.get(key).map(|v| { *v }));
                }
                MapMethod::Insert { key, val } => {
                    let val = (*val) & 0x7fff_ffff_ffff_ffff;
                    if bt_map.len() < capacity {
                        art.insert(*key, val, &guard);
                        bt_map.insert(*key, val);
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
