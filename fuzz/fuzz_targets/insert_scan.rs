#![no_main]
use arbitrary::Arbitrary;
use con_art_rust::{Art, Key, UsizeKey};
use libfuzzer_sys::fuzz_target;

/// Follow the tutorial from this post: https://tiemoko.com/blog/diff-fuzz/
#[derive(Arbitrary, Debug)]
enum MapMethod {
    Insert { key: u32 },
    Range { low_v: u32, cnt: u8, buff_size: u8 },
}

fuzz_target!(|methods: Vec<MapMethod>| {
    let art = Art::new();

    for m_c in methods.chunks(1024) {
        for m in m_c {
            let guard = art.pin();
            match m {
                MapMethod::Insert { key } => {
                    let key = *key as usize;
                    let val = key;
                    art.insert(UsizeKey::key_from(key), val, &guard);
                }
                MapMethod::Range {
                    low_v,
                    cnt,
                    buff_size,
                } => {
                    let mut art_scan_buffer = vec![0; *buff_size as usize];
                    let low_v = *low_v as usize;
                    let cnt = *cnt as usize;

                    let low_key = UsizeKey::key_from(low_v);
                    let high_key = UsizeKey::key_from(low_v + cnt);
                    let art_range = art.range(&low_key, &high_key, &mut art_scan_buffer, &guard);

                    for (_i, v) in art_scan_buffer
                        .iter()
                        .take(art_range.unwrap_or(0))
                        .enumerate()
                    {
                        assert!(*v >= low_v);
                        assert!(*v < low_v + cnt);
                    }
                }
            }
        }
    }
});
