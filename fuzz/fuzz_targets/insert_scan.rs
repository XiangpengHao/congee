#![no_main]
use arbitrary::Arbitrary;
use con_art_rust::{Key, Tree, UsizeKey};
use libfuzzer_sys::fuzz_target;

/// Follow the tutorial from this post: https://tiemoko.com/blog/diff-fuzz/
#[derive(Arbitrary, Debug)]
enum MapMethod {
    Insert { key: u32, val: u32 },
    Range { low_v: u32, cnt: u8 },
}

fuzz_target!(|methods: Vec<MapMethod>| {
    let art = Tree::new();

    let mut art_scan_buffer = vec![0; 128];

    for m_c in methods.chunks(1024) {
        for m in m_c {
            let guard = art.pin();
            match m {
                MapMethod::Insert { key, val } => {
                    let key = *key as usize;
                    let val = (*val as usize) << 16;
                    art.insert(UsizeKey::key_from(key), val, &guard);
                }
                MapMethod::Range { low_v, cnt } => {
                    let low_v = *low_v as usize;
                    let cnt = *cnt as usize;

                    let low_key = UsizeKey::key_from(low_v);
                    let high_key = UsizeKey::key_from(low_v + cnt);
                    let art_range = art.look_up_range(&low_key, &high_key, &mut art_scan_buffer);

                    for (_i, v) in art_scan_buffer
                        .iter()
                        .take(art_range.unwrap_or(0))
                        .enumerate()
                    {
                        let val = *v;
                        assert_eq!(val & 0xffff, 0);
                    }
                }
            }
        }
    }
});
