#![no_main]
use arbitrary::Arbitrary;
use congee::Art;
use libfuzzer_sys::fuzz_target;

/// Follow the tutorial from this post: https://tiemoko.com/blog/diff-fuzz/
#[derive(Arbitrary, Debug)]
enum MapMethod {
    Insert {
        key: usize,
        val: usize,
    },
    Range {
        low_v: usize,
        cnt: u8,
        buff_size: u8,
    },
}

fuzz_target!(|methods: Vec<MapMethod>| {
    let art = Art::new();

    let mut art_scan_buffer = [(0, 0); 256];
    for m_c in methods.chunks(1024) {
        for m in m_c {
            let guard = art.pin();
            match m {
                MapMethod::Insert { key, val } => {
                    art.insert(*key, *val, &guard);
                }
                MapMethod::Range {
                    low_v,
                    cnt,
                    buff_size,
                } => {
                    let low_v = *low_v as usize;
                    let cnt = *cnt as usize;

                    let low_key = low_v;
                    let high_key = low_v + cnt;
                    let art_range = art.range(&low_key, &high_key, &mut art_scan_buffer, &guard);

                    assert!(art_range <= *buff_size as usize);

                    for (_i, v) in art_scan_buffer.iter().take(art_range).enumerate() {
                        assert!(v.1 >= low_v);
                        assert!(v.1 < low_v + cnt);
                    }
                }
            }
        }
    }
});
