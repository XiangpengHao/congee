#![no_main]
use arbitrary::Arbitrary;
use congee::{CongeeCompactSet, CongeeSet};
use libfuzzer_sys::fuzz_target;
use std::collections::HashSet;

#[derive(Arbitrary, Debug)]
enum Op {
    Insert { key: usize },
}

fuzz_target!(|ops: Vec<Op>| {
    let set = CongeeSet::<usize>::default();
    let mut hs = HashSet::new();

    // Apply operations to HashSet and CongeeSet
    for chunk in ops.chunks(2048) {
        let guard = set.pin();
        for op in chunk {
            match op {
                Op::Insert { key } => {
                    hs.insert(*key);
                    let _ = set.insert(*key, &guard);
                }
            }
        }
    }

    // Build compact set snapshot
    let data = set.to_compact_set();
    let compact = CongeeCompactSet::<usize>::new(&data);
    let guard = set.pin();

    // Ensure total cardinality matches exactly
    assert_eq!(set.len(&guard), hs.len());
    let stats = compact.stats();
    assert_eq!(stats.kv_pairs, hs.len());

    // Validate membership for every element observed in HashSet
    for key in hs.iter() {
        assert!(set.contains(key, &guard));
        assert!(compact.contains(key));
    }
});
