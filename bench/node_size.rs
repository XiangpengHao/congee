use congee::{CongeeSet, DefaultAllocator, MemoryStatsAllocator};
use rand::Rng;

fn bench_workload(key_gen: impl Fn(usize) -> usize) {
    let set = CongeeSet::new(MemoryStatsAllocator::new(DefaultAllocator {}));
    let mut guard = set.pin();

    let value_cnt = 100_000_000;
    for i in 0..value_cnt {
        set.insert(key_gen(i), &guard).unwrap();
        if i % 10_000_000 == 0 {
            guard.flush();
            guard = set.pin();
            let allocated = set.allocated_bytes();
            let deallocated = set.deallocated_bytes();
            println!(
                "inserted: {} keys, allocated: {} bytes, deallocated: {} bytes",
                i, allocated, deallocated
            );
        }
    }

    guard.flush();
    drop(guard);
    for _ in 0..128 {
        crossbeam_epoch::pin().flush();
    }

    println!(
        "total keys: {}, total allocated: {} bytes, total deallocated: {} bytes, {} bytes per key",
        value_cnt,
        set.allocated_bytes(),
        set.deallocated_bytes(),
        set.allocated_bytes() / value_cnt,
    );

    let stats = set.stats();
    println!("{}", stats);
}

fn main() {
    println!("====sequential_keys====");
    bench_workload(|i| i);

    println!("====random_keys====");
    bench_workload(|_i| rand::thread_rng().gen_range(0..usize::MAX));
}
