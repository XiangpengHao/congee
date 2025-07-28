use std::time::Instant;
use rand::{Rng, thread_rng};
use congee::CongeeSet;

fn hash_key(key: usize) -> usize {
    const MULTIPLIER: usize = 0x9e3779b97f4a7c15;
    key.wrapping_mul(MULTIPLIER)
}

fn setup_tree() -> (CongeeSet<usize>, crossbeam_epoch::Guard) {
    println!("Setting up tree with data (not profiled)...");
    
    let tree = CongeeSet::<usize>::default();
    let guard = tree.pin();
    
    // Smaller dataset for faster setup
    let initial_cnt = 1_000_000; // 1M keys for faster setup
    
    println!("Inserting {} keys...", initial_cnt);
    let insert_start = Instant::now();
    
    // Insert keys with same pattern as benchmark
    for i in 0..initial_cnt {
        if i % 2 == 0 {
            tree.insert(hash_key(i), &guard).unwrap();
        } else {
            tree.insert(i, &guard).unwrap();
        }
    }
    
    let insert_duration = insert_start.elapsed();
    println!("Setup completed in {:?}", insert_duration);
    
    (tree, guard)
}

fn main() {
    println!("Benchmark workload replication test - Flamegraph optimized");
    
    // Setup phase - not profiled
    let (tree, guard) = setup_tree();
    
    println!("\n=== STARTING PROFILED SECTION ===");
    println!("This is the hot path that will be profiled by flamegraph");
    
    // Increased scope for better flamegraph visibility
    let iterations = 20_000;    // 20K iterations 
    let lookups_per_iter = 1000; // 1K lookups per iteration = 20M total lookups
    let initial_cnt = 1_000_000; // Match setup size
    let mut rng = thread_rng();
    
    let start = Instant::now();
    let mut found_count = 0;
    
    // This loop is what we want to profile - the contains() performance
    for _ in 0..iterations {
        for _ in 0..lookups_per_iter {
            let val = rng.gen_range(0..initial_cnt);
            if tree.contains(&val, &guard) {
                found_count += 1;
            }
        }
    }
    
    let duration = start.elapsed();
    let total_lookups = iterations * lookups_per_iter;
    
    println!("=== PROFILED SECTION COMPLETED ===");
    println!("Performance test results:");
    println!("- Total lookups: {}", total_lookups);
    println!("- Found: {}", found_count);
    println!("- Duration: {:?}", duration);
    println!("- Lookups per second: {:.0}", total_lookups as f64 / duration.as_secs_f64());
    println!("- Average lookup time: {:.2}ns", duration.as_nanos() as f64 / total_lookups as f64);
}