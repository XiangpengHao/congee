use std::time::Instant;
use congee::{CongeeSet, CongeeFlat, CongeeFlatStruct, CongeeCompact, CongeeCompactV2};
// use congee::{CongeeSet, CongeeFlat, CongeeFlatStruct};

fn main() {
    let tree = CongeeSet::<usize>::default();
    let guard = tree.pin();

    // Insert test keys: 1,2,3,4,5 and 1000-1014 and 10002-10004
    for i in 1..=5 {
        tree.insert(i, &guard).unwrap();
    }
    for i in 1000..=1014 {
        tree.insert(i, &guard).unwrap();
    }
    for i in 100002..=100004 {
        tree.insert(i, &guard).unwrap();
    }

    let set_stats = tree.stats();
    println!("Congee set stats: \n{}", set_stats);
    println!("Total keys inserted: {}", 5 + 15 + 3);

    // Generate all formats
    let columnar_bytes = tree.to_flatbuffer();
    let struct_bytes = tree.to_flatbuffer_struct();
    let compact_bytes = tree.to_compact();
    let compact_v2_bytes = tree.to_compact_v2();
    
    println!("\n*** Size Comparison ***");
    println!("CongeeFlat (columnar): {} bytes", columnar_bytes.len());
    println!("CongeeFlatStruct: {} bytes", struct_bytes.len());
    println!("CongeeCompact: {} bytes", compact_bytes.len());
    println!("CongeeCompactV2: {} bytes", compact_v2_bytes.len());
    println!("CongeeSet: {} bytes", set_stats.total_memory_bytes());
    
    println!("\nMemory savings (CongeeCompact vs others):");
    println!("vs CongeeFlat: {:.1}x smaller", columnar_bytes.len() as f64 / compact_bytes.len() as f64);
    println!("vs CongeeFlatStruct: {:.1}x smaller", struct_bytes.len() as f64 / compact_bytes.len() as f64);
    println!("vs CongeeSet: {:.1}x smaller", set_stats.total_memory_bytes() as f64 / compact_bytes.len() as f64);
    
    println!("\nMemory savings (CongeeCompactV2 vs others):");
    println!("vs CongeeFlat: {:.1}x smaller", columnar_bytes.len() as f64 / compact_v2_bytes.len() as f64);
    println!("vs CongeeFlatStruct: {:.1}x smaller", struct_bytes.len() as f64 / compact_v2_bytes.len() as f64);
    println!("vs CongeeCompact: {:.1}x smaller", compact_bytes.len() as f64 / compact_v2_bytes.len() as f64);
    println!("vs CongeeSet: {:.1}x smaller", set_stats.total_memory_bytes() as f64 / compact_v2_bytes.len() as f64);

    // Create readers
    let congee_flat = CongeeFlat::new(&columnar_bytes);
    let congee_flat_struct = CongeeFlatStruct::new(&struct_bytes);
    let congee_compact = CongeeCompact::new(&compact_bytes);
    let congee_compact_v2 = CongeeCompactV2::new(&compact_v2_bytes);
    
    // Debug structures
    println!("\n*** Debug Structures ***");
    congee_flat_struct.debug_print();
    congee_compact.debug_print();
    congee_compact_v2.debug_print();
    
    println!("\n*** CongeeCompactV2 Stats ***");
    let compact_v2_stats = congee_compact_v2.stats();
    println!("{}", compact_v2_stats);
    
    println!("Accurate memory efficiency vs CongeeSet: {:.1}x smaller", 
             compact_v2_stats.memory_efficiency_vs_congee_set(set_stats.total_memory_bytes()));
    
    // // Test correctness
    let test_keys = vec![
        1u64.to_be_bytes(),
        1005u64.to_be_bytes(),
        10003u64.to_be_bytes(),
        99999u64.to_be_bytes(), // missing key
    ];
    
    println!("\n*** Correctness Test ***");
    for key in &test_keys {
        let start = Instant::now();
        let expected = tree.contains(&usize::from_be_bytes(*key), &guard);
        let duration = start.elapsed();
        println!("CongeeSet: {} in {:?}", expected, duration);
        let start = Instant::now();
        let flat_result = congee_flat.contains(key);
        let duration = start.elapsed();
        println!("CongeeFlat: {} in {:?}", flat_result, duration);
        let start = Instant::now();
        let struct_result = congee_flat_struct.contains(key);
        let duration = start.elapsed();
        println!("CongeeFlatStruct: {} in {:?}", struct_result, duration);
        let start = Instant::now();
        let compact_result = congee_compact.contains(key);
        let duration = start.elapsed();
        println!("CongeeCompact: {} in {:?}", compact_result, duration);
        let start = Instant::now();
        let compact_v2_result = congee_compact_v2.contains(key);
        let duration = start.elapsed();
        println!("CongeeCompactV2: {} in {:?}", compact_v2_result, duration);
        println!("Key {:?}: expected={}, flat={}, struct={}, compact={}, compact_v2={}", 
                 key, expected, flat_result, struct_result, compact_result, compact_v2_result);
        
        assert_eq!(expected, flat_result, "CongeeFlat mismatch");
        assert_eq!(expected, struct_result, "CongeeFlatStruct mismatch");
        assert_eq!(expected, compact_result, "CongeeCompact mismatch");
        assert_eq!(expected, compact_v2_result, "CongeeCompactV2 mismatch");
    }
    
    println!("\n*** Performance Test ***");
    let iterations = 100_000;
    let perf_keys: Vec<[u8; 8]> = (0..100).map(|i| (i as u64).to_be_bytes()).collect();

    // CongeeFlat performance
    let start = Instant::now();
    for _ in 0..iterations {
        for key in &perf_keys {
            let _ = congee_flat.contains(key);
        }
    }
    let flat_duration = start.elapsed();
    
    // CongeeFlatStruct performance  
    let start = Instant::now();
    for _ in 0..iterations {
        for key in &perf_keys {
            let _ = congee_flat_struct.contains(key);
        }
    }
    let struct_duration = start.elapsed();
    
    // CongeeCompact performance
    let start = Instant::now();
    for _ in 0..iterations {
        for key in &perf_keys {
            let _ = congee_compact.contains(key);
        }
    }
    let compact_duration = start.elapsed();
    
    // CongeeCompactV2 performance
    let start = Instant::now();
    for _ in 0..iterations {
        for key in &perf_keys {
            let _ = congee_compact_v2.contains(key);
        }
    }
    let compact_v2_duration = start.elapsed();
    
    // CongeeSet performance (baseline)
    let start = Instant::now();
    for _ in 0..iterations {
        for key in &perf_keys {
            let _ = tree.contains(&usize::from_be_bytes(*key), &guard);
        }
    }
    let set_duration = start.elapsed();
    
    let total_ops = iterations * perf_keys.len();
    println!("CongeeFlat: {} ops in {:?} ({:.0} ops/sec)", 
             total_ops, flat_duration, total_ops as f64 / flat_duration.as_secs_f64());
    println!("CongeeFlatStruct: {} ops in {:?} ({:.0} ops/sec)", 
             total_ops, struct_duration, total_ops as f64 / struct_duration.as_secs_f64());
    println!("CongeeCompact: {} ops in {:?} ({:.0} ops/sec)", 
             total_ops, compact_duration, total_ops as f64 / compact_duration.as_secs_f64());
    println!("CongeeCompactV2: {} ops in {:?} ({:.0} ops/sec)", 
             total_ops, compact_v2_duration, total_ops as f64 / compact_v2_duration.as_secs_f64());
    println!("CongeeSet (baseline): {} ops in {:?} ({:.0} ops/sec)", 
             total_ops, set_duration, total_ops as f64 / set_duration.as_secs_f64());
             
    println!("\nSpeedup (CongeeCompact vs FlatBuffers):");
    println!("vs CongeeFlat: {:.1}x faster", flat_duration.as_secs_f64() / compact_duration.as_secs_f64());
    println!("vs CongeeFlatStruct: {:.1}x faster", struct_duration.as_secs_f64() / compact_duration.as_secs_f64());
    
    println!("\nSpeedup (CongeeCompactV2 vs others):");
    println!("vs CongeeFlat: {:.1}x faster", flat_duration.as_secs_f64() / compact_v2_duration.as_secs_f64());
    println!("vs CongeeFlatStruct: {:.1}x faster", struct_duration.as_secs_f64() / compact_v2_duration.as_secs_f64());
    println!("vs CongeeCompact: {:.1}x faster", compact_duration.as_secs_f64() / compact_v2_duration.as_secs_f64());
}