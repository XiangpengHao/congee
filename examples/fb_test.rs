use std::env;
use std::time::Instant;
use congee::{CongeeSet, CongeeFlat, CongeeFlatStruct, CongeeCompact};

fn main() {
    let args: Vec<String> = env::args().collect();
    let test_types = if args.len() > 1 {
        args[1..].to_vec()
    } else {
        vec!["all".to_string()]
    };

    let run_flat = test_types.contains(&"flat".to_string()) || test_types.contains(&"all".to_string());
    let run_struct = test_types.contains(&"struct".to_string()) || test_types.contains(&"all".to_string());
    let run_compact = test_types.contains(&"compact".to_string()) || test_types.contains(&"all".to_string());
    let run_set = test_types.contains(&"set".to_string()) || test_types.contains(&"all".to_string());
    let tree = CongeeSet::<usize>::default();
    let guard = tree.pin();

    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 1]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 2]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 3]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 4]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([1, 1, 2, 3, 4, 5, 6, 5]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 1]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 2]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 3]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 4]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 5]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 6]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 7]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 8]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 9]), &guard).unwrap();
    tree.insert(usize::from_be_bytes([2, 1, 2, 3, 4, 5, 6, 10]), &guard).unwrap();

    
    // random keys
    // let mut rng = rand::thread_rng();
    // for _ in 0..16000 {
    //     let mut key = [0u8; 8];
    //     rng.fill(&mut key);
    //     tree.insert(usize::from_be_bytes(key), &guard).unwrap();
    // }

    // sequential keys
    for i in 0..16000 {
        let key = (i as u64).to_be_bytes();
        tree.insert(usize::from_be_bytes(key), &guard).unwrap();
    }

    // let set_stats = tree.stats();
    // println!("Congee set stats: \n{}", set_stats);


    let columnar_bytes = if run_flat { Some(tree.to_flatbuffer()) } else { None };
    let congee_flat = columnar_bytes.as_ref().map(|bytes| CongeeFlat::new(bytes));
    
    let struct_bytes = if run_struct { Some(tree.to_flatbuffer_struct()) } else { None };
    let congee_flat_struct = struct_bytes.as_ref().map(|bytes| CongeeFlatStruct::new(bytes));

    let compact_bytes = if run_compact { Some(tree.to_compact()) } else { None };
    let congee_compact = compact_bytes.as_ref().map(|bytes| CongeeCompact::new(bytes));

    // println!("\n*** memory comparison ***");
    // println!("CongeeSet memory: {} bytes", set_stats.total_memory_bytes());
    
    // if let Some(ref bytes) = columnar_bytes {
    //     println!("CongeeFlat memory: {} bytes - {:.1}% reduction ({:.2}x smaller)", 
    //             bytes.len(), 
    //             (1.0 - bytes.len() as f64 / set_stats.total_memory_bytes() as f64) * 100.0, 
    //             set_stats.total_memory_bytes() as f64 / bytes.len() as f64);
    // }
    
    // if let Some(ref bytes) = struct_bytes {
    //     println!("CongeeFlatStruct memory: {} bytes - {:.1}% reduction ({:.2}x smaller)", 
    //             bytes.len(), 
    //             (1.0 - bytes.len() as f64 / set_stats.total_memory_bytes() as f64) * 100.0, 
    //             set_stats.total_memory_bytes() as f64 / bytes.len() as f64);
    // }
    
    // if let Some(ref bytes) = compact_bytes {
    //     println!("CongeeCompact memory: {} bytes - {:.1}% reduction ({:.2}x smaller)", 
    //             bytes.len(), 
    //             (1.0 - bytes.len() as f64 / set_stats.total_memory_bytes() as f64) * 100.0, 
    //             set_stats.total_memory_bytes() as f64 / bytes.len() as f64);
    // }

    // println!("\n*** performance tests ***");
    let iterations = 100000;
    let test_keys: Vec<[u8; 8]> = (0..1000).map(|i| (i as u64).to_be_bytes()).collect();

    if let Some(ref flat) = congee_flat {
        // println!("Running CongeeFlat performance test...");
        let start = Instant::now();
        for _ in 0..iterations {
            for key in &test_keys {
                let _ = flat.contains(key);
            }
        }
        let duration = start.elapsed();
        // println!("CongeeFlat: {} contains() calls in {:?}", iterations * test_keys.len(), duration);
    }

    if let Some(ref flat_struct) = congee_flat_struct {
        // println!("Running CongeeFlatStruct performance test...");
        let start = Instant::now();
        for _ in 0..iterations {
            for key in &test_keys {
                let _ = flat_struct.contains(key);
            }
        }
        let duration = start.elapsed();
        // println!("CongeeFlatStruct: {} contains() calls in {:?}", iterations * test_keys.len(), duration);
    }

    if let Some(ref compact) = congee_compact {
        // println!("Running CongeeCompact performance test...");
        let start = Instant::now();
        for _ in 0..iterations {
            for key in &test_keys {
                let _ = compact.contains(key);
            }
        }
        let duration = start.elapsed();
        // println!("CongeeCompact: {} contains() calls in {:?}", iterations * test_keys.len(), duration);
    }
    
    if run_set {
        // println!("Running CongeeSet performance test...");
        let start = Instant::now();
        for _ in 0..iterations {
            for key in &test_keys {
                let _ = tree.contains(&usize::from_be_bytes(*key), &guard);
            }
        }
        let duration = start.elapsed();
        // println!("CongeeSet: {} contains() calls in {:?}", iterations * test_keys.len(), duration);
    }
}
