use congee::{CongeeSet, CongeeCompactSet};
use serde::{Deserialize, Serialize};
use shumai::{ShumaiBench, config};
use std::fmt::Display;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Serialize, Clone, Copy, Debug, Deserialize)]
pub enum FlatFormat {
    CongeeSet,
    CongeeCompactSet,
}

impl Display for FlatFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Serialize, Clone, Copy, Debug, Deserialize)]
pub enum KeyPattern {
    Sequential,
    Random,
}

impl Display for KeyPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[config(path = "bench/benchmark.toml")]
pub struct CompactSetComparison {
    pub name: String,
    pub threads: Vec<usize>,
    pub time: usize,
    pub dataset_size: Vec<usize>,
    #[matrix]
    pub format: FlatFormat,
    #[matrix] 
    pub key_pattern: KeyPattern,
}

struct CompactSetTestBench {
    congee_set: Option<CongeeSet<usize>>,
    congee_compact_set: Option<CongeeCompactSet<'static>>,
    // compact_set_bytes: Option<Vec<u8>>,
    test_keys: Vec<[u8; 8]>,
    format: FlatFormat,
    dataset_size: usize,
}

impl CompactSetTestBench {
    fn new(format: FlatFormat, key_pattern: KeyPattern, dataset_size: usize) -> Self {
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        let test_keys: Vec<[u8; 8]> = match key_pattern {
            KeyPattern::Sequential => (0..dataset_size).map(|i| (i as u64).to_be_bytes()).collect(),
            KeyPattern::Random => {
                use rand::{Rng, thread_rng};
                let mut rng = thread_rng();
                (0..dataset_size).map(|_| {
                    let key: u64 = rng.r#gen();
                    key.to_be_bytes()
                }).collect()
            }
        };
        for key in &test_keys {
            tree.insert(usize::from_be_bytes(*key), &guard).unwrap();
        }
        
        println!("Tree stats: \n{}", tree.stats());
        // let mut compact_set_bytes = None;
        
        let (congee_set, congee_compact_set) = match format {
            FlatFormat::CongeeSet => (Some(tree), None),
            FlatFormat::CongeeCompactSet => {
                let bytes = tree.to_compact_set();
                let leaked_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
                let compact_set = CongeeCompactSet::new(leaked_bytes);
                // compact_set_bytes = Some(leaked_bytes.to_vec());
                (None, Some(compact_set))
            },
        };
        
        Self {
            congee_set,
            congee_compact_set,
            // compact_set_bytes,
            test_keys,
            format,
            dataset_size,
        }
    }
    
    // fn get_memory_usage(&self) -> usize {
    //     match self.format {
    //         FlatFormat::CongeeSet => {
    //             self.congee_set.as_ref().unwrap().stats().total_memory_bytes()
    //         },
    //         FlatFormat::CongeeCompactSet => {
    //             // Include both data array and node_offsets array overhead
    //             self.congee_compact_set.as_ref().unwrap().total_memory_bytes()
    //         },
    //     }
    // }
}

impl ShumaiBench for CompactSetTestBench {
    type Config = CompactSetComparison;
    type Result = usize;

    fn load(&mut self) -> Option<serde_json::Value> {
        // let memory_bytes = self.get_memory_usage();
        // let bytes_per_key = memory_bytes as f64 / self.dataset_size as f64;
        
        Some(serde_json::json!({
            "format": format!("{:?}", self.format),
            "dataset_size": self.dataset_size,
            // "memory_bytes": memory_bytes,
            // "bytes_per_key": bytes_per_key,
        }))
    }

    fn run(&self, context: shumai::Context<Self::Config>) -> Self::Result {
        let mut op_count = 0;
        
        // Reset access stats before benchmarking for CompactSet
        #[cfg(feature = "access-stats")]
        if let Some(compact_set) = &self.congee_compact_set {
            compact_set.reset_access_stats();
        }
        
        context.wait_for_start();
        
        let mut key_idx = 0;
        while context.is_running() {
            // Cycle through test keys for consistent access pattern
            if key_idx >= self.test_keys.len() {
                key_idx = 0;
            }
            
            let key = &self.test_keys[key_idx];
            let _found = match self.format {
                FlatFormat::CongeeSet => {
                    let guard = self.congee_set.as_ref().unwrap().pin();
                    self.congee_set.as_ref().unwrap().contains(&usize::from_be_bytes(*key), &guard)
                },
                FlatFormat::CongeeCompactSet => {
                    self.congee_compact_set.as_ref().unwrap().contains(key)
                },
            };
            
            op_count += 1;
            key_idx += 1;
        }
        
        op_count
    }

    fn cleanup(&mut self) -> Option<serde_json::Value> {
        // Collect access frequency statistics for CompactSet
        #[cfg(feature = "access-stats")]
        {
            if let Some(compact_set) = &self.congee_compact_set {
                
                let access_stats = compact_set.get_access_stats();
                let stats = compact_set.stats();
                let (n4_ratio, n16_ratio, n48_ratio, n256_ratio) = stats.access_ratios();
                let (n4_dist, n16_dist, n48_dist, n256_dist) = stats.access_distribution();
                
                let total_nodes = stats.total_internal_nodes() + stats.total_leaf_nodes();
                
                println!("\n=== Node Distribution Analysis ===");
                println!("N4_Internal: {}, N4_Leaf: {} (Total: {})", 
                         stats.n4_internal_count, stats.n4_leaf_count, 
                         stats.n4_internal_count + stats.n4_leaf_count);
                println!("N16_Internal: {}, N16_Leaf: {} (Total: {})", 
                         stats.n16_internal_count, stats.n16_leaf_count,
                         stats.n16_internal_count + stats.n16_leaf_count);
                println!("N48_Internal: {}, N48_Leaf: {} (Total: {})", 
                         stats.n48_internal_count, stats.n48_leaf_count,
                         stats.n48_internal_count + stats.n48_leaf_count);
                println!("N256_Internal: {}, N256_Leaf: {} (Total: {})", 
                         stats.n256_internal_count, stats.n256_leaf_count,
                         stats.n256_internal_count + stats.n256_leaf_count);
                
                println!("\n=== Access Frequency Analysis ===");
                println!("Total Accesses: {}", stats.total_accesses());
                println!("N4 Accesses: {} ({:.1}%) - Internal: {}, Leaf: {}", 
                         access_stats.n4_accesses, n4_dist, 
                         access_stats.n4_internal_accesses, access_stats.n4_leaf_accesses);
                println!("N16 Accesses: {} ({:.1}%) - Internal: {}, Leaf: {}", 
                         access_stats.n16_accesses, n16_dist,
                         access_stats.n16_internal_accesses, access_stats.n16_leaf_accesses);
                println!("N48 Accesses: {} ({:.1}%) - Internal: {}, Leaf: {}", 
                         access_stats.n48_accesses, n48_dist,
                         access_stats.n48_internal_accesses, access_stats.n48_leaf_accesses);
                println!("N256 Accesses: {} ({:.1}%) - Internal: {}, Leaf: {}", 
                         access_stats.n256_accesses, n256_dist,
                         access_stats.n256_internal_accesses, access_stats.n256_leaf_accesses);
                
                return Some(serde_json::json!({
                    "access_frequency": {
                        "total_accesses": stats.total_accesses(),
                        "n4_accesses": access_stats.n4_accesses,
                        "n16_accesses": access_stats.n16_accesses,
                        "n48_accesses": access_stats.n48_accesses,
                        "n256_accesses": access_stats.n256_accesses,
                        "detailed_accesses": {
                            "n4_internal": access_stats.n4_internal_accesses,
                            "n4_leaf": access_stats.n4_leaf_accesses,
                            "n16_internal": access_stats.n16_internal_accesses,
                            "n16_leaf": access_stats.n16_leaf_accesses,
                            "n48_internal": access_stats.n48_internal_accesses,
                            "n48_leaf": access_stats.n48_leaf_accesses,
                            "n256_internal": access_stats.n256_internal_accesses,
                            "n256_leaf": access_stats.n256_leaf_accesses
                        },
                        "access_ratios": {
                            "n4_ratio": n4_ratio,
                            "n16_ratio": n16_ratio,
                            "n48_ratio": n48_ratio,
                            "n256_ratio": n256_ratio
                        },
                        "access_distribution": {
                            "n4_percent": n4_dist,
                            "n16_percent": n16_dist,
                            "n48_percent": n48_dist,
                            "n256_percent": n256_dist
                        }
                    },
                    "node_counts": {
                        "total_nodes": total_nodes,
                        "n4_total": stats.n4_internal_count + stats.n4_leaf_count,
                        "n16_total": stats.n16_internal_count + stats.n16_leaf_count,
                        "n48_total": stats.n48_internal_count + stats.n48_leaf_count,
                        "n256_total": stats.n256_internal_count + stats.n256_leaf_count
                    },
                    "node_distribution": {
                        "n4_internal": stats.n4_internal_count,
                        "n4_leaf": stats.n4_leaf_count,
                        "n16_internal": stats.n16_internal_count,
                        "n16_leaf": stats.n16_leaf_count,
                        "n48_internal": stats.n48_internal_count,
                        "n48_leaf": stats.n48_leaf_count,
                        "n256_internal": stats.n256_internal_count,
                        "n256_leaf": stats.n256_leaf_count
                    }
                }));
            }
        }
        
        None
    }
}

fn main() {
    let config = CompactSetComparison::load().expect("Failed to parse config!");
    let repeat = 3;

    println!("Config: {:?}", config);
    for c in config.iter() {
        println!("Current config: {:?}", c);
        for &dataset_size in &c.dataset_size {
            let mut test_bench = CompactSetTestBench::new(c.format, c.key_pattern, dataset_size);
            let result = shumai::run(&mut test_bench, c, repeat);
            result.write_json().unwrap();
        }
    }
}