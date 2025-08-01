use congee::{CongeeSet, CongeeFlat, CongeeFlatStruct, CongeeCompact, CongeeCompactV2};
use serde::{Deserialize, Serialize};
use shumai::{ShumaiBench, config};
use std::fmt::Display;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Serialize, Clone, Copy, Debug, Deserialize)]
pub enum FlatFormat {
    CongeeSet,
    CongeeFlat, 
    CongeeFlatStruct,
    CongeeCompact,
    CongeeCompactV2,
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
pub struct FlatComparison {
    pub name: String,
    pub threads: Vec<usize>,
    pub time: usize,
    pub dataset_size: Vec<usize>,
    #[matrix]
    pub format: FlatFormat,
    #[matrix] 
    pub key_pattern: KeyPattern,
}

struct FlatTestBench {
    congee_set: Option<CongeeSet<usize>>,
    congee_flat: Option<CongeeFlat<'static>>,
    congee_flat_struct: Option<CongeeFlatStruct<'static>>,
    congee_compact: Option<CongeeCompact<'static>>,
    congee_compact_v2: Option<CongeeCompactV2<'static>>,
    flat_bytes: Option<Vec<u8>>,
    struct_bytes: Option<Vec<u8>>,
    compact_bytes: Option<Vec<u8>>,
    compact_v2_bytes: Option<Vec<u8>>,
    test_keys: Vec<[u8; 8]>,
    format: FlatFormat,
    dataset_size: usize,
}

impl FlatTestBench {
    fn new(format: FlatFormat, key_pattern: KeyPattern, dataset_size: usize) -> Self {
        // Create initial CongeeSet with data
        let tree = CongeeSet::<usize>::default();
        let guard = tree.pin();
        
        // Generate test keys based on pattern
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
        
        // Insert all keys into the tree
        for key in &test_keys {
            tree.insert(usize::from_be_bytes(*key), &guard).unwrap();
        }
        
        println!("Tree stats: \n{}", tree.stats());
        // Generate format-specific data structures
        let mut flat_bytes = None;
        let mut struct_bytes = None;
        let mut compact_bytes = None;
        let mut compact_v2_bytes = None;
        
        let (congee_set, congee_flat, congee_flat_struct, congee_compact, congee_compact_v2) = match format {
            FlatFormat::CongeeSet => (Some(tree), None, None, None, None),
            FlatFormat::CongeeFlat => {
                let bytes = tree.to_flatbuffer();
                // Safety: We're leaking the bytes to make them 'static, but they'll be dropped with the struct
                let leaked_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
                let flat = CongeeFlat::new(leaked_bytes);
                flat_bytes = Some(leaked_bytes.to_vec());
                (None, Some(flat), None, None, None)
            },
            FlatFormat::CongeeFlatStruct => {
                let bytes = tree.to_flatbuffer_struct();
                let leaked_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
                let flat_struct = CongeeFlatStruct::new(leaked_bytes);
                struct_bytes = Some(leaked_bytes.to_vec());
                (None, None, Some(flat_struct), None, None)
            },
            FlatFormat::CongeeCompact => {
                let bytes = tree.to_compact();
                let leaked_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
                let compact = CongeeCompact::new(leaked_bytes);
                compact_bytes = Some(leaked_bytes.to_vec());
                (None, None, None, Some(compact), None)
            },
            FlatFormat::CongeeCompactV2 => {
                let bytes = tree.to_compact_v2();
                let leaked_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
                let compact_v2 = CongeeCompactV2::new(leaked_bytes);
                compact_v2_bytes = Some(leaked_bytes.to_vec());
                (None, None, None, None, Some(compact_v2))
            },
        };
        
        Self {
            congee_set,
            congee_flat,
            congee_flat_struct,
            congee_compact,
            congee_compact_v2,
            flat_bytes,
            struct_bytes,
            compact_bytes,
            compact_v2_bytes,
            test_keys,
            format,
            dataset_size,
        }
    }
    
    fn get_memory_usage(&self) -> usize {
        match self.format {
            FlatFormat::CongeeSet => {
                self.congee_set.as_ref().unwrap().stats().total_memory_bytes()
            },
            FlatFormat::CongeeFlat => self.flat_bytes.as_ref().unwrap().len(),
            FlatFormat::CongeeFlatStruct => self.struct_bytes.as_ref().unwrap().len(),
            FlatFormat::CongeeCompact => self.compact_bytes.as_ref().unwrap().len(),
            FlatFormat::CongeeCompactV2 => self.compact_v2_bytes.as_ref().unwrap().len(),
        }
    }
}

impl ShumaiBench for FlatTestBench {
    type Config = FlatComparison;
    type Result = usize;

    fn load(&mut self) -> Option<serde_json::Value> {
        let memory_bytes = self.get_memory_usage();
        let bytes_per_key = memory_bytes as f64 / self.dataset_size as f64;
        
        Some(serde_json::json!({
            "format": format!("{:?}", self.format),
            "dataset_size": self.dataset_size,
            "memory_bytes": memory_bytes,
            "bytes_per_key": bytes_per_key,
        }))
    }

    fn run(&self, context: shumai::Context<Self::Config>) -> Self::Result {
        let mut op_count = 0;
        
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
                FlatFormat::CongeeFlat => {
                    self.congee_flat.as_ref().unwrap().contains(key)
                },
                FlatFormat::CongeeFlatStruct => {
                    self.congee_flat_struct.as_ref().unwrap().contains(key)
                },
                FlatFormat::CongeeCompact => {
                    self.congee_compact.as_ref().unwrap().contains(key)
                },
                FlatFormat::CongeeCompactV2 => {
                    self.congee_compact_v2.as_ref().unwrap().contains(key)
                },
            };
            
            op_count += 1;
            key_idx += 1;
        }
        
        op_count
    }

    fn cleanup(&mut self) -> Option<serde_json::Value> {
        None
    }
}

fn main() {
    let config = FlatComparison::load().expect("Failed to parse config!");
    let repeat = 3;

    println!("Config: {:?}", config);
    for c in config.iter() {
        println!("Current config: {:?}", c);
        for &dataset_size in &c.dataset_size {
            let mut test_bench = FlatTestBench::new(c.format, c.key_pattern, dataset_size);
            let result = shumai::run(&mut test_bench, c, repeat);
            result.write_json().unwrap();
        }
    }
}