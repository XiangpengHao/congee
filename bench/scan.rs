use std::collections::HashSet;

use congee::Congee;
use rand::{prelude::Distribution, thread_rng, Rng};
use shumai::{config, ShumaiBench};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[config(path = "bench/benchmark.toml")]
pub struct Scan {
    pub name: String,
    pub threads: Vec<usize>,
    pub time: usize,
}

struct TestBench {
    index: Congee<usize, usize>,
    initial_cnt: usize,
}

impl ShumaiBench for TestBench {
    type Result = usize;
    type Config = Scan;

    fn load(&mut self) -> Option<serde_json::Value> {
        let guard = self.index.pin();
        let mut unique_values = HashSet::new();
        let dist = selfsimilar::SelfSimilarDistribution::new(0, self.initial_cnt * 10, 0.4);
        let mut rng = thread_rng();
        for i in 0..self.initial_cnt {
            let k = dist.sample(&mut rng);
            self.index.insert(k, i, &guard).unwrap();
            unique_values.insert(k);
        }

        #[cfg(feature = "stats")]
        println!("{}", self.index.stats());

        Some(serde_json::json!({
            "unique_values": unique_values.len(),
        }))
    }

    fn run(&self, context: shumai::Context<Self::Config>) -> Self::Result {
        let mut op_cnt = 0;
        let max_scan_cnt = 50;
        let mut scan_buffer = vec![(0, 0); max_scan_cnt];
        context.wait_for_start();

        let mut rng = thread_rng();
        let guard = self.index.pin();
        while context.is_running() {
            let scan_cnt = rng.gen_range(0..max_scan_cnt);
            let low_key_v = rng.gen_range(0..(self.initial_cnt - scan_cnt * 10));
            let high_key = low_key_v + scan_cnt * 10;

            let scanned = self
                .index
                .range(&low_key_v, &high_key, &mut scan_buffer, &guard);

            for v in scan_buffer.iter().take(scanned) {
                assert!(v.0 >= low_key_v);
            }
            op_cnt += 1;
        }
        op_cnt
    }

    fn cleanup(&mut self) -> Option<serde_json::Value> {
        None
    }
}

fn main() {
    let config = Scan::load().expect("Failed to parse config!");
    let repeat = 3;

    for c in config.iter() {
        let mut test_bench = TestBench {
            index: Congee::default(),
            initial_cnt: 50_000_000,
        };
        let result = shumai::run(&mut test_bench, c, repeat);
        result.write_json().unwrap();
    }
}
