use congee::Art;
use rand::{thread_rng, Rng};
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
    index: Art,
    initial_cnt: usize,
}

impl ShumaiBench for TestBench {
    type Result = usize;
    type Config = Scan;

    fn load(&mut self) -> Option<serde_json::Value> {
        let guard = self.index.pin();
        for i in 0..self.initial_cnt {
            self.index.insert(i, i, &guard);
        }

        None
    }

    fn run(&self, context: shumai::Context<Self::Config>) -> Self::Result {
        let mut op_cnt = 0;
        let max_scan_cnt = 50;
        let mut scan_buffer = vec![0; 50];
        context.wait_for_start();

        let mut rng = thread_rng();
        let guard = self.index.pin();
        while context.is_running() {
            let scan_cnt = max_scan_cnt;
            let low_key_v = rng.gen_range(0..(self.initial_cnt - scan_cnt));

            let low_key = low_key_v;
            let high_key = low_key_v + scan_cnt;

            let scanned = self
                .index
                .range(&low_key, &high_key, &mut scan_buffer, &guard)
                .unwrap_or(0);

            for v in scan_buffer.iter().take(scanned) {
                assert!(*v >= low_key_v);
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
            index: Art::new(),
            initial_cnt: 50_000_000,
        };
        let result = shumai::run(&mut test_bench, c, repeat);
        result.write_json().unwrap();
    }
}
