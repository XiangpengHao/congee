use con_art_rust::{tree::Tree, Key, UsizeKey};
use rand::{thread_rng, Rng};
use shumai::{bench_config, ShumaiBench};

#[bench_config]
pub mod test_config {
    use serde::Serialize;
    use shumai::ShumaiConfig;

    #[derive(ShumaiConfig, Serialize, Clone, Debug)]
    pub struct Scan {
        pub name: String,
        pub threads: Vec<usize>,
        pub time: usize,
    }
}

struct TestBench {
    index: Tree<UsizeKey>,
    initial_cnt: usize,
}

impl ShumaiBench for TestBench {
    type Result = usize;
    type Config = Scan;

    fn load(&self) -> Option<serde_json::Value> {
        let guard = self.index.pin();
        for i in 0..self.initial_cnt {
            self.index.insert(UsizeKey::key_from(i), i, &guard);
        }

        None
    }

    fn run(&self, context: shumai::Context<Self::Config>) -> Self::Result {
        let mut op_cnt = 0;
        let max_scan_cnt = 10;
        let mut scan_buffer = Vec::with_capacity(max_scan_cnt);
        for _i in 0..max_scan_cnt {
            scan_buffer.push(0)
        }
        context.wait_for_start();

        let mut rng = thread_rng();
        while context.is_running() {
            let scan_cnt = rng.gen_range(1..max_scan_cnt);
            let low_key_v = rng.gen_range(0..(self.initial_cnt - scan_cnt));

            let low_key = UsizeKey::key_from(low_key_v);
            let high_key = UsizeKey::key_from(low_key_v + scan_cnt);

            let scanned = self
                .index
                .look_up_range(&low_key, &high_key, &mut scan_buffer)
                .unwrap_or(0);

            for v in scan_buffer.iter().take(scanned) {
                assert!(*v >= low_key_v);
            }
            op_cnt += 1;
        }
        op_cnt
    }

    fn cleanup(&self) -> Option<serde_json::Value> {
        None
    }
}

fn main() {
    let config = Scan::load_config("bench/benchmark.toml").expect("Failed to parse config!");
    let repeat = 3;

    for c in config.iter() {
        let test_bench = TestBench {
            index: Tree::new(),
            initial_cnt: 50_000_000,
        };
        let result = shumai::run(&test_bench, c, repeat);
        result.write_json().unwrap();
    }
}
