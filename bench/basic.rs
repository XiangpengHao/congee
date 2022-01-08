use con_art_rust::{tree::Tree, Key};
use rand::{thread_rng, Rng};
use shumai::{bench_config, ShumaiBench};

#[bench_config]
pub mod test_config {
    use serde::{Deserialize, Serialize};
    use shumai::ShumaiConfig;

    #[derive(Serialize, Clone, Debug, Deserialize)]
    pub enum Workload {
        ReadOnly,
        InsertOnly,
        ScanOnly,
    }

    #[derive(ShumaiConfig, Serialize, Clone, Debug)]
    pub struct Basic {
        pub name: String,
        pub threads: Vec<usize>,
        pub time: usize,
        pub workload: Workload,
    }
}

struct TestBench {
    tree: Tree,
    initial_cnt: usize,
}

impl ShumaiBench for TestBench {
    type Config = Basic;
    type Result = usize;

    fn load(&self) -> Option<serde_json::Value> {
        let guard = self.tree.pin();
        for i in 0..self.initial_cnt {
            self.tree.insert(Key::from(i), i, &guard);
        }
        None
    }

    fn run(&self, context: shumai::Context<Self::Config>) -> Self::Result {
        let mut op_cnt = 0;
        context.wait_for_start();

        let mut rng = thread_rng();
        let guard = crossbeam_epoch::pin();
        while context.is_running() {
            match context.config.workload {
                test_config::Workload::ReadOnly => {
                    let val = rng.gen_range(0..self.initial_cnt);
                    let r = self.tree.get(&Key::from(val), &guard).unwrap();
                    assert_eq!(r, val);
                }
                test_config::Workload::InsertOnly => {
                    // unimplemented!()
                }
                test_config::Workload::ScanOnly => {
                    unimplemented!()
                }
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
    let config = Basic::load_config("bench/benchmark.toml").expect("Failed to parse config!");
    let repeat = 3;

    for c in config.iter() {
        println!("config: {:#?}", c);
        let test_bench = TestBench {
            tree: Tree::new(),
            initial_cnt: 50_000_000,
        };
        let result = shumai::run(&test_bench, c, repeat);
        result.to_json();
    }
}
