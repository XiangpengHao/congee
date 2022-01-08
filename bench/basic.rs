use con_art_rust::{tree::Tree, Key};
use rand::{thread_rng, Rng};
use shumai::{bench_config, ShumaiBench};

#[bench_config]
pub mod test_config {
    use serde::{Deserialize, Serialize};
    use shumai::ShumaiConfig;
    use std::fmt::Display;

    #[derive(Serialize, Clone, Copy, Debug, Deserialize)]
    pub enum Workload {
        ReadOnly,
        InsertOnly,
        ScanOnly,
    }

    impl Display for Workload {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{:?}", self)
        }
    }

    #[derive(Serialize, Clone, Copy, Debug, Deserialize)]
    pub enum IndexType {
        Flurry,
        ART,
    }

    impl Display for IndexType {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{:?}", self)
        }
    }

    #[derive(ShumaiConfig, Serialize, Clone, Debug)]
    pub struct Basic {
        pub name: String,
        pub threads: Vec<usize>,
        pub time: usize,
        #[matrix]
        pub workload: Workload,
        #[matrix]
        pub index_type: IndexType,
    }
}

struct TestBench<Index: DBIndex> {
    index: Index,
    initial_cnt: usize,
}

trait DBIndex: Send + Sync {
    fn insert(&self, key: usize, v: usize);
    fn get(&self, key: &usize) -> Option<usize>;
}

impl DBIndex for Tree<usize> {
    fn insert(&self, key: usize, v: usize) {
        let guard = self.pin();
        self.insert(key, v, &guard);
    }

    fn get(&self, key: &usize) -> Option<usize> {
        let guard = self.pin();
        self.get(key, &guard)
    }
}

impl DBIndex for flurry::HashMap<usize, usize> {
    fn insert(&self, key: usize, v: usize) {
        self.pin().insert(key, v);
    }

    fn get(&self, key: &usize) -> Option<usize> {
        self.pin().get(key).map(|v| *v)
    }
}

impl<Index: DBIndex> ShumaiBench for TestBench<Index> {
    type Config = Basic;
    type Result = usize;

    fn load(&self) -> Option<serde_json::Value> {
        for i in 0..self.initial_cnt {
            self.index.insert(usize::key_from(i), i);
        }
        None
    }

    fn run(&self, context: shumai::Context<Self::Config>) -> Self::Result {
        let mut op_cnt = 0;
        context.wait_for_start();

        let mut rng = thread_rng();
        while context.is_running() {
            match context.config.workload {
                test_config::Workload::ReadOnly => {
                    let val = rng.gen_range(0..self.initial_cnt);
                    let r = self.index.get(&usize::key_from(val)).unwrap();
                    assert_eq!(r, val);
                }
                test_config::Workload::InsertOnly => {
                    let val = rng.gen();
                    self.index.insert(usize::key_from(val), val);
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
        match c.index_type {
            test_config::IndexType::Flurry => {
                let test_bench = TestBench {
                    index: Tree::new(),
                    initial_cnt: 50_000_000,
                };
                let result = shumai::run(&test_bench, c, repeat);
                result.write_json().unwrap();
            }
            test_config::IndexType::ART => {
                let test_bench = TestBench {
                    index: flurry::HashMap::new(),
                    initial_cnt: 50_000_000,
                };
                let result = shumai::run(&test_bench, c, repeat);
                result.write_json().unwrap();
            }
        }
    }
}
