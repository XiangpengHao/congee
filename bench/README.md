## Benchmark Guide

### Multi-thread performance of Congee 

```bash
env SHUMAI_FILTER="ReadOnly-ART" RUSTFLAGS='-C target-cpu=native' cargo bench --bench basic
```


### Single thread Congee vs std::HashMap

We internally use `AHashMap` as it performs much better than `std::HashMap`.

```bash
env SHUMAI_FILTER="single-thread" RUSTFLAGS='-C target-cpu=native' cargo bench --bench basic
```

Example output:
```
============================================================
Loading data...
finished in 1.95s
============================================================
Running benchmark for 3 seconds with 1 threads: basic-single-thread-ART
Iteration 0 finished------------------
11555348

Iteration 1 finished------------------
11300362

Iteration 2 finished------------------
11287117

Benchmark results saved to file: target/benchmark/2024-10-07/10-57-basic-single-thread-ART.json
============================================================
Loading data...
finished in 5.52s
============================================================
Running benchmark for 3 seconds with 1 threads: basic-single-thread-SingleHashMap
Iteration 0 finished------------------
14818107

Iteration 1 finished------------------
14582199

Iteration 2 finished------------------
14339184

Benchmark results saved to file: target/benchmark/2024-10-07/10-57-basic-single-thread-SingleHashMap.json
```

### Collect perf metrics

Simply add `--features "perf"` to the cargo bench command.

```bash
env SHUMAI_FILTER="single-thread" RUSTFLAGS='-C target-cpu=native' cargo bench --bench basic --features "perf"
```

Example output:
```json
{
	"name": "perf",
	"value": {
		"branch_miss": 41634853,
		"branches": 1213790237,
		"cache_miss": 344751417,
		"cache_reference": 621479438,
		"context_switch": 0,
		"cpu_migration": 0,
		"cycles": 32342888548,
		"inst": 11316270133,
		"page_faults": 0,
		"stalled_cycles_frontend": 376732948
	}
}
```
