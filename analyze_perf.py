#!/usr/bin/env python3
import json

# Read the old benchmark results
with open('target/benchmark/2025-08-01/20-15-flatcomparison-compact-CongeeCompactV2.json') as f:
    compact_v2_old = json.load(f)

# Read the new (optimized) benchmark results
with open('target/benchmark/2025-08-01/20-21-flatcomparison-compact-CongeeCompactV2.json') as f:
    compact_v2_new = json.load(f)

with open('target/benchmark/2025-08-01/20-21-flatcomparison-compact-CongeeCompact.json') as f:
    compact = json.load(f)

with open('target/benchmark/2025-08-01/20-21-flatcomparison-compact-CongeeSet.json') as f:
    congee_set = json.load(f)

def analyze_iteration(data, iteration_idx=1):  # Use middle iteration
    iter_data = data['run'][0]['iterations'][iteration_idx]
    ops = iter_data['result']
    perf = iter_data['measurements'][1]['value']  # perf measurements
    
    print(f"Format: {data['config']['format']}")
    print(f"Operations: {ops:,}")
    print(f"Cache miss rate: {perf['cache_miss'] / perf['cache_reference'] * 100:.1f}%")
    print(f"Branch miss rate: {perf['branch_miss'] / perf['branches'] * 100:.1f}%")
    print(f"Instructions per op: {perf['inst'] / ops:.1f}")
    print(f"Cycles per op: {perf['cycles'] / ops:.1f}")
    print(f"Cache misses per op: {perf['cache_miss'] / ops:.2f}")
    print(f"Branch misses per op: {perf['branch_miss'] / ops:.2f}")
    print(f"Memory bytes: {data['load']['user_metrics']['memory_bytes']:,}")
    print(f"Bytes per key: {data['load']['user_metrics']['bytes_per_key']:.1f}")
    print("-" * 50)

print("Performance Analysis - Before and After Inlining")
print("=" * 60)
print("BEFORE INLINING:")
analyze_iteration(compact_v2_old)
print("AFTER INLINING:")
analyze_iteration(compact_v2_new)
print("COMPARISON BASELINES:")
analyze_iteration(compact)
analyze_iteration(congee_set)

# Calculate improvement
old_ops = compact_v2_old['run'][0]['iterations'][1]['result']
new_ops = compact_v2_new['run'][0]['iterations'][1]['result']
improvement = new_ops / old_ops

print(f"\n*** INLINING IMPROVEMENT ***")
print(f"Old CongeeCompactV2: {old_ops:,} ops/sec")
print(f"New CongeeCompactV2: {new_ops:,} ops/sec")
print(f"Improvement: {improvement:.2f}x faster ({(improvement-1)*100:.1f}% increase)")

# Compare to baselines
compact_ops = compact['run'][0]['iterations'][1]['result']
set_ops = congee_set['run'][0]['iterations'][1]['result']

print(f"\nVs CongeeCompact: {new_ops / compact_ops:.2f}x ({new_ops/compact_ops*100:.1f}% of CongeeCompact speed)")
print(f"Vs CongeeSet: {new_ops / set_ops:.2f}x ({new_ops/set_ops*100:.1f}% of CongeeSet speed)")