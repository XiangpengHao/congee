#!/usr/bin/env python3
"""
Benchmark Results Analyzer
Processes Shumai JSON benchmark results and generates markdown tables.

Usage: python analyze_results.py file1.json file2.json ...
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any

def load_benchmark_result(file_path: str) -> Dict[str, Any]:
    """Load and parse a benchmark JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def extract_metrics(result: Dict[str, Any]) -> Dict[str, Any]:
    """Extract key metrics from a benchmark result."""
    config = result['config']
    load_data = result['load']['user_metrics']
    
    # Calculate average operations from iterations
    iterations = result['run'][0]['iterations']
    operations = [iter_data['result'] for iter_data in iterations]
    avg_operations = sum(operations) / len(operations)
    
    # Extract configuration
    dataset_size = config['dataset_size'][0]
    test_time = config['time']
    
    # Calculate derived metrics
    throughput = avg_operations / test_time  # ops/sec
    latency_ns = (test_time * 1_000_000_000) / avg_operations  # ns/op
    
    return {
        'format': config['format'],
        'dataset_size': dataset_size,
        'key_pattern': config['key_pattern'],
        'test_time': test_time,
        'threads': config['threads'][0],
        'memory_bytes': load_data['memory_bytes'],
        'bytes_per_key': load_data['bytes_per_key'],
        'avg_operations': int(avg_operations),
        'throughput_ops_per_sec': throughput,
        'latency_ns': latency_ns,
        'iterations': len(iterations)
    }

def format_number(num: float, suffix: str = "") -> str:
    """Format numbers with appropriate units."""
    if suffix == "ops/sec":
        if num >= 1_000_000:
            return f"{num/1_000_000:.2f}M"
        elif num >= 1_000:
            return f"{num/1_000:.2f}K"
        else:
            return f"{num:.0f}"
    elif suffix == "bytes":
        return f"{int(num):,}"
    else:
        return f"{num:.2f}"

def generate_markdown_table(metrics_list: List[Dict[str, Any]]) -> str:
    """Generate a markdown table from metrics."""
    if not metrics_list:
        return "No data to display"
    
    # Sort by format name for consistent ordering
    metrics_list.sort(key=lambda x: x['format'])
    
    # Get baseline (CongeeSet) for relative comparisons
    baseline = next((m for m in metrics_list if m['format'] == 'CongeeSet'), None)
    
    # Build configuration section
    first_metric = metrics_list[0]
    config_md = f"""## **Benchmark Configuration**

- **Dataset Size**: {first_metric['dataset_size']:,} keys
- **Key Pattern**: {first_metric['key_pattern']}
- **Duration**: {first_metric['test_time']} seconds per test
- **Threads**: {first_metric['threads']}
- **Iterations**: {first_metric['iterations']} repetitions each

## **Performance Comparison Results**

"""
    
    # Build table header
    formats = [m['format'] for m in metrics_list]
    header = "| Metric | " + " | ".join(formats) + " |\n"
    separator = "|" + "--------|" * (len(formats) + 1) + "\n"
    
    # Build table rows
    rows = []
    
    # Memory metrics
    memory_row = "| **Memory (bytes)** |"
    bytes_per_key_row = "| **Bytes per key** |"
    for m in metrics_list:
        memory_row += f" {format_number(m['memory_bytes'], 'bytes')} |"
        bytes_per_key_row += f" {m['bytes_per_key']:.2f} |"
    
    # Performance metrics  
    ops_row = "| **Avg Operations** |"
    throughput_row = "| **Throughput (ops/sec)** |"
    latency_row = "| **Latency (ns/op)** |"
    for m in metrics_list:
        ops_row += f" {m['avg_operations']:,} |"
        throughput_row += f" {format_number(m['throughput_ops_per_sec'], 'ops/sec')} |"
        latency_row += f" {m['latency_ns']:.1f} |"
    
    # Relative metrics (if baseline exists)
    if baseline:
        memory_eff_row = "| **Memory efficiency vs CongeeSet** |"
        perf_rel_row = "| **Performance vs CongeeSet** |"
        
        for m in metrics_list:
            if m['format'] == 'CongeeSet':
                memory_eff_row += " 1.0x |"
                perf_rel_row += " 1.0x |"
            else:
                mem_ratio = baseline['memory_bytes'] / m['memory_bytes']
                perf_ratio = m['throughput_ops_per_sec'] / baseline['throughput_ops_per_sec']
                
                if mem_ratio > 1:
                    memory_eff_row += f" **{mem_ratio:.1f}x smaller** |"
                else:
                    memory_eff_row += f" {mem_ratio:.1f}x |"
                
                if perf_ratio < 1:
                    slowdown = 1 / perf_ratio
                    perf_rel_row += f" {perf_ratio:.2f}x ({slowdown:.1f}x slower) |"
                else:
                    perf_rel_row += f" {perf_ratio:.2f}x |"
        
        rows = [memory_row, bytes_per_key_row, ops_row, throughput_row, latency_row, memory_eff_row, perf_rel_row]
    else:
        rows = [memory_row, bytes_per_key_row, ops_row, throughput_row, latency_row]
    
    table_md = header + separator + "\n".join(rows) + "\n"
    
    return config_md + table_md

def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_results.py file1.json file2.json ...")
        sys.exit(1)
    
    metrics_list = []
    
    for file_path in sys.argv[1:]:
        try:
            result = load_benchmark_result(file_path)
            metrics = extract_metrics(result)
            metrics_list.append(metrics)
            print(f"✓ Processed: {Path(file_path).name}", file=sys.stderr)
        except Exception as e:
            print(f"✗ Error processing {file_path}: {e}", file=sys.stderr)
            continue
    
    if not metrics_list:
        print("No valid benchmark files processed", file=sys.stderr)
        sys.exit(1)
    
    # Generate and print markdown table
    markdown_output = generate_markdown_table(metrics_list)
    print(markdown_output)

if __name__ == "__main__":
    main()