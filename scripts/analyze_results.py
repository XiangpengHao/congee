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

def extract_metrics(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract key metrics from a benchmark result for all thread configurations."""
    config = result['config']
    load_data = result['load']['user_metrics']
    
    # Extract configuration common to all thread runs
    dataset_size = config['dataset_size'][0]
    test_time = config['time']
    
    metrics_list = []
    
    # Process each thread configuration
    for run_data in result['run']:
        thread_count = run_data['thread_cnt']
        iterations = run_data['iterations']
        
        # Calculate average operations from iterations
        operations = [iter_data['result'] for iter_data in iterations]
        avg_operations = sum(operations) / len(operations)
        
        # Calculate derived metrics
        throughput = avg_operations / test_time  # ops/sec
        latency_ns = (test_time * 1_000_000_000) / avg_operations  # ns/op
        
        # Extract perf stats from measurements (average across iterations)
        perf_stats = {}
        disk_io_stats = {}
        
        for iteration in iterations:
            if 'measurements' in iteration:
                for measurement in iteration['measurements']:
                    if measurement['name'] == 'perf' and 'value' in measurement:
                        perf_data = measurement['value']
                        for key, value in perf_data.items():
                            if key not in perf_stats:
                                perf_stats[key] = []
                            perf_stats[key].append(value)
                    elif measurement['name'] == 'disk_io' and 'value' in measurement:
                        disk_data = measurement['value']
                        for key, value in disk_data.items():
                            if key not in disk_io_stats:
                                disk_io_stats[key] = []
                            disk_io_stats[key].append(value)
        
        # Calculate averages for perf stats
        avg_perf_stats = {}
        for key, values in perf_stats.items():
            if values:  # Only include if we have data
                avg_perf_stats[key] = sum(values) / len(values)
        
        # Calculate averages for disk I/O stats
        avg_disk_io_stats = {}
        for key, values in disk_io_stats.items():
            if values:  # Only include if we have data
                avg_disk_io_stats[key] = sum(values) / len(values)
        
        metrics = {
            'format': config['format'],
            'dataset_size': dataset_size,
            'key_pattern': config['key_pattern'],
            'test_time': test_time,
            'threads': thread_count,
            'memory_bytes': load_data['memory_bytes'],
            'bytes_per_key': load_data['bytes_per_key'],
            'avg_operations': int(avg_operations),
            'throughput_ops_per_sec': throughput,
            'latency_ns': latency_ns,
            'iterations': len(iterations),
            'perf_stats': avg_perf_stats,
            'disk_io_stats': avg_disk_io_stats
        }
        
        metrics_list.append(metrics)
    
    return metrics_list

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
    elif suffix == "large":
        if num >= 1_000_000_000:
            return f"{num/1_000_000_000:.2f}B"
        elif num >= 1_000_000:
            return f"{num/1_000_000:.2f}M"
        elif num >= 1_000:
            return f"{num/1_000:.2f}K"
        else:
            return f"{num:.0f}"
    else:
        return f"{num:.2f}"

def generate_markdown_table(metrics_by_thread: Dict[int, List[Dict[str, Any]]]) -> str:
    """Generate markdown tables from metrics organized by thread count."""
    if not metrics_by_thread:
        return "No data to display"
    
    # Extract system configuration from the first result
    first_result_file = None
    for file_path in sys.argv[1:]:
        try:
            with open(file_path, 'r') as f:
                first_result_file = json.load(f)
                break
        except:
            continue
    
    # Build system configuration section
    system_config_md = ""
    if first_result_file and 'env' in first_result_file:
        env = first_result_file['env']
        system_config_md = f"""## System Configuration

- **Hostname**: {env.get('hostname', 'N/A')}
- **Operating System**: {env.get('os_version', 'N/A')}
- **Kernel Version**: {env.get('kernel_version', 'N/A')}
- **CPU Cores**: {env.get('cpu_num', 'N/A')} logical cores
- **Physical Cores**: {env.get('physical_core_num', 'N/A')} physical cores
- **Total Memory**: {format_number(env.get('total_memory', 0), 'bytes')} ({env.get('total_memory', 0) / (1024**3):.1f} GB)

"""
    
    # Build benchmark configuration section
    first_metrics_list = next(iter(metrics_by_thread.values()))
    first_metric = first_metrics_list[0]
    thread_counts = sorted(metrics_by_thread.keys())
    config_md = f"""## Benchmark Configuration

- **Dataset Size**: {first_metric['dataset_size']:,} keys
- **Key Pattern**: {first_metric['key_pattern']}
- **Duration**: {first_metric['test_time']} seconds per test
- **Thread Counts**: {', '.join(map(str, thread_counts))}
- **Iterations**: {first_metric['iterations']} repetitions each

### Performance Comparison

"""
    
    # Generate tables for each thread count
    all_tables = []
    
    for thread_count in sorted(metrics_by_thread.keys()):
        metrics_list = metrics_by_thread[thread_count]
        
        # Sort by format name for consistent ordering
        metrics_list.sort(key=lambda x: x['format'])
        
        # Get baseline (CongeeSet) for relative comparisons
        baseline = next((m for m in metrics_list if m['format'] == 'CongeeSet'), None)
        
        # Thread-specific header
        thread_header = f"### {thread_count} Thread{'s' if thread_count > 1 else ''}\n\n"
        
        # Build table header
        formats = [m['format'] for m in metrics_list]
        header = "| Metric | " + " | ".join(formats) + " |\n"
        separator = "|" + "--------|" * (len(formats) + 1) + "\n"
        
        # Build table rows
        rows = []
        
        # Memory metrics (only show for single thread to avoid repetition)
        if thread_count == sorted(metrics_by_thread.keys())[0]:
            memory_row = "| **Memory (bytes)** |"
            bytes_per_key_row = "| **Bytes per key** |"
            
            # Find best values for memory metrics (lowest is best)
            min_memory = min(m['memory_bytes'] for m in metrics_list)
            min_bytes_per_key = min(m['bytes_per_key'] for m in metrics_list)
            
            for m in metrics_list:
                memory_val = format_number(m['memory_bytes'], 'bytes')
                if m['memory_bytes'] == min_memory:
                    memory_val = f"**{memory_val}**"
                memory_row += f" {memory_val} |"
                
                bytes_val = f"{m['bytes_per_key']:.2f}"
                if m['bytes_per_key'] == min_bytes_per_key:
                    bytes_val = f"**{bytes_val}**"
                bytes_per_key_row += f" {bytes_val} |"
            
            rows.extend([memory_row, bytes_per_key_row])
        
        # Performance metrics
        ops_row = "| **Throughput** |"
        latency_row = "| **Latency (ns/op)** |"
        
        # Find best values for performance metrics
        max_operations = max(m['avg_operations'] for m in metrics_list)
        min_latency = min(m['latency_ns'] for m in metrics_list)
        
        for m in metrics_list:
            ops_val = f"{format_number(m['avg_operations'], 'ops/sec')}"
            if m['avg_operations'] == max_operations:
                ops_val = f"**{ops_val}**"
            ops_row += f" {ops_val} |"
            
            latency_val = f"{m['latency_ns']:.1f}"
            if m['latency_ns'] == min_latency:
                latency_val = f"**{latency_val}**"
            latency_row += f" {latency_val} |"
        
        rows.extend([ops_row, latency_row])
        
        # Add perf stats if available
        if any('perf_stats' in m and m['perf_stats'] for m in metrics_list):
            # Collect all unique perf stat keys
            all_perf_keys = set()
            for m in metrics_list:
                if 'perf_stats' in m:
                    all_perf_keys.update(m['perf_stats'].keys())
            
            # Add section header for perf stats
            rows.append("| **--- CPU Performance Stats ---** |" + " |" * len(formats))
            
            # Custom ordering for cache-related metrics
            perf_keys_ordered = []
            cache_keys = {'cache_reference', 'cache_miss'}
            branch_keys = {'branches', 'branch_miss'}
            
            # Add non-cache, non-branch keys first
            for perf_key in sorted(all_perf_keys):
                if perf_key not in cache_keys and perf_key not in branch_keys:
                    perf_keys_ordered.append(perf_key)
            
            # Add branch keys in specific order if they exist
            if 'branches' in all_perf_keys:
                perf_keys_ordered.append('branches')
            if 'branch_miss' in all_perf_keys:
                perf_keys_ordered.append('branch_miss')
            
            # Add cache keys in specific order if they exist
            if 'cache_reference' in all_perf_keys:
                perf_keys_ordered.append('cache_reference')
            if 'cache_miss' in all_perf_keys:
                perf_keys_ordered.append('cache_miss')
            
            # Add rows for each perf stat
            for perf_key in perf_keys_ordered:
                # Format the key name for display
                display_name = perf_key.replace('_', ' ').title()
                perf_row = f"| **{display_name}** |"
                
                # Determine best value for this metric (higher or lower is better)
                values = []
                for m in metrics_list:
                    if 'perf_stats' in m and perf_key in m['perf_stats']:
                        values.append(m['perf_stats'][perf_key])
                
                if values:
                    # For most metrics, lower is better (misses, stalls, migrations)
                    # For instructions and cycles, context depends on workload
                    if perf_key in ['branch_miss', 'cache_miss', 'context_switch', 'cpu_migration', 'page_faults', 'stalled_cycles_frontend']:
                        best_value = min(values)
                    elif perf_key in ['branches', 'cache_reference', 'cycles', 'inst']:
                        # For these, it depends on context - higher could mean more work done
                        # We'll highlight the highest for now
                        best_value = max(values)
                    else:
                        best_value = min(values)  # Default to lower is better
                else:
                    best_value = None
                
                for m in metrics_list:
                    if 'perf_stats' in m and perf_key in m['perf_stats']:
                        value = m['perf_stats'][perf_key]
                        formatted_val = format_number(value, 'large')
                        if best_value is not None and value == best_value:
                            formatted_val = f"**{formatted_val}**"
                        perf_row += f" {formatted_val} |"
                    else:
                        perf_row += " - |"
                rows.append(perf_row)
                
                # Add IPC row right after Instructions row
                if perf_key == 'inst':
                    ipc_row = "| **Instructions Per Cycle (IPC)** |"
                    
                    # For IPC, higher is better
                    ipc_values = []
                    for m in metrics_list:
                        if ('perf_stats' in m and 'cycles' in m['perf_stats'] and 
                            'inst' in m['perf_stats'] and m['perf_stats']['cycles'] > 0):
                            cycles = m['perf_stats']['cycles']
                            instructions = m['perf_stats']['inst']
                            ipc = instructions / cycles
                            ipc_values.append(ipc)
                        else:
                            ipc_values.append(None)
                    
                    max_ipc = max(ipc for ipc in ipc_values if ipc is not None) if any(ipc is not None for ipc in ipc_values) else None
                    
                    for i, m in enumerate(metrics_list):
                        if ('perf_stats' in m and 'cycles' in m['perf_stats'] and 
                            'inst' in m['perf_stats'] and m['perf_stats']['cycles'] > 0):
                            cycles = m['perf_stats']['cycles']
                            instructions = m['perf_stats']['inst']
                            ipc = instructions / cycles
                            ipc_val = f"{ipc:.2f}"
                            if max_ipc is not None and abs(ipc - max_ipc) < 1e-6:  # Handle floating point comparison
                                ipc_val = f"**{ipc_val}**"
                            ipc_row += f" {ipc_val} |"
                        else:
                            ipc_row += " - |"
                    rows.append(ipc_row)
                
                # Add Cache Miss Rate row right after Cache Miss row
                if perf_key == 'cache_miss':
                    miss_rate_row = "| **Cache Miss Rate (%)** |"
                    
                    # For miss rate, lower is better
                    miss_rates = []
                    for m in metrics_list:
                        if ('perf_stats' in m and 'cache_miss' in m['perf_stats'] and 
                            'cache_reference' in m['perf_stats'] and m['perf_stats']['cache_reference'] > 0):
                            cache_miss = m['perf_stats']['cache_miss']
                            cache_ref = m['perf_stats']['cache_reference']
                            miss_rate = (cache_miss / cache_ref) * 100
                            miss_rates.append(miss_rate)
                        else:
                            miss_rates.append(None)
                    
                    min_miss_rate = min(rate for rate in miss_rates if rate is not None) if any(rate is not None for rate in miss_rates) else None
                    
                    for i, m in enumerate(metrics_list):
                        if ('perf_stats' in m and 'cache_miss' in m['perf_stats'] and 
                            'cache_reference' in m['perf_stats'] and m['perf_stats']['cache_reference'] > 0):
                            cache_miss = m['perf_stats']['cache_miss']
                            cache_ref = m['perf_stats']['cache_reference']
                            miss_rate = (cache_miss / cache_ref) * 100
                            rate_val = f"{miss_rate:.1f}%"
                            if min_miss_rate is not None and abs(miss_rate - min_miss_rate) < 1e-6:
                                rate_val = f"**{rate_val}**"
                            miss_rate_row += f" {rate_val} |"
                        else:
                            miss_rate_row += " - |"
                    rows.append(miss_rate_row)
                
                # Add Branch Miss Rate row right after Branch Miss row
                if perf_key == 'branch_miss':
                    branch_miss_rate_row = "| **Branch Miss Rate (%)** |"
                    
                    # For branch miss rate, lower is better
                    branch_miss_rates = []
                    for m in metrics_list:
                        if ('perf_stats' in m and 'branch_miss' in m['perf_stats'] and 
                            'branches' in m['perf_stats'] and m['perf_stats']['branches'] > 0):
                            branch_miss = m['perf_stats']['branch_miss']
                            branches = m['perf_stats']['branches']
                            miss_rate = (branch_miss / branches) * 100
                            branch_miss_rates.append(miss_rate)
                        else:
                            branch_miss_rates.append(None)
                    
                    min_branch_miss_rate = min(rate for rate in branch_miss_rates if rate is not None) if any(rate is not None for rate in branch_miss_rates) else None
                    
                    for i, m in enumerate(metrics_list):
                        if ('perf_stats' in m and 'branch_miss' in m['perf_stats'] and 
                            'branches' in m['perf_stats'] and m['perf_stats']['branches'] > 0):
                            branch_miss = m['perf_stats']['branch_miss']
                            branches = m['perf_stats']['branches']
                            miss_rate = (branch_miss / branches) * 100
                            rate_val = f"{miss_rate:.1f}%"
                            if min_branch_miss_rate is not None and abs(miss_rate - min_branch_miss_rate) < 1e-6:
                                rate_val = f"**{rate_val}**"
                            branch_miss_rate_row += f" {rate_val} |"
                        else:
                            branch_miss_rate_row += " - |"
                    rows.append(branch_miss_rate_row)
        
        # Add disk I/O stats if available
        if any('disk_io_stats' in m and m['disk_io_stats'] for m in metrics_list):
            # Collect all unique disk I/O stat keys
            all_disk_keys = set()
            for m in metrics_list:
                if 'disk_io_stats' in m:
                    all_disk_keys.update(m['disk_io_stats'].keys())
            
            # Add section header for disk I/O stats
            rows.append("| **--- Disk I/O Stats ---** |" + " |" * len(formats))
            
            # Add rows for each disk I/O stat
            for disk_key in sorted(all_disk_keys):
                # Format the key name for display
                display_name = disk_key.replace('_', ' ').title()
                disk_row = f"| **{display_name}** |"
                
                # For disk I/O, lower is typically better
                values = []
                for m in metrics_list:
                    if 'disk_io_stats' in m and disk_key in m['disk_io_stats']:
                        values.append(m['disk_io_stats'][disk_key])
                
                best_value = min(values) if values else None
                
                for m in metrics_list:
                    if 'disk_io_stats' in m and disk_key in m['disk_io_stats']:
                        value = m['disk_io_stats'][disk_key]
                        formatted_val = format_number(value, 'bytes')
                        if best_value is not None and value == best_value:
                            formatted_val = f"**{formatted_val}**"
                        disk_row += f" {formatted_val} |"
                    else:
                        disk_row += " - |"
                rows.append(disk_row)
        
        # Relative metrics (if baseline exists)
        if baseline:
            rows.append("| **--- Relative Comparisons ---** |" + " |" * len(formats))
            
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
                        memory_eff_row += f" **{mem_ratio:.1f}x** |"
                    
                    if perf_ratio < 1:
                        slowdown = 1 / perf_ratio
                        perf_rel_row += f" **{perf_ratio:.2f}x ({slowdown:.1f}x slower)** |"
                    else:
                        perf_rel_row += f" **{perf_ratio:.2f}x** |"
            
            rows.extend([memory_eff_row, perf_rel_row])
        
        # Create the table for this thread count
        table_md = thread_header + header + separator + "\n".join(rows) + "\n\n"
        all_tables.append(table_md)
    
    # Combine all tables
    return system_config_md + config_md + "".join(all_tables)

def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_results.py file1.json file2.json ...")
        sys.exit(1)
    
    all_metrics = []
    
    for file_path in sys.argv[1:]:
        try:
            result = load_benchmark_result(file_path)
            metrics_list = extract_metrics(result)  # Now returns a list
            all_metrics.extend(metrics_list)
            # print(f"✓ Processed: {Path(file_path).name}", file=sys.stderr)
        except Exception as e:
            # print(f"✗ Error processing {file_path}: {e}", file=sys.stderr)
            continue
    
    if not all_metrics:
        print("No valid benchmark files processed", file=sys.stderr)
        sys.exit(1)
    
    # Organize metrics by thread count
    metrics_by_thread = {}
    for metric in all_metrics:
        thread_count = metric['threads']
        if thread_count not in metrics_by_thread:
            metrics_by_thread[thread_count] = []
        metrics_by_thread[thread_count].append(metric)
    
    # Generate and print markdown table
    markdown_output = generate_markdown_table(metrics_by_thread)
    print(markdown_output)

if __name__ == "__main__":
    main()