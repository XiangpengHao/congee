use std::{collections::HashMap, fmt::Display, ptr::NonNull};

use crate::{
    Allocator,
    congee_inner::{CongeeInner, CongeeVisitor},
    nodes::{BaseNode, NodeType},
};

#[cfg_attr(feature = "stats", derive(serde::Serialize))]
#[derive(Default, Debug, Clone)]
pub struct NodeStats {
    levels: HashMap<usize, LevelStats>,
    kv_pairs: usize,
    /// Global prefix length distribution [length_0, length_1, ..., length_8]
    prefix_distribution: [usize; 9],
}

impl NodeStats {

    pub fn total_memory_bytes(&self) -> usize {
        self.levels.values().map(|l| l.memory_size()).sum()
    }

    pub fn total_nodes(&self) -> usize {
        self.levels.values().map(|l| l.node_count()).sum()
    }

    pub fn kv_pairs(&self) -> usize {
        self.kv_pairs
    }

    /// Memory breakdown by node type in bytes
    pub fn memory_by_node_type(&self) -> (usize, usize, usize, usize) {
        let n4_memory: usize = self.levels.values().map(|l| l.n4.node_count * 56).sum();
        let n16_memory: usize = self.levels.values().map(|l| l.n16.node_count * 160).sum();
        let n48_memory: usize = self.levels.values().map(|l| l.n48.node_count * 664).sum();
        let n256_memory: usize = self.levels.values().map(|l| l.n256.node_count * 2096).sum();
        (n4_memory, n16_memory, n48_memory, n256_memory)
    }

    /// Get global prefix length distribution
    pub fn prefix_distribution(&self) -> &[usize; 9] {
        &self.prefix_distribution
    }

    /// Get prefix length distribution for a specific level
    pub fn level_prefix_distribution(&self, level: usize) -> Option<&[usize; 9]> {
        self.levels.get(&level).map(|l| &l.prefix_distribution)
    }
}

impl Display for NodeStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn calc_load_factor(n: &NodeInfo, scale: usize) -> f64 {
            if n.node_count == 0 {
                return 0.0;
            }
            (n.value_count as f64) / (n.node_count as f64 * scale as f64)
        }

        fn format_memory(bytes: usize) -> String {
            if bytes < 1024 {
                format!("{} B", bytes)
            } else if bytes < 1024 * 1024 {
                format!("{:.1} KB", bytes as f64 / 1024.0)
            } else if bytes < 1024 * 1024 * 1024 {
                format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
            } else {
                format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
            }
        }

        let mut levels = self.levels.values().collect::<Vec<_>>();
        levels.sort_by_key(|l| l.level);

        let mut node_count = 0;
        let mut total_f = 0.0;
        let mut memory_size = 0;
        let mut value_count = 0;

        writeln!(f, "╭──────────────────────────────────────────────────────────────────────────────────────────────────────────────╮")?;
        writeln!(f, "│                                            Congee Statistics                                                 │")?;
        writeln!(f, "├──────────────────────────────────────────────────────────────────────────────────────────────────────────────┤")?;

        for l in levels.iter() {
            total_f += l.n4.value_count as f64 / 4.0;
            total_f += l.n16.value_count as f64 / 16.0;
            total_f += l.n48.value_count as f64 / 48.0;
            total_f += l.n256.value_count as f64 / 256.0;

            node_count += l.node_count();
            value_count += l.value_count();
            let level_memory = l.memory_size();
            memory_size += level_memory;

            let mem_str = format_memory(level_memory);
            writeln!(
                f,
                "│ L{:2} │ N4:{:7}({:4.1}) │ N16:{:7}({:4.1}) │ N48:{:7}({:4.1}) │ N256:{:7}({:4.1}) │     {:>8}           │",
                l.level,
                l.n4.node_count,
                calc_load_factor(&l.n4, 4),
                l.n16.node_count,
                calc_load_factor(&l.n16, 16),
                l.n48.node_count,
                calc_load_factor(&l.n48, 48),
                l.n256.node_count,
                calc_load_factor(&l.n256, 256),
                mem_str
            )?;
        }

        writeln!(f, "├──────────────────────────────────────────────────────────────────────────────────────────────────────────────┤")?;
        
        let total_mem_str = format_memory(memory_size);
        writeln!(f, "│ Total Memory: {:>10} │ Entries: {:>8} │ KV Pairs: {:>8}                                            │", 
            total_mem_str, value_count, self.kv_pairs)?;

        // Node count breakdown by type
        let n4_count: usize = levels.iter().map(|l| l.n4.node_count).sum();
        let n16_count: usize = levels.iter().map(|l| l.n16.node_count).sum();
        let n48_count: usize = levels.iter().map(|l| l.n48.node_count).sum();
        let n256_count: usize = levels.iter().map(|l| l.n256.node_count).sum();
        
        writeln!(f, "│ Node Counts:  {:>10} │ N4: {:>8} │ N16: {:>8} │ N48: {:>8} │ N256: {:>8}                     │", 
            node_count, n4_count, n16_count, n48_count, n256_count)?;

        let load_factor = if node_count > 0 { total_f / (node_count as f64) } else { 0.0 };
        let load_status = if load_factor < 0.5 && node_count > 0 { " (low)" } else { "" };
        let n4_mem = format_memory(n4_count * 56);
        let n16_mem = format_memory(n16_count * 160);
        let n48_mem = format_memory(n48_count * 664);
        let n256_mem = format_memory(n256_count * 2096);
        
        writeln!(f, "│ Load Factor: {:4.2}{:<6} │ N4: {:<8} │ N16: {:<8} │ N48: {:<8} │ N256: {:<8}                      │", 
            load_factor, load_status, n4_mem, n16_mem, n48_mem, n256_mem)?;
        
        writeln!(f, "├──────────────────────────────────────────────────────────────────────────────────────────────────────────────┤")?;
        writeln!(f, "│                                            Prefix Length Distribution                                        │")?;
        writeln!(f, "├──────────────────────────────────────────────────────────────────────────────────────────────────────────────┤")?;
        
        write!(f, "│ All │")?;
        for i in 0..=7 {
            write!(f, " {}:{:>7} │", i, self.prefix_distribution[i])?;
        }
        write!(f, " {}:{:>1}    │", 8, self.prefix_distribution[8])?;
        writeln!(f, "")?;
        
        // Per-level prefix distribution
        for l in levels.iter() {
            write!(f, "│ L{:>2} │", l.level)?;
            for i in 0..=7 {
                write!(f, " {}:{:>7} │", i, l.prefix_distribution[i])?;
            }
            write!(f, " {}:{:>1}    │", 8, l.prefix_distribution[8])?;
            writeln!(f, "")?;
        }

        writeln!(f, "╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────╯")?;

        Ok(())
    }
}

#[cfg_attr(feature = "stats", derive(serde::Serialize))]
#[derive(Debug, Clone, Default)]
struct NodeInfo {
    node_count: usize,
    value_count: usize,
}

#[cfg_attr(feature = "stats", derive(serde::Serialize))]
#[derive(Debug, Clone)]
pub struct LevelStats {
    level: usize,
    n4: NodeInfo, // (node count, leaf count)
    n16: NodeInfo,
    n48: NodeInfo,
    n256: NodeInfo,
    /// Prefix length distribution for this level [length_0, length_1, ..., length_8]
    prefix_distribution: [usize; 9],
}

impl LevelStats {
    fn new_level(level: usize) -> Self {
        Self {
            level,
            n4: NodeInfo::default(),
            n16: NodeInfo::default(),
            n48: NodeInfo::default(),
            n256: NodeInfo::default(),
            prefix_distribution: [0; 9],
        }
    }

    fn memory_size(&self) -> usize {
        self.n4.node_count * NodeType::N4.node_layout().size()
            + self.n16.node_count * NodeType::N16.node_layout().size()
            + self.n48.node_count * NodeType::N48.node_layout().size()
            + self.n256.node_count * NodeType::N256.node_layout().size()
    }

    fn node_count(&self) -> usize {
        self.n4.node_count + self.n16.node_count + self.n48.node_count + self.n256.node_count
    }

    fn value_count(&self) -> usize {
        self.n4.value_count + self.n16.value_count + self.n48.value_count + self.n256.value_count
    }
}

struct StatsVisitor {
    node_stats: NodeStats,
}

impl<const K_LEN: usize> CongeeVisitor<K_LEN> for StatsVisitor {
    fn pre_visit_sub_node(&mut self, node: NonNull<BaseNode>, tree_level: usize) {
        let node = BaseNode::read_lock(node).unwrap();

        self.node_stats
            .levels
            .entry(tree_level)
            .or_insert_with(|| LevelStats::new_level(tree_level));

        // Track prefix length
        let prefix_len = node.as_ref().prefix().len();
        if prefix_len <= 8 {
            // Update global prefix distribution
            self.node_stats.prefix_distribution[prefix_len] += 1;
            // Update level-specific prefix distribution
            self.node_stats
                .levels
                .get_mut(&tree_level)
                .unwrap()
                .prefix_distribution[prefix_len] += 1;
        }

        match node.as_ref().get_type() {
            crate::nodes::NodeType::N4 => {
                self.node_stats
                    .levels
                    .get_mut(&tree_level)
                    .unwrap()
                    .n4
                    .node_count += 1;
                self.node_stats
                    .levels
                    .get_mut(&tree_level)
                    .unwrap()
                    .n4
                    .value_count += node.as_ref().value_count();
            }
            crate::nodes::NodeType::N16 => {
                self.node_stats
                    .levels
                    .get_mut(&tree_level)
                    .unwrap()
                    .n16
                    .node_count += 1;
                self.node_stats
                    .levels
                    .get_mut(&tree_level)
                    .unwrap()
                    .n16
                    .value_count += node.as_ref().value_count();
            }
            crate::nodes::NodeType::N48 => {
                self.node_stats
                    .levels
                    .get_mut(&tree_level)
                    .unwrap()
                    .n48
                    .node_count += 1;
                self.node_stats
                    .levels
                    .get_mut(&tree_level)
                    .unwrap()
                    .n48
                    .value_count += node.as_ref().value_count();
            }
            crate::nodes::NodeType::N256 => {
                self.node_stats
                    .levels
                    .get_mut(&tree_level)
                    .unwrap()
                    .n256
                    .node_count += 1;
                self.node_stats
                    .levels
                    .get_mut(&tree_level)
                    .unwrap()
                    .n256
                    .value_count += node.as_ref().value_count();
            }
        }
    }
}

impl<const K_LEN: usize, A: Allocator + Clone + Send> CongeeInner<K_LEN, A> {
    /// Returns the node stats for the tree.
    pub fn stats(&self) -> NodeStats {
        let mut visitor = StatsVisitor {
            node_stats: NodeStats::default(),
        };

        self.dfs_visitor_slow(&mut visitor).unwrap();
        let pin = crossbeam_epoch::pin();
        visitor.node_stats.kv_pairs = self.value_count(&pin);

        visitor.node_stats
    }
}
