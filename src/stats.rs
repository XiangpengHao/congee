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
}

impl Display for NodeStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn calc_load_factor(n: &NodeInfo, scale: usize) -> f64 {
            if n.node_count == 0 {
                return 0.0;
            }
            (n.value_count as f64) / (n.node_count as f64 * scale as f64)
        }

        let mut levels = self.levels.values().collect::<Vec<_>>();
        levels.sort_by_key(|l| l.level);

        let mut node_count = 0;
        let mut total_f = 0.0;
        let mut memory_size = 0;
        let mut value_count = 0;

        for l in levels.iter() {
            total_f += l.n4.value_count as f64 / 4.0;
            total_f += l.n16.value_count as f64 / 16.0;
            total_f += l.n48.value_count as f64 / 48.0;
            total_f += l.n256.value_count as f64 / 256.0;

            node_count += l.node_count();
            memory_size += l.memory_size();
            value_count += l.value_count();

            writeln!(
                f,
                "Level: {} --- || N4: {:8}, {:8.2} || N16: {:8}, {:8.2} || N48: {:8}, {:8.2} || N256: {:8}, {:8.2} ||",
                l.level,
                l.n4.node_count,
                calc_load_factor(&l.n4, 4),
                l.n16.node_count,
                calc_load_factor(&l.n16, 16),
                l.n48.node_count,
                calc_load_factor(&l.n48, 48),
                l.n256.node_count,
                calc_load_factor(&l.n256, 256),
            )?;
        }

        writeln!(
            f,
            "Overall node count: {node_count}, entry count: {value_count}",
        )?;

        let load_factor = total_f / (node_count as f64);
        if load_factor < 0.5 {
            writeln!(f, "Load factor: {load_factor:.2} (too low)")?;
        } else {
            writeln!(f, "Load factor: {load_factor:.2}")?;
        }

        writeln!(f, "Active memory usage: {} Mb", memory_size / 1024 / 1024)?;

        writeln!(f, "KV count: {}", self.kv_pairs)?;

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
}

impl LevelStats {
    fn new_level(level: usize) -> Self {
        Self {
            level,
            n4: NodeInfo::default(),
            n16: NodeInfo::default(),
            n48: NodeInfo::default(),
            n256: NodeInfo::default(),
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
