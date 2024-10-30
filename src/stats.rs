use std::fmt::Display;

use crate::{
    base_node::{BaseNode, NodeType, MAX_KEY_LEN},
    tree::RawCongee,
    Allocator,
};

#[derive(Default, Debug, serde::Serialize)]
pub struct NodeStats(Vec<LevelStats>);

impl Display for NodeStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn calc_load_factor(n: (usize, usize), scale: usize) -> f64 {
            if n.0 == 0 {
                return 0.0;
            }
            (n.1 as f64) / (n.0 as f64 * scale as f64)
        }

        let mut total_node = 0;
        let mut total_f = 0.0;
        let mut memory_size = 0;

        for l in self.0.iter() {
            total_f += l.n4.1 as f64 / 4.0;
            total_f += l.n16.1 as f64 / 16.0;
            total_f += l.n48.1 as f64 / 48.0;
            total_f += l.n256.1 as f64 / 256.0;

            total_node += l.total_nodes();
            memory_size += l.memory_size();

            writeln!(
                f,
                "Level: {} --- || N4: {:8}, {:8.2} || N16: {:8}, {:8.2} || N48: {:8}, {:8.2} || N256: {:8}, {:8.2} ||",
                l.level,
                l.n4.0,
                calc_load_factor(l.n4, 4),
                l.n16.0,
                calc_load_factor(l.n16, 16),
                l.n48.0,
                calc_load_factor(l.n48, 48),
                l.n256.0,
                calc_load_factor(l.n256, 256),
            )?;
        }

        let load_factor = total_f / (total_node as f64);
        if load_factor < 0.5 {
            writeln!(f, "Load factor: {load_factor:.2} (too low)")?;
        } else {
            writeln!(f, "Load factor: {load_factor:.2}")?;
        }

        writeln!(f, "Active memory usage: {} Mb", memory_size / 1024 / 1024)?;

        Ok(())
    }
}

#[derive(Debug, serde::Serialize, Clone)]
pub struct LevelStats {
    level: usize,
    n4: (usize, usize), // (node count, leaf count)
    n16: (usize, usize),
    n48: (usize, usize),
    n256: (usize, usize),
}

impl LevelStats {
    fn new_level(level: usize) -> Self {
        Self {
            level,
            n4: (0, 0),
            n16: (0, 0),
            n48: (0, 0),
            n256: (0, 0),
        }
    }

    fn memory_size(&self) -> usize {
        self.n4.0 * NodeType::N4.node_layout().size()
            + self.n16.0 * NodeType::N16.node_layout().size()
            + self.n48.0 * NodeType::N48.node_layout().size()
            + self.n256.0 * NodeType::N256.node_layout().size()
    }

    fn total_nodes(&self) -> usize {
        self.n4.0 + self.n16.0 + self.n48.0 + self.n256.0
    }
}

impl<const K_LEN: usize, A: Allocator + Clone> RawCongee<K_LEN, A> {
    /// Returns the node stats for the tree.
    pub fn stats(&self) -> NodeStats {
        let mut node_stats = NodeStats::default();

        let mut sub_nodes = vec![(0, 0, self.root as *const BaseNode)];

        while let Some((level, key_level, node)) = sub_nodes.pop() {
            let node = unsafe { &*node };

            if node_stats.0.len() <= level {
                node_stats.0.push(LevelStats::new_level(level));
            }

            match node.get_type() {
                crate::base_node::NodeType::N4 => {
                    node_stats.0[level].n4.0 += 1;
                    node_stats.0[level].n4.1 += node.get_count();
                }
                crate::base_node::NodeType::N16 => {
                    node_stats.0[level].n16.0 += 1;
                    node_stats.0[level].n16.1 += node.get_count();
                }
                crate::base_node::NodeType::N48 => {
                    node_stats.0[level].n48.0 += 1;
                    node_stats.0[level].n48.1 += node.get_count();
                }
                crate::base_node::NodeType::N256 => {
                    node_stats.0[level].n256.0 += 1;
                    node_stats.0[level].n256.1 += node.get_count();
                }
            }

            let children = node.get_children(0, 255);
            for (_k, n) in children {
                if key_level != (MAX_KEY_LEN - 1) {
                    sub_nodes.push((
                        level + 1,
                        key_level + 1 + unsafe { &*n.as_ptr() }.prefix().len(),
                        n.as_ptr(),
                    ));
                }
            }
        }
        node_stats
    }
}
