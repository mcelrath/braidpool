use rand::prelude::*;
use std::collections::{HashMap, HashSet, VecDeque};

pub type BeadIdx = usize;
pub type NodeId = u32;
pub type ParentMap = HashMap<BeadIdx, HashSet<BeadIdx>>;

#[derive(Debug, Clone)]
pub struct SimpleNode {
    pub id: NodeId,
    pub position: (f64, f64),      // (latitude, longitude) in radians
    pub peers: Vec<(NodeId, f64)>, // (peer_id, latency)
    pub hashrate: f64,
    pub known_beads: ParentMap,
    pub tips: HashSet<BeadIdx>,
    pub next_mining_time: f64,
}

pub struct SimpleNetwork {
    pub nodes: Vec<SimpleNode>,
    pub current_time: f64,
    pub pending_transmissions: VecDeque<Transmission>,
    pub parents: ParentMap, // This is what we'll generate for the algorithm
    pub next_bead_id: BeadIdx,
}

#[derive(Debug, Clone)]
pub struct Transmission {
    pub bead_id: BeadIdx,
    pub bead_parents: HashSet<BeadIdx>,
    pub target_node: NodeId,
    pub arrival_time: f64,
}

impl SimpleNetwork {
    pub fn new(num_nodes: usize, _peers_per_node: usize, seed: u64) -> Self {
        let _rng = StdRng::seed_from_u64(seed);
        let mut nodes = Vec::new();

        // Generate nodes with simplified structure - we don't need geographic complexity for benchmarking
        for i in 0..num_nodes {
            let node = SimpleNode {
                id: i as NodeId,
                position: (0.0, 0.0), // Simplified - no need for real coordinates
                peers: Vec::new(),    // Simplified - no peer connections needed
                hashrate: 1.0 / num_nodes as f64,
                known_beads: HashMap::new(),
                tips: HashSet::new(),
                next_mining_time: 0.0,
            };
            nodes.push(node);
        }

        let mut network = SimpleNetwork {
            nodes,
            current_time: 0.0,
            pending_transmissions: VecDeque::new(),
            parents: HashMap::new(),
            next_bead_id: 0,
        };

        // Create genesis bead
        network.create_genesis_parents();
        network
    }

    fn create_genesis_parents(&mut self) {
        // Genesis bead has no parents
        self.parents.insert(0, HashSet::new());
        self.next_bead_id = 1;

        // Distribute genesis to all nodes
        for node in &mut self.nodes {
            node.known_beads.insert(0, HashSet::new());
            node.tips.insert(0);
            // Set initial mining times
            node.next_mining_time = Self::sample_geometric(&mut rand::thread_rng(), node.hashrate);
        }
    }

    fn sample_geometric(rng: &mut impl Rng, hashrate: f64) -> f64 {
        // Simplified geometric distribution
        let p = hashrate;
        let u: f64 = rng.gen();
        ((1.0 - u).ln() / (1.0 - p).ln()).ceil() as f64
    }

    pub fn simulate_parents(&mut self, target_beads: usize) -> ParentMap {
        while self.parents.len() < target_beads {
            // Find the node with the earliest mining time
            let (next_node_idx, next_time, next_mining_time_delta) = {
                let (idx, node) = self
                    .nodes
                    .iter()
                    .enumerate()
                    .min_by(|(_, a), (_, b)| {
                        a.next_mining_time.partial_cmp(&b.next_mining_time).unwrap()
                    })
                    .unwrap();

                let hashrate = node.hashrate;
                let delta = Self::sample_geometric(&mut rand::thread_rng(), hashrate);

                (idx, node.next_mining_time, delta)
            };

            // Advance time to the next mining event
            self.current_time = next_time;

            // Create wider, more branching braids by selecting multiple parents
            // Use the most recent beads as potential parents to create branching
            let total_beads = self.parents.len();
            let target_parent_count = std::cmp::max(3, (total_beads / 50) + 1); // Grow with braid size

            let mut working_parents = HashSet::new();

            // Select parents from recent beads to create more branching structure
            let start_idx = if total_beads > target_parent_count * 2 {
                total_beads - target_parent_count * 2
            } else {
                0
            };

            for i in start_idx..total_beads {
                if working_parents.len() >= target_parent_count {
                    break;
                }
                working_parents.insert(i);
            }

            // If we don't have enough parents, also use some from the mining node's tips
            if working_parents.len() < target_parent_count {
                for &tip in &self.nodes[next_node_idx].tips {
                    working_parents.insert(tip);
                    if working_parents.len() >= target_parent_count {
                        break;
                    }
                }
            }

            if !working_parents.is_empty() {
                // Create new bead
                let bead_id = self.next_bead_id;
                self.next_bead_id += 1;

                // Add to global parents map
                self.parents.insert(bead_id, working_parents.clone());

                // Update all nodes that know the parents
                for node in &mut self.nodes {
                    let parents_known = working_parents
                        .iter()
                        .all(|parent_id| node.known_beads.contains_key(parent_id));

                    if parents_known {
                        // Add to node's knowledge
                        node.known_beads.insert(bead_id, working_parents.clone());

                        // Update tips
                        for parent in &working_parents {
                            node.tips.remove(parent);
                        }
                        node.tips.insert(bead_id);
                    }
                }

                // Schedule next mining time for the mining node
                self.nodes[next_node_idx].next_mining_time += next_mining_time_delta;
            }
        }

        self.parents.clone()
    }

    pub fn analyze_braid_structure(parents: &ParentMap) {
        println!("\n=== Braid Structure Analysis ===");
        println!("Total beads: {}", parents.len());

        // Calculate parent statistics
        let total_parents: usize = parents.values().map(|p| p.len()).sum();
        let avg_parents = total_parents as f64 / parents.len() as f64;

        let parent_counts: Vec<usize> = parents.values().map(|p| p.len()).collect();
        let max_parents = parent_counts.iter().max().unwrap_or(&0);
        let min_parents = parent_counts.iter().min().unwrap_or(&0);

        println!("Parents per bead:");
        println!("  Average: {:.2}", avg_parents);
        println!("  Min: {}", min_parents);
        println!("  Max: {}", max_parents);

        // Count nodes with different parent counts
        let mut parent_distribution = std::collections::HashMap::new();
        for count in &parent_counts {
            *parent_distribution.entry(*count).or_insert(0) += 1;
        }

        println!("  Distribution:");
        let mut sorted_pairs: Vec<_> = parent_distribution.iter().collect();
        sorted_pairs.sort_by_key(|&(k, _)| k);
        for (count, frequency) in sorted_pairs {
            println!(
                "    {} parents: {} beads ({:.1}%)",
                count,
                frequency,
                (*frequency as f64 / parents.len() as f64) * 100.0
            );
        }

        // Count genesis and tips
        let genesis_count = parents.values().filter(|p| p.is_empty()).count();
        let mut referenced = HashSet::new();
        for parents_set in parents.values() {
            for parent in parents_set {
                referenced.insert(*parent);
            }
        }
        let all_beads: HashSet<BeadIdx> = parents.keys().copied().collect();
        let tips: HashSet<BeadIdx> = &all_beads - &referenced;

        println!("Genesis beads: {}", genesis_count);
        println!("Tips: {}", tips.len());

        // Calculate fan-in distribution (how many beads reference each parent)
        let mut fan_in_count = std::collections::HashMap::new();
        for parents_set in parents.values() {
            for parent in parents_set {
                *fan_in_count.entry(*parent).or_insert(0) += 1;
            }
        }

        if !fan_in_count.is_empty() {
            let fan_in_values: Vec<usize> = fan_in_count.values().copied().collect();
            let avg_fan_in =
                fan_in_values.iter().sum::<usize>() as f64 / fan_in_values.len() as f64;
            let max_fan_in = fan_in_values.iter().max().unwrap_or(&0);

            println!("Fan-in distribution (how many children each parent has):");
            println!("  Average: {:.2}", avg_fan_in);
            println!("  Max: {}", max_fan_in);
        }
    }
}
