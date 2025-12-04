use crate::braid::benchmark_types::{BeadIdx, ParentMap, SimpleNetwork};
use crate::braid::simple_generator::SimpleParentGenerator;
use rand::prelude::*;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

#[derive(Debug, Clone, Copy)]
pub enum ParentGeneratorKind {
    Layered,
    Simple,
    Network,
}

impl ParentGeneratorKind {
    pub fn name(&self) -> &'static str {
        match self {
            ParentGeneratorKind::Layered => "layered",
            ParentGeneratorKind::Simple => "simple",
            ParentGeneratorKind::Network => "network",
        }
    }
}

impl FromStr for ParentGeneratorKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_lowercase().as_str() {
            "layered" => Ok(ParentGeneratorKind::Layered),
            "simple" => Ok(ParentGeneratorKind::Simple),
            "network" => Ok(ParentGeneratorKind::Network),
            other => Err(format!("Unknown generator: {}", other)),
        }
    }
}

impl Default for ParentGeneratorKind {
    fn default() -> Self {
        ParentGeneratorKind::Layered
    }
}

/// Generate a parent map tuned to the desired average cohort width.
pub fn generate_parents_for_scenario(
    kind: ParentGeneratorKind,
    total_beads: usize,
    avg_cohort_size: f64,
    seed: u64,
) -> ParentMap {
    let width = avg_cohort_size.clamp(1.0, 500.0);
    match kind {
        ParentGeneratorKind::Layered => layered_parents(total_beads, width, seed),
        ParentGeneratorKind::Simple => simple_parents(total_beads, width.ceil() as usize, seed),
        ParentGeneratorKind::Network => network_parents(total_beads, width.ceil() as usize, seed),
    }
}

fn layered_parents(total_beads: usize, target_width: f64, seed: u64) -> ParentMap {
    let mut parents: ParentMap = HashMap::new();
    parents.insert(0, HashSet::new());
    let mut prev_layer = vec![0usize];
    let mut current_id = 1usize;
    let mut rng = StdRng::seed_from_u64(seed);
    let floor_w = target_width.floor().max(1.0) as usize;
    let ceil_w = target_width.ceil().max(1.0) as usize;
    let p_ceil = (target_width - target_width.floor()).max(0.0);

    while current_id < total_beads {
        let mut layer = Vec::new();
        let width = if rng.gen::<f64>() < p_ceil {
            ceil_w
        } else {
            floor_w
        };
        for _ in 0..width {
            if current_id >= total_beads {
                break;
            }
            let parent_set = choose_parent_subset(&prev_layer, width, &mut rng);
            parents.insert(current_id, parent_set);
            layer.push(current_id);
            current_id += 1;
        }
        if layer.is_empty() {
            break;
        }
        prev_layer = layer;
    }

    parents
}

fn choose_parent_subset(
    prev_layer: &[BeadIdx],
    max_parents: usize,
    rng: &mut StdRng,
) -> HashSet<BeadIdx> {
    let mut parents = HashSet::new();
    if prev_layer.is_empty() {
        return parents;
    }

    let count = max_parents.min(prev_layer.len()).max(1);
    let mut candidates = prev_layer.to_vec();

    for _ in 0..count {
        if candidates.is_empty() {
            break;
        }
        let idx = rng.gen_range(0..candidates.len());
        parents.insert(candidates.swap_remove(idx));
    }

    if parents.is_empty() {
        parents.insert(prev_layer[0]);
    }

    parents
}

fn simple_parents(total_beads: usize, max_parents: usize, seed: u64) -> ParentMap {
    let mut generator = SimpleParentGenerator::new(seed, (1, max_parents.max(1)));
    generator.generate_parents(total_beads)
}

fn network_parents(total_beads: usize, _width: usize, seed: u64) -> ParentMap {
    let mut network = SimpleNetwork::new(20, 3, seed);
    network.simulate_parents(total_beads)
}
