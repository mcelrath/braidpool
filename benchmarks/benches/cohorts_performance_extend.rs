use bitcoin::pow::CompactTarget;
use bitcoin::{BlockHash, BlockHeader, BlockTime, BlockVersion, TxMerkleNode};
use criterion::{
    black_box, criterion_group, criterion_main, AxisScale, BenchmarkId, Criterion,
    PlotConfiguration,
};
use node::bead::Bead;
use node::braid::{algorithms, Relatives};
use node::braid::{Braid, ExtendStrategy};
use node::committed_metadata::CommittedMetadata;
use node::uncommitted_metadata::UnCommittedMetadata;
use rand::prelude::*;
use std::collections::HashMap;
use std::time::Instant;

// Helper to create a dummy bead with specific parents
fn create_dummy_bead(index: usize, parents: Vec<BlockHash>) -> Bead {
    // Create minimal valid metadata
    let committed = CommittedMetadata {
        parents: parents.iter().cloned().collect(),
        weak_target: CompactTarget::from_consensus(0x207fffff), // Min difficulty
        ..Default::default()
    };

    // Create minimal uncommitted metadata
    let uncommitted = UnCommittedMetadata {
        ..Default::default()
    };

    // Create dummy block header
    let empty_merkle_bytes: [u8; 32] = [0; 32];
    let block_header = BlockHeader {
        bits: CompactTarget::from_consensus(0x207fffff),
        merkle_root: TxMerkleNode::from_byte_array(empty_merkle_bytes),
        nonce: index as u32,
        prev_blockhash: BlockHash::from_byte_array([0; 32]),
        time: BlockTime::from_u32(12345 + index as u32),
        version: BlockVersion::TWO,
    };

    Bead {
        block_header,
        committed_metadata: committed,
        uncommitted_metadata: uncommitted,
    }
}

// Generates a DAG with 'layers' of width 'width'.
// Beads in layer L choose ALL parents from layer L-1.
// This forces cohorts of size ~width (Fully Connected Bipartite Layers).
fn generate_wide_parents(
    total_beads: usize,
    width: usize,
    seed: u64,
) -> HashMap<usize, Vec<usize>> {
    let _rng = StdRng::seed_from_u64(seed);
    let mut parents_map: HashMap<usize, Vec<usize>> = HashMap::new();

    // Bead 0 is genesis.
    parents_map.insert(0, vec![]);

    let mut prev_layer = vec![0];
    let mut current_id = 1;

    while current_id < total_beads {
        let mut current_layer = Vec::new();
        let layer_size = width; // constant width

        for _ in 0..layer_size {
            if current_id >= total_beads {
                break;
            }

            // Fully connected: All beads in prev_layer are parents
            let chosen_parents = prev_layer.clone();

            parents_map.insert(current_id, chosen_parents);
            current_layer.push(current_id);
            current_id += 1;
        }
        prev_layer = current_layer;
    }

    parents_map
}

struct ScenarioResult {
    total_beads: usize,
    target_width: usize,
    actual_avg_beads_per_cohort: f64,
    avg_anticone: f64, // New field
    heuristic_time_ms: f64,
    cached_time_ms: f64,
    nocache_time_ms: f64,
}

fn calculate_avg_anticone(braid: &Braid) -> f64 {
    let n = braid.beads.len() as f64;
    if n <= 1.0 {
        return 0.0;
    }

    let mut full_ancestors_map = Relatives::new(); // Will hold final ancestors for all beads
    let mut temp_algo_cache = Relatives::new(); // Cache used by algorithms::all_ancestors

    // Ensure ancestor_cache is fully populated for all beads.
    // braid.ancestor_cache (from Braid::new) is populated by algorithms::cohorts
    // which calls all_ancestors for tail elements. We need it for ALL beads.
    for i in 0..braid.beads.len() {
        algorithms::all_ancestors(
            i,
            &braid.parents,
            &mut full_ancestors_map,
            &mut temp_algo_cache,
        );
    }

    let total_ancestors: usize = full_ancestors_map.values().map(|s| s.len()).sum();
    // Debug print
    //println!("Debug: N={}, Total Ancestors = {}, Avg Ancestors = {:.2}", n, total_ancestors, total_ancestors as f64 / n);

    let avg_ancestors = total_ancestors as f64 / n;

    (n - 1.0) - (2.0 * avg_ancestors)
}

pub fn benchmark_extend_strategies(c: &mut Criterion) {
    let mut group = c.benchmark_group("extend_strategies_width_comparison");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(10));

    // (Total Beads, Target Width)
    // We want to test small vs large width.
    let scenarios = [
        (1000, 1),
        (1000, 10),
        (1000, 50),
        (1000, 100),  // Very wide
        (10000, 500), // New large test
    ];

    let strategies = [
        ExtendStrategy::Heuristic,
        ExtendStrategy::Cached,
        ExtendStrategy::NoCache,
    ];

    let mut all_scenario_results: Vec<ScenarioResult> = Vec::new(); // Collect results for final table

    for &(count, width) in &scenarios {
        // Generate wide DAG
        let gen_count = count + width + 10;
        let parents_map = generate_wide_parents(gen_count, width, 42);

        let mut idx_to_hash: HashMap<usize, BlockHash> = HashMap::new();
        let mut beads: Vec<Bead> = Vec::with_capacity(gen_count);

        for i in 0..gen_count {
            let parent_indices = parents_map.get(&i).unwrap();
            let parent_hashes: Vec<BlockHash> = parent_indices
                .iter()
                .map(|&pid| idx_to_hash[&pid])
                .collect();

            let bead = create_dummy_bead(i, parent_hashes);
            idx_to_hash.insert(i, bead.hash());
            beads.push(bead);
        }

        // Create braid up to 'count'
        let initial_beads = beads[0..count].to_vec();
        let braid = Braid::new(initial_beads);

        // We want to add a bead that continues the pattern.
        let bead_to_add = beads[count].clone();

        // Stats
        let num_cohorts = braid.cohorts.len();
        let actual_avg_beads_per_cohort = if num_cohorts > 0 {
            count as f64 / num_cohorts as f64
        } else {
            0.0
        };

        // Calculate k-width (anticone) using the initial braid state
        let avg_anticone = calculate_avg_anticone(&braid);

        let mut scenario_times = HashMap::new(); // Collect times for current scenario

        let mut baseline_cohorts = None;

        for &strategy in &strategies {
            // 1. Verification
            let mut b_verify = braid.clone();
            b_verify.extend_strategy = strategy;
            b_verify.extend(&bead_to_add);

            if strategy == ExtendStrategy::Heuristic {
                baseline_cohorts = Some(b_verify.cohorts.clone());
            } else {
                let baseline = baseline_cohorts.as_ref().expect("Heuristic must run first");
                if &b_verify.cohorts != baseline {
                    eprintln!("WARNING: Strategy {:?} produced different cohorts than Heuristic for width {}!", strategy, width);
                }
            }

            // 2. Manual Timing
            // We must measure clone time separately to subtract it, as it dominates for large N.
            let iterations = 20;
            let mut total_extend_duration = std::time::Duration::new(0, 0);

            for _ in 0..iterations {
                let b_temp = braid.clone();
                // Start timing ONLY the extend operation
                let start = Instant::now();
                let mut b_bench = b_temp;
                b_bench.extend_strategy = strategy;
                black_box(b_bench.extend(black_box(&bead_to_add)));
                total_extend_duration += start.elapsed();
            }

            let avg_time_ms = total_extend_duration.as_secs_f64() * 1000.0 / iterations as f64;

            scenario_times.insert(strategy, avg_time_ms);

            // 3. Criterion
            let strategy_name = format!("{:?}_w{}", strategy, width);
            group.bench_with_input(BenchmarkId::new(strategy_name, count), &count, |b, _| {
                b.iter_batched(
                    || (braid.clone(), bead_to_add.clone()),
                    |(mut b, new_bead)| {
                        b.extend_strategy = strategy;
                        black_box(b.extend(black_box(&new_bead)))
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }

        all_scenario_results.push(ScenarioResult {
            total_beads: count,
            target_width: width,
            actual_avg_beads_per_cohort: actual_avg_beads_per_cohort,
            avg_anticone: avg_anticone,
            heuristic_time_ms: *scenario_times.get(&ExtendStrategy::Heuristic).unwrap(),
            cached_time_ms: *scenario_times.get(&ExtendStrategy::Cached).unwrap(),
            nocache_time_ms: *scenario_times.get(&ExtendStrategy::NoCache).unwrap(),
        });
    }

    group.finish();

    println!("\n=== Final Comparison Table (Columnar) ===");
    println!(
        "| {:>8} | {:>18} | {:>12} | {:>18} | {:>18} | {:>18} |",
        "Beads", "Avg Beads/Cohort", "Avg k-width", "Heuristic (ms)", "Cached (ms)", "NoCache (ms)"
    );
    println!("|:--------:|:----------------:|:------------:|:------------------:|:------------------:|:------------------:|");
    for res in all_scenario_results {
        println!(
            "| {:>8} | {:>18.2} | {:>12.2} | {:>18.4} | {:>18.4} | {:>18.4} |",
            res.total_beads,
            res.actual_avg_beads_per_cohort,
            res.avg_anticone,
            res.heuristic_time_ms,
            res.cached_time_ms,
            res.nocache_time_ms
        );
    }
}

criterion_group!(benches, benchmark_extend_strategies);
criterion_main!(benches);
