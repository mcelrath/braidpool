use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, PlotConfiguration, AxisScale};
use node::braid::algorithms::{cohorts, reverse};
use braidpool_benchmarks::braid::{SimpleNetwork, BeadIdx};
use std::collections::{HashSet, HashMap};

pub fn benchmark_cohorts_vs_bead_count_analyzed(c: &mut Criterion) {
    let mut group = c.benchmark_group("cohorts_performance_analyzed");

    // Configure for performance graph
    group.plot_config(PlotConfiguration::default()
        .summary_scale(AxisScale::Logarithmic));

    // Test across wide range of bead counts
    let bead_counts = [100, 500, 1000, 2000, 5000];

    for &bead_count in &bead_counts {
        println!("Generating parent map for {} beads...", bead_count);

        // Pre-generate parent maps
        let mut network = SimpleNetwork::new(25, 4, 42);
        let parents = network.simulate_parents(bead_count);
        let children = reverse(&parents);

        // Analyze braid structure
        SimpleNetwork::analyze_braid_structure(&parents);

        // Run cohorts analysis
        let mut cache = HashMap::new();
        let initial = HashSet::new();
        let cohorts_result = cohorts(&parents, &children, &initial, &mut cache);
        println!("Cohorts algorithm found {} cohorts", cohorts_result.len());

        // Analyze cohort structure
        println!("\n=== Detailed Cohort Analysis ===");
        let mut cohort_sizes: Vec<usize> = cohorts_result.iter().map(|c| c.len()).collect();
        cohort_sizes.sort();

        if !cohort_sizes.is_empty() {
            let avg_cohort_size = cohort_sizes.iter().sum::<usize>() as f64 / cohort_sizes.len() as f64;
            let max_cohort_size = cohort_sizes.last().unwrap_or(&0);
            let min_cohort_size = cohort_sizes.first().unwrap_or(&0);

            println!("Cohort sizes:");
            println!("  Average: {:.2}", avg_cohort_size);
            println!("  Min: {}", min_cohort_size);
            println!("  Max: {}", max_cohort_size);
            println!("  Median: {}", cohort_sizes[cohort_sizes.len() / 2]);

            // Distribution of cohort sizes
            let mut cohort_distribution = std::collections::HashMap::new();
            for size in &cohort_sizes {
                *cohort_distribution.entry(*size).or_insert(0) += 1;
            }

            println!("  Cohort size distribution:");
            let mut sorted_pairs: Vec<_> = cohort_distribution.iter().collect();
            sorted_pairs.sort_by_key(|&(k, _)| k);
            for (size, frequency) in sorted_pairs {
                println!("    {} beads: {} cohorts ({:.1}%)", size, frequency,
                        (*frequency as f64 / cohorts_result.len() as f64) * 100.0);
            }

            // Parents per cohort
            let parents_per_cohort: Vec<usize> = cohorts_result.iter()
                .map(|cohort| cohort.iter().map(|&bead_id| parents.get(&bead_id).map(|p| p.len()).unwrap_or(0)).sum::<usize>())
                .collect();

            if !parents_per_cohort.is_empty() {
                let avg_parents_per_cohort = parents_per_cohort.iter().sum::<usize>() as f64 / parents_per_cohort.len() as f64;
                println!("\nParents per cohort:");
                println!("  Average: {:.2}", avg_parents_per_cohort);
                println!("  Distribution:");
                for (i, &count) in parents_per_cohort.iter().enumerate() {
                    println!("    Cohort {}: {} parents", i + 1, count);
                }
            }

            // Individual cohort details
            println!("\nIndividual Cohort Details:");
            for (i, cohort) in cohorts_result.iter().enumerate() {
                let parent_count = cohort.iter()
                    .map(|&bead_id| parents.get(&bead_id).map(|p| p.len()).unwrap_or(0))
                    .sum::<usize>();
                println!("  Cohort {}: {} beads, {} total parent references", i + 1, cohort.len(), parent_count);

                // Show a few beads from this cohort
                if cohort.len() > 0 {
                    let first_few: Vec<BeadIdx> = cohort.iter().take(3).copied().collect();
                    println!("    Sample beads: {:?}", first_few);
                }
            }
        }

        println!("\nStarting benchmark...");
        group.bench_with_input(
            BenchmarkId::new("cohorts_analyzed", bead_count),
            &bead_count,
            |b, _| {
                // Benchmark ONLY the cohorts() function
                b.iter(|| {
                    let initial = HashSet::new();
                    let mut cache = HashMap::new();
                    black_box(cohorts(black_box(&parents), black_box(&children), &initial, &mut cache))
                });
            },
        );

        println!("---\n");
    }

    group.finish();
}

criterion_group!(benches, benchmark_cohorts_vs_bead_count_analyzed);
criterion_main!(benches);