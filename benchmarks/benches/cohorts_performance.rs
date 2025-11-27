use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, PlotConfiguration, AxisScale};
use node::braid::algorithms::{cohorts, reverse};
use braidpool_benchmarks::braid::benchmark_types::SimpleNetwork;
use std::collections::{HashSet, HashMap};

pub fn benchmark_cohorts_vs_bead_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("cohorts_performance_vs_beads");

    // Configure for performance graph
    group.plot_config(PlotConfiguration::default()
        .summary_scale(AxisScale::Logarithmic));

    // Test across wide range of bead counts
    let bead_counts = [50, 100, 200, 500, 1000, 2000, 5000, 10000];

    for &bead_count in &bead_counts {
        println!("Generating parent map for {} beads...", bead_count);

        // Pre-generate parent maps (separate from timing)
        let mut network = SimpleNetwork::new(25, 4, 42);  // Fixed network
        let parents = network.simulate_parents(bead_count);
        let children = reverse(&parents);

        // Calculate and report average parents per bead
        let total_parents: usize = parents.values().map(|p| p.len()).sum();
        let avg_parents = total_parents as f64 / parents.len() as f64;
        println!("Generated {} beads (avg parents per bead: {:.2}), starting benchmark...", bead_count, avg_parents);

        group.bench_with_input(
            BenchmarkId::new("cohorts", bead_count),
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
    }

    group.finish();
}

criterion_group!(benches, benchmark_cohorts_vs_bead_count);
criterion_main!(benches);