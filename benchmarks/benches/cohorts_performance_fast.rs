use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, PlotConfiguration, AxisScale};
use node::braid::algorithms::{cohorts, reverse};
use braidpool_benchmarks::braid::simple_generator::SimpleParentGenerator;
use std::collections::{HashMap, HashSet};

pub fn benchmark_cohorts_vs_bead_count_fast(c: &mut Criterion) {
    let mut group = c.benchmark_group("cohorts_performance_fast");

    // Configure for performance graph
    group.plot_config(PlotConfiguration::default()
        .summary_scale(AxisScale::Logarithmic));

    // Test across wide range of bead counts (including larger ones)
    let bead_counts = [50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000];

    for &bead_count in &bead_counts {
        println!("Generating parent map for {} beads...", bead_count);

        // Pre-generate parent maps using fast generator
        let parents = SimpleParentGenerator::generate_parents_fast(bead_count, 42);
        let children = reverse(&parents);

        println!("Generated {} beads, starting benchmark...", bead_count);

        group.bench_with_input(
            BenchmarkId::new("cohorts_fast", bead_count),
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

criterion_group!(benches, benchmark_cohorts_vs_bead_count_fast);
criterion_main!(benches);