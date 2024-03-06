use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use rust_billion_row_challenge::process_file;

fn benchmark(c: &mut Criterion) {
    let address = std::env::var("MEASUREMENTS_FILE").expect("No file specified");

    let mut group = c.benchmark_group("File Processing");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(5));
    group.measurement_time(Duration::from_secs(100));
    group.bench_function("process_file", |b| b.iter(|| process_file(&address)));

    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);