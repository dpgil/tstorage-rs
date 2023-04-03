use std::fs;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rstorage::{
    storage::{Config, Storage},
    DataPoint, EncodeStrategy, Row,
};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("insert 100k series", |b| {
        b.iter(|| {
            let data_path = String::from("./test_bench_data");
            let mut storage = Storage::new(Config {
                partition_duration: 10000,
                insert_window: 100,
                data_path: data_path.clone(),
                encode_strategy: EncodeStrategy::CSV,
                num_writeable_partitions: 2,
            });
            let num_metrics = 100_000;
            let metric = "hello";
            for i in 0..num_metrics {
                storage.insert(&[Row {
                    metric,
                    data_point: DataPoint {
                        timestamp: i,
                        value: 123.0,
                    },
                }])
            }
            #[allow(unused_must_use)]
            {
                fs::remove_dir_all(data_path);
            }
        });
    });

    c.bench_function("select", |b| {
        // Insert 100k data points
        let data_path = String::from("./test_bench_data");
        let mut storage = Storage::new(Config {
            partition_duration: 10_000,
            insert_window: 100,
            data_path: data_path.clone(),
            encode_strategy: EncodeStrategy::CSV,
            num_writeable_partitions: 2,
        });
        let num_metrics = 10_000;
        let metric = "hello";
        for i in 0..num_metrics {
            black_box(storage.insert(&[Row {
                metric,
                data_point: DataPoint {
                    timestamp: i,
                    value: 123.0,
                },
            }]))
        }

        let num_selects = 1000;
        b.iter(|| {
            for _ in 0..num_selects {
                black_box(storage.select(metric, 0, 100_000));
            }
        });

        #[allow(unused_must_use)]
        {
            fs::remove_dir_all(data_path);
        }
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
