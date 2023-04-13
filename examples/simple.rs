use std::{fs, time::Duration};

use rstorage::{
    storage::{Config, DiskConfig, PartitionConfig, Storage, StorageError},
    DataPoint, Row,
};

fn main() -> Result<(), StorageError> {
    let data_path = String::from("./examples/simple");
    let storage = Storage::new(Config {
        partition: PartitionConfig {
            hot_partitions: 2,
            max_partitions: 10,
            duration: 100,
        },
        disk: Some(DiskConfig {
            data_path: data_path.clone(),
            encode_strategy: rstorage::EncodeStrategy::Gorilla,
        }),
        insert_window: 200,
        sweep_interval: Some(1),
    })?;

    let batch_size = 500;
    let batches = 5;
    let sleep_time_secs = 1;
    let ts = 1600000000;

    for i in 0..batches {
        for j in 0..batch_size {
            let timestamp = ts + (i * batch_size + j);
            if let Err(e) = storage.insert(&Row {
                metric: "my_metric",
                data_point: DataPoint {
                    timestamp,
                    value: 0.1,
                },
            }) {
                println!(
                    "error inserting data point at timestamp {}: error: {}",
                    timestamp, e
                );
            }
        }

        println!("inserted {} metrics", batch_size);
        std::thread::sleep(Duration::from_secs(sleep_time_secs));
    }

    storage.close()?;

    // Uncomment this line to see the generated files on disk.
    fs::remove_dir_all(data_path).unwrap();
    Ok(())
}
