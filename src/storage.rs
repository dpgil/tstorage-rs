use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Seek, Write},
    path::Path,
};

use crate::{
    disk_partition::{MetricMetadata, PartitionMetadata, DATA_FILE_NAME, META_FILE_NAME},
    metric::{DataPoint, Row},
    partition::{MemoryPartition, PointPartitionOrdering},
    window::InsertWindow,
};
use anyhow::Result;

pub struct Storage {
    partitions: Vec<MemoryPartition>,
    config: Config,
    insert_window: InsertWindow,
}

#[derive(Default)]
pub struct Config {
    // The size of partitions that store data points.
    // TODO:minor: use Duration.
    partition_duration: i64,
    // Tolerance for inserting out-of-order data points.
    // Given the last data point inserted with timestamp t,
    // the insert window will allow a data point with a timestamp
    // in the past up to t - insert_window.
    // An insert_window of 0 means out-of-order inserts are not
    // allowed.
    insert_window: i64,
    // Path to where disk partitions are stored.
    data_path: String,
}

const NUM_WRITEABLE_PARTITIONS: i64 = 2;

impl Storage {
    pub fn new(config: Config) -> Self {
        Self {
            partitions: vec![],
            insert_window: InsertWindow::new(config.insert_window),
            config,
        }
    }

    pub fn select(&self, name: &str, start: i64, end: i64) -> Vec<DataPoint> {
        let mut result = vec![];
        for partition in &self.partitions {
            result.append(&mut partition.select(name, start, end));
        }
        result
    }

    pub fn insert(&mut self, rows: &[Row]) {
        // TODO:minor: should we assume rows are sorted?
        for row in rows {
            if self.insert_window.contains(row.data_point.timestamp) {
                self.insert_row(row);
                self.insert_window.update(row.data_point.timestamp);
            }
            // TODO:minor: explicitly reject points outside of insert window.
        }
    }

    fn create_partition_with_row(&mut self, row: &Row) {
        self.partitions.push(MemoryPartition::new(
            Some(self.config.partition_duration),
            row,
        ))
    }

    fn cascade_past_insert(&self, row: &Row) {
        match self.partitions.split_last() {
            Some((_, past_partitions)) => {
                let mut iter = past_partitions.iter().rev();
                let mut i = 1;
                while i < NUM_WRITEABLE_PARTITIONS {
                    i += 1;

                    match iter.next() {
                        Some(p) => match p.ordering(row) {
                            PointPartitionOrdering::Current => {
                                p.insert(row);
                                return;
                            }
                            PointPartitionOrdering::Future => return,
                            PointPartitionOrdering::Past => continue,
                        },
                        None => return,
                    }
                }
                // TODO:minor: non-writeable point. This means
                // insert_window > num_writeable_partitions * partition_duration, in other words,
                // we're accepting points that later cannot be written. We can validate this in the config.
            }
            None => return,
        }
    }

    fn insert_row(&mut self, row: &Row) {
        match self.partitions.last() {
            Some(p) => match p.ordering(row) {
                PointPartitionOrdering::Current => p.insert(row),
                PointPartitionOrdering::Future => self.create_partition_with_row(row),
                PointPartitionOrdering::Past => self.cascade_past_insert(row),
            },
            None => self.create_partition_with_row(row),
        }
    }

    fn flush(&self, partition: MemoryPartition) -> Result<()> {
        let dir_path = Path::new(&self.config.data_path).join(format!(
            "p-{}-{}",
            partition.min_timestamp(),
            partition.max_timestamp()
        ));
        fs::create_dir_all(dir_path.clone())?;

        let data_file_path = Path::new(&dir_path).join(DATA_FILE_NAME);
        let mut data = File::create(data_file_path)?;
        let mut metrics = HashMap::<String, MetricMetadata>::new();
        let mut total_data_points: i64 = 0;
        let min_timestamp = partition.min_timestamp();
        let max_timestamp = partition.max_timestamp();
        for (name, metric_entry) in partition.map.into_iter() {
            let offset = data.seek(std::io::SeekFrom::Current(0))?;
            // TODO: Pull out as CSV encoder
            for data_point in metric_entry.data_points.iter() {
                data.write_all(
                    format!("{},{}\n", data_point.timestamp, data_point.value).as_bytes(),
                )?;
            }
            let num_data_points: i64 = metric_entry.data_points.len().try_into().unwrap();
            total_data_points += num_data_points;
            metrics.insert(
                name.clone(),
                MetricMetadata {
                    name,
                    offset: offset.try_into().unwrap(),
                    min_timestamp: metric_entry.min_timestamp(),
                    max_timestamp: metric_entry.max_timestamp(),
                    num_data_points,
                },
            );
        }

        let partition_metadata = PartitionMetadata {
            min_timestamp,
            max_timestamp,
            num_data_points: total_data_points,
            metrics,
            created_at: 444, // TODO: Support created_at time
        };
        let meta_string = serde_json::to_string(&partition_metadata)?;
        let meta_file_path = Path::new(&dir_path).join(META_FILE_NAME);
        let mut meta = File::create(meta_file_path)?;
        meta.write_all(meta_string.as_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use crate::metric::{DataPoint, Row};

    use super::{Config, Storage};

    #[test]
    fn test_storage_insert_multiple_partitions() {
        let mut storage = Storage::new(Config {
            partition_duration: 2,
            ..Default::default()
        });

        let metric = "hello";
        let data_points = [
            DataPoint {
                timestamp: 1, // start of 1st partition
                value: 0.0,
            },
            DataPoint {
                timestamp: 2,
                value: 1.0,
            },
            DataPoint {
                timestamp: 3, // start of 2nd partition
                value: 0.0,
            },
            DataPoint {
                timestamp: 4,
                value: 0.0,
            },
            DataPoint {
                timestamp: 4,
                value: 1.0,
            },
            DataPoint {
                timestamp: 4,
                value: 2.0,
            },
            DataPoint {
                timestamp: 5, // start of 3rd partition
                value: 1.0,
            },
            DataPoint {
                timestamp: 6,
                value: 2.0,
            },
        ];

        for data_point in data_points {
            storage.insert(&[Row {
                metric: metric.to_string(),
                data_point,
            }])
        }
        assert_eq!(storage.partitions.len(), 3);

        let result = storage.select(&metric.to_string(), 0, 7);
        assert_eq!(result, data_points);
    }

    #[test]
    fn test_storage_insert_window() {
        let mut storage = Storage::new(Config {
            partition_duration: 100,
            insert_window: 5,
            ..Default::default()
        });

        let metric = "hello";
        let data_points = [
            DataPoint {
                timestamp: 10,
                value: 0.0,
            },
            DataPoint {
                timestamp: 20,
                value: 1.0,
            },
            DataPoint {
                timestamp: 15, // Within insert window (20-15>=5)
                value: 0.0,
            },
            DataPoint {
                timestamp: 14, // Outside of insert window
                value: 0.0,
            },
        ];

        for data_point in data_points {
            storage.insert(&[Row {
                metric: metric.to_string(),
                data_point,
            }])
        }

        let result = storage.select(&metric.to_string(), 0, 20);
        assert_eq!(result, vec![data_points[0], data_points[2], data_points[1]]);
    }

    #[test]
    // The first timestamp sets the base minimum timestamp for the storage.
    // Not my favorite solution but before we have more partitions, we drop
    // any delayed timestamps.
    fn test_storage_martian_point() {
        let mut storage = Storage::new(Config {
            partition_duration: 100,
            insert_window: 5,
            ..Default::default()
        });

        let metric = "hello";
        let data_points = [
            DataPoint {
                timestamp: 10,
                value: 0.0,
            },
            DataPoint {
                timestamp: 5, // no partition available for this one
                value: 0.0,
            },
        ];

        for data_point in data_points {
            storage.insert(&[Row {
                metric: metric.to_string(),
                data_point,
            }])
        }

        let result = storage.select(&metric.to_string(), 0, 20);
        assert_eq!(result, vec![data_points[0]]);
    }

    #[test]
    fn test_storage_insert_window_across_partitions() {
        let mut storage = Storage::new(Config {
            partition_duration: 2,
            insert_window: 5,
            ..Default::default()
        });

        let metric = "hello";
        let data_points = [
            DataPoint {
                timestamp: 1,
                value: 0.0,
            },
            DataPoint {
                timestamp: 2,
                value: 0.0,
            },
            DataPoint {
                timestamp: 3, // start of 2nd partition
                value: 0.0,
            },
            DataPoint {
                timestamp: 2, // within insert window, but inserted into previous partition
                value: 1.0,
            },
        ];

        for data_point in data_points {
            storage.insert(&[Row {
                metric: metric.to_string(),
                data_point,
            }])
        }

        let result = storage.select(&metric.to_string(), 0, 20);

        let mut expected = data_points.clone();
        expected.sort_by_key(|d| d.timestamp);

        assert_eq!(result, expected);
    }
}
