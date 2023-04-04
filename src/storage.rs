use std::path::{Path, PathBuf};

use crate::{
    encode::encode::EncodeStrategy,
    metric::{DataPoint, Row},
    partition::{disk::open, memory::MemoryPartition, Partition, PointPartitionOrdering, Boundary},
    window::InsertWindow,
};
use anyhow::Result;

pub struct Storage {
    partitions: Vec<Box<dyn Partition>>,
    config: Config,
    insert_window: InsertWindow,
}

#[derive(Default)]
pub struct Config {
    // The size of partitions that store data points.
    // TODO:minor: use Duration.
    pub partition_duration: i64,
    // Tolerance for inserting out-of-order data points.
    // Given the last data point inserted with timestamp t,
    // the insert window will allow a data point with a timestamp
    // in the past up to t - insert_window.
    // An insert_window of 0 means out-of-order inserts are not
    // allowed.
    pub insert_window: i64,
    // Path to where disk partitions are stored.
    pub data_path: String,
    // Type of encoder for data point encoding.
    pub encode_strategy: EncodeStrategy,
    // Number of partitions to keep writeable, remainder will be read-only.
    pub num_writeable_partitions: usize,
}

impl Storage {
    pub fn new(config: Config) -> Self {
        Self {
            partitions: vec![],
            insert_window: InsertWindow::new(config.insert_window),
            config,
        }
    }

    pub fn select(&self, name: &str, start: i64, end: i64) -> Result<Vec<DataPoint>> {
        let mut result = vec![];
        for partition in &self.partitions {
            result.append(&mut partition.select(name, start, end)?);
        }
        Ok(result)
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
        self.partitions.push(Box::new(MemoryPartition::new(
            Some(self.config.partition_duration),
            row,
        )));

        // TODO: do this asynchronously
        self.flush_partitions();
    }

    fn flush_partitions(&mut self) {
        self.partitions
            .iter_mut()
            .rev()
            .enumerate()
            .for_each(|(i, p)| {
                if i < self.config.num_writeable_partitions {
                    return;
                }

                // Flush partition and convert it to disk partition.
                // TODO: make sure we're not re-flushing disk partitions here.
                let dir_path = get_dir_path(&self.config.data_path, p.boundary());
                if let Err(e) = p.flush(&dir_path, self.config.encode_strategy) {
                    // TODO: handle flush error
                    println!("error flushing partition {:?}", e);
                    return;
                }
                match open(&dir_path) {
                    Ok(disk_partition) => {
                        // Swap the partition with a disk partition.
                        *p = Box::new(disk_partition);
                    }
                    Err(e) => {
                        // TODO: handle open error
                        println!("error opening partition {:?}", e);
                        return;
                    }
                }
            });
    }

    fn cascade_past_insert(&self, row: &Row) {
        match self.partitions.split_last() {
            Some((_, past_partitions)) => {
                let mut iter = past_partitions.iter().rev();
                let mut i = 1;
                while i < self.config.num_writeable_partitions {
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
}

pub fn get_dir_path(data_path: &str, boundary: Boundary) -> PathBuf {
    Path::new(data_path).join(format!(
        "p-{}-{}",
        boundary.min_timestamp(),
        boundary.max_timestamp()
    ))
}

#[cfg(test)]
pub mod tests {
    use std::fs;

    use crate::metric::{DataPoint, Row};

    use super::{Config, Storage};

    #[test]
    fn test_storage_insert_multiple_partitions() {
        let mut storage = Storage::new(Config {
            partition_duration: 2,
            num_writeable_partitions: 3,
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
            storage.insert(&[Row { metric, data_point }]);
        }
        assert_eq!(storage.partitions.len(), 3);

        let result = storage.select(&metric.to_string(), 0, 7).unwrap();
        assert_eq!(result, data_points);
    }

    #[test]
    fn test_storage_insert_window() {
        let mut storage = Storage::new(Config {
            partition_duration: 100,
            insert_window: 5,
            num_writeable_partitions: 2,
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
            storage.insert(&[Row { metric, data_point }]);
        }

        let result = storage.select(&metric.to_string(), 0, 20).unwrap();
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
            num_writeable_partitions: 2,
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
            storage.insert(&[Row { metric, data_point }]);
        }

        let result = storage.select(&metric.to_string(), 0, 20).unwrap();
        assert_eq!(result, vec![data_points[0]]);
    }

    #[test]
    fn test_storage_insert_window_across_partitions() {
        let mut storage = Storage::new(Config {
            partition_duration: 2,
            insert_window: 5,
            num_writeable_partitions: 2,
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
            storage.insert(&[Row { metric, data_point }]);
        }

        let result = storage.select(&metric.to_string(), 0, 20).unwrap();

        let mut expected = data_points.clone();
        expected.sort_by_key(|d| d.timestamp);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_storage_memory_and_disk_partitions() {
        let data_path = String::from("./test_storage_memory_and_disk_partitions");
        let mut storage = Storage::new(Config {
            partition_duration: 10,
            insert_window: 20,
            num_writeable_partitions: 2,
            data_path: data_path.clone(),
            ..Default::default()
        });

        let metric = "hello";
        let data_points = [
            DataPoint {
                timestamp: 0,
                value: 0.0,
            },
            DataPoint {
                timestamp: 5,
                value: 0.0,
            },
            DataPoint {
                timestamp: 10, // start of 2nd partition
                value: 0.0,
            },
            DataPoint {
                timestamp: 12,
                value: 1.0,
            },
            DataPoint {
                timestamp: 20, // start of 3rd partition, 1st partition should now be flushed
                value: 2.0,
            },
            DataPoint {
                timestamp: 30, // start of 4th partition, 2nd partition should now be flushed
                value: 3.0,
            },
        ];

        for data_point in data_points {
            storage.insert(&[Row { metric, data_point }]);
        }

        let result = storage.select(&metric.to_string(), 0, 40).unwrap();

        let expected = data_points.clone();
        assert_eq!(result, expected);

        fs::remove_dir_all(data_path).unwrap();
    }
}
