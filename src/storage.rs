use crate::{
    metric::{DataPoint, Row},
    partition::{MemoryPartition, PointPartitionOrdering},
    window::InsertWindow,
};

pub struct Storage {
    partitions: Vec<MemoryPartition>,
    config: Config,
    insert_window: InsertWindow,
}

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
}

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

    fn insert_row(&mut self, row: &Row) {
        match self.partitions.last() {
            Some(p) => match p.ordering(row) {
                PointPartitionOrdering::Current => p.insert(row),
                PointPartitionOrdering::Future => {
                    self.partitions.push(MemoryPartition::new(
                        Some(self.config.partition_duration),
                        row,
                    ));
                }
                PointPartitionOrdering::Past => {
                    // TODO:major: insert into old partitions.
                }
            },
            None => {
                self.partitions.push(MemoryPartition::new(
                    Some(self.config.partition_duration),
                    row,
                ));
            }
        }
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
}

#[cfg(test)]
pub mod tests {
    use crate::metric::{DataPoint, Row};

    use super::{Config, Storage};

    #[test]
    fn test_storage_insert_multiple_partitions() {
        let mut storage = Storage::new(Config {
            partition_duration: 2,
            insert_window: 0,
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
}
