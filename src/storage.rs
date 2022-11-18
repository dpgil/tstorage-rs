use crate::{
    metric::{DataPoint, Row},
    partition::{MemoryPartition, PointPartitionOrdering},
};

pub struct Storage {
    partitions: Vec<MemoryPartition>,
    config: Config,
}

pub struct Config {
    // TODO:minor: use Duration.
    partition_duration: i64,
}

impl Storage {
    pub fn new(config: Config) -> Self {
        Self {
            partitions: vec![],
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
}
