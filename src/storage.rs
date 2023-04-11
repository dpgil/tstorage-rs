use crate::{
    metric::{DataPoint, Row},
    partition::{
        disk::{self, flush, open, open_all},
        memory::MemoryPartition,
        Boundary, Partition, PartitionError, PointPartitionOrdering,
    },
    window::InsertWindow,
    EncodeStrategy,
};
use anyhow::Result;
use log::error;
use std::{
    num::TryFromIntError,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, RwLock},
    thread,
    time::Duration,
};
use thiserror::Error;

#[derive(Default)]
pub struct Config {
    pub partition: PartitionConfig,
    pub disk: Option<DiskConfig>,
    // Tolerance for inserting out-of-order data points.
    // Given the last data point inserted with timestamp t,
    // the insert window will allow a data point with a timestamp
    // in the past up to t - insert_window.
    // An insert_window of 0 means out-of-order inserts are not
    // allowed.
    pub insert_window: i64,
}

#[derive(Default)]
pub struct PartitionConfig {
    // Number of partitions to keep in memory, remaining will be flushed to disk.
    pub hot_partitions: usize,
    // Maximum number of partitions to persist. The database will evict
    // partitions older than this.
    pub max_partitions: i64,
    // The size of partitions that store data points, in seconds.
    pub duration: i64,
}

#[derive(Default)]
pub struct DiskConfig {
    // Path to where disk partitions are stored.
    pub data_path: String,
    // Type of encoder for data point encoding when flushing to disk.
    pub encode_strategy: EncodeStrategy,
}

#[derive(Error, Debug, PartialEq)]
pub enum ConfigError {
    #[error("insert window is larger than writable window")]
    InsertWindowError,
    #[error("hot_partitions is greater than max_partitions")]
    NumPartitionsError,
    #[error("error converting hot_partitions to i64")]
    HotPartitionsFormatError(TryFromIntError),
    #[error("hot partitions must be greater than zero to support writing data")]
    HotPartitionsError,
    #[error("disk config required when hot_partitions != max_partitions")]
    DiskConfigError,
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.partition.hot_partitions == 0 {
            return Err(ConfigError::HotPartitionsError);
        }

        let hot_partitions_result: Result<i64, TryFromIntError> =
            self.partition.hot_partitions.try_into();
        match hot_partitions_result {
            Ok(hot_partitions) => {
                let writable_window = hot_partitions * self.partition.duration;
                if self.insert_window > writable_window {
                    return Err(ConfigError::InsertWindowError);
                }
                if hot_partitions > self.partition.max_partitions {
                    return Err(ConfigError::NumPartitionsError);
                }
                if hot_partitions < self.partition.max_partitions && self.disk.is_none() {
                    return Err(ConfigError::DiskConfigError);
                }
                return Ok(());
            }
            Err(e) => Err(ConfigError::HotPartitionsFormatError(e)),
        }
    }
}

pub type StoragePartition = dyn Partition + Send + Sync;

struct StorageOuter {
    inner: Arc<Storage>,
}

impl StorageOuter {
    pub fn new(config: Config) -> Result<Self, StorageError> {
        let storage = Storage::new(config)?;
        let inner = Arc::new(storage);

        // TODO: naming
        let my_inner = inner.clone();
        thread::spawn(move || loop {
            // TODO: configurable sleep time
            thread::sleep(Duration::from_secs(1));
            my_inner.remove_expired_partitions();
            my_inner.flush_partitions();
            // TODO: tripwire
        });

        Ok(Self { inner })
    }
}

pub struct Storage {
    partitions: RwLock<Vec<Box<StoragePartition>>>,
    insert_window: InsertWindow,
    partition_config: PartitionConfig,
    disk_config: DiskConfig,
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("error validating config")]
    InvalidConfig(#[from] ConfigError),
    #[error("data point inserted outside of storage insert window")]
    OutOfBounds,
    #[error("can't find appropriate partition for data point")]
    PartitionNotFound,
    #[error("no partitions")]
    EmptyPartitionList,
    #[error("error inserting data point into partition")]
    FailedInsert(PartitionError),
    #[error("failed to flush partition")]
    FailedFlush(PartitionError),
    #[error("failed to open partition")]
    FailedOpen(#[from] disk::Error),
    #[error("failed to clean partition")]
    FailedClean(PartitionError),
    #[error("failed to get partition lock")]
    LockFailure,
    #[error("failed to get data from partition")]
    FailedSelect,
}

impl Storage {
    pub fn new(config: Config) -> Result<Self, StorageError> {
        config.validate()?;

        let partitions: Vec<Box<StoragePartition>> = match config.disk.as_ref() {
            Some(disk_config) => open_all(&disk_config.data_path)?,
            None => vec![],
        };

        Ok(Self {
            partitions: RwLock::new(partitions),
            insert_window: InsertWindow::new(config.insert_window),
            partition_config: config.partition,
            disk_config: config.disk.unwrap_or_default(),
        })
    }

    pub fn select(&self, name: &str, start: i64, end: i64) -> Result<Vec<DataPoint>, StorageError> {
        match self.partitions.read() {
            Ok(partitions) => {
                let mut result = vec![];
                for partition in partitions.iter() {
                    let points = &mut partition
                        .select(name, start, end)
                        .map_err(|_| StorageError::FailedSelect)?;
                    result.append(points);
                }
                Ok(result)
            }
            Err(_) => Err(StorageError::LockFailure),
        }
    }

    pub fn insert(&mut self, row: &Row) -> Result<(), StorageError> {
        if !self.insert_window.contains(row.data_point.timestamp) {
            return Err(StorageError::OutOfBounds);
        }
        self.insert_row(row)?;
        self.insert_window.update(row.data_point.timestamp);
        Ok(())
    }

    fn create_partition_with_row(&mut self, row: &Row) -> Result<(), StorageError> {
        match self.partitions.write() {
            Ok(mut partitions) => {
                partitions.push(Box::new(MemoryPartition::new(
                    Some(self.partition_config.duration),
                    row,
                )));
                Ok(())
            }
            Err(e) => Err(StorageError::LockFailure),
        }
    }

    pub fn remove_expired_partitions(&mut self) -> Result<(), StorageError> {
        // Grab the read lock to figure out which partitions are expired.
        let partitions_to_remove: Vec<&Box<StoragePartition>> = match self.partitions.read() {
            Ok(partitions) => {
                let retention_boundary =
                    self.partition_config.max_partitions * self.partition_config.duration;
                let retain_after = match partitions.last() {
                    Some(p) => Ok(p.boundary().max_timestamp() - retention_boundary),
                    None => Err(StorageError::EmptyPartitionList),
                }?;
                let to_remove = vec![];
                partitions.iter().for_each(|p| {
                    to_remove.push(p);
                });
                Ok(to_remove)
            }
            Err(_) => Err(StorageError::LockFailure),
        }?;

        // Flush the partitions without holding onto any lock.
        let partitions_to_swap: Vec<Box<StoragePartition>> =
            Vec::with_capacity(partitions_to_remove.len());
        for p in partitions_to_remove {
            let dir_path = get_dir_path(&self.disk_config.data_path, p.boundary());
            if let Err(e) = p.flush(&dir_path, self.disk_config.encode_strategy) {
                match e {
                    // Unflushable partitions are expected not to be flushed.
                    // This prevents disk partitions from being flushed more than once.
                    PartitionError::Unflushable => {}
                    // TODO:improvement: do we want to bail here? This means one corrupt partition
                    // will prevent all future partitions from ever being flushed. We could instead
                    // flush as much as possible and return the errors at the end.
                    _ => return Err(StorageError::FailedFlush(e)),
                };
            }

            match open(&dir_path) {
                Ok(disk_partition) => {
                    // Swap the partition with a disk partition.
                    let x = Box::new(disk_partition);
                    // partitions_to_swap.push(Box::new(disk_partition));
                }
                Err(e) => {
                    // TODO:improvement: same comment as above about bailing here.
                    return Err(StorageError::FailedOpen(e));
                }
            }
        }

        // Swap the in memory partitions with the flushed partitions.
        //
        // IMPORTANT: We can do this after not having the lock because
        // this is the only place that touches the tail of the partition list.
        // The only other place in the code that modifies the partitions list
        // only adds new partitions to the head and nothing else, so we can
        // safely assume nothing has changed in the list at this point.
        match self.partitions.write() {
            Ok(partitions) => {
                partitions.iter_mut().enumerate().for_each(|(i, p)| {
                    *p = partitions_to_swap[i];
                });
            }
            Err(_) => return Err(StorageError::LockFailure),
        };

        Ok(())
    }

    fn remove_expired_partitions_inner(
        &self,
        partitions: MutexGuard<Vec<Box<StoragePartition>>>,
    ) -> Result<(), StorageError> {
        let retention_boundary =
            self.partition_config.max_partitions * self.partition_config.duration;
        let retain_after = match partitions.last() {
            Some(p) => Ok(p.boundary().max_timestamp() - retention_boundary),
            None => Err(StorageError::EmptyPartitionList),
        }?;

        partitions.retain(|p| {
            if p.boundary().max_timestamp() > retain_after {
                return true;
            }
            // Expired partition, attempt to remove it. Keep the partition only
            // if the clean failed, so we retry later on instead of leaking.
            // TODO: propagate clean failures instead of silently attempting
            // to clean over and over.
            p.clean().is_err()
        });

        Ok(())
    }

    pub fn flush_partitions(&mut self) -> Result<(), StorageError> {
        match self.partitions.lock() {
            Ok(partitions) => self.flush_partitions_inner(partitions),
            Err(_) => todo!(),
        }
    }

    fn flush_partitions_inner(
        &mut self,
        partitions: MutexGuard<Vec<Box<StoragePartition>>>,
    ) -> Result<(), StorageError> {
        partitions
            .iter_mut()
            .rev()
            .enumerate()
            .map(|(i, p)| {
                if i < self.partition_config.hot_partitions {
                    // Hot partitions are not flushed.
                    return Ok(());
                }

                let dir_path = get_dir_path(&self.disk_config.data_path, p.boundary());
                if let Err(e) = p.flush(&dir_path, self.disk_config.encode_strategy) {
                    return match e {
                        // Unflushable partitions are expected not to be flushed.
                        // This prevents disk partitions from being flushed more than once.
                        PartitionError::Unflushable => Ok(()),
                        _ => Err(StorageError::FailedFlush(e)),
                    };
                }
                match open(&dir_path) {
                    Ok(disk_partition) => {
                        // Swap the partition with a disk partition.
                        *p = Box::new(disk_partition);
                        return Ok(());
                    }
                    Err(e) => {
                        return Err(StorageError::FailedOpen(e));
                    }
                }
            })
            .collect()
    }

    fn cascade_past_insert(&self, row: &Row) -> Result<(), StorageError> {
        match self.partitions.split_last() {
            Some((_, past_partitions)) => {
                let mut iter = past_partitions.iter().rev();
                let mut i = 1;
                while i < self.partition_config.hot_partitions {
                    i += 1;

                    match iter.next() {
                        Some(p) => match p.ordering(row) {
                            PointPartitionOrdering::Current => {
                                return p.insert(row).map_err(StorageError::FailedInsert);
                            }
                            PointPartitionOrdering::Future => {
                                return Err(StorageError::PartitionNotFound);
                            }
                            PointPartitionOrdering::Past => continue,
                        },
                        None => return Err(StorageError::OutOfBounds),
                    }
                }
                // Unwritable point. This means insert_window > hot_partitions * partition_duration,
                // in other words, we're accepting points that later cannot be written. This is validated
                // in the config during Storage construction so it's unexpected.
                error!("partition not found for data point {:?}", row);
                return Err(StorageError::PartitionNotFound);
            }
            None => return Err(StorageError::OutOfBounds),
        }
    }

    fn insert_row(&mut self, row: &Row) -> Result<(), StorageError> {
        match self.partitions.last() {
            Some(p) => match p.ordering(row) {
                PointPartitionOrdering::Current => {
                    p.insert(row).map_err(StorageError::FailedInsert)
                }
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

    use crate::{
        metric::{DataPoint, Row},
        storage::{ConfigError, DiskConfig, PartitionConfig, StorageError},
    };

    use super::{Config, Storage};

    #[test]
    fn test_storage_insert_multiple_partitions() {
        let mut storage = Storage::new(Config {
            partition: PartitionConfig {
                duration: 2,
                hot_partitions: 3,
                max_partitions: 3,
            },
            ..Default::default()
        })
        .unwrap();

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
            storage.insert(&Row { metric, data_point }).unwrap();
        }
        assert_eq!(storage.partitions.len(), 3);

        let result = storage.select(&metric.to_string(), 0, 7).unwrap();
        assert_eq!(result, data_points);
    }

    #[test]
    fn test_storage_insert_window() {
        let mut storage = Storage::new(Config {
            partition: PartitionConfig {
                duration: 100,
                hot_partitions: 2,
                max_partitions: 2,
            },
            insert_window: 5,
            ..Default::default()
        })
        .unwrap();

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

        match data_points.split_last() {
            Some((out_of_bounds, elements)) => {
                for data_point in elements {
                    storage
                        .insert(&Row {
                            metric,
                            data_point: *data_point,
                        })
                        .unwrap();
                }
                assert!(matches!(
                    storage
                        .insert(&Row {
                            metric,
                            data_point: *out_of_bounds
                        })
                        .err(),
                    Some(StorageError::OutOfBounds)
                ));
            }
            None => unreachable!(),
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
            partition: PartitionConfig {
                duration: 100,
                hot_partitions: 2,
                max_partitions: 2,
            },
            insert_window: 5,
            ..Default::default()
        })
        .unwrap();

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

        match data_points.split_first() {
            Some((in_bounds, out_of_bounds)) => {
                storage
                    .insert(&Row {
                        metric,
                        data_point: *in_bounds,
                    })
                    .unwrap();
                assert!(matches!(
                    storage
                        .insert(&Row {
                            metric,
                            data_point: out_of_bounds[0]
                        })
                        .err(),
                    Some(StorageError::OutOfBounds),
                ));
            }
            None => unreachable!(),
        }

        let result = storage.select(&metric.to_string(), 0, 20).unwrap();
        assert_eq!(result, vec![data_points[0]]);
    }

    #[test]
    fn test_storage_insert_window_across_partitions() {
        let mut storage = Storage::new(Config {
            partition: PartitionConfig {
                duration: 2,
                hot_partitions: 2,
                max_partitions: 2,
            },
            insert_window: 2,
            ..Default::default()
        })
        .unwrap();

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
            storage.insert(&Row { metric, data_point }).unwrap();
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
            partition: PartitionConfig {
                duration: 10,
                hot_partitions: 2,
                max_partitions: 10,
            },
            disk: Some(DiskConfig {
                data_path: data_path.clone(),
                ..Default::default()
            }),
            insert_window: 20,
            ..Default::default()
        })
        .unwrap();

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
            storage.insert(&Row { metric, data_point }).unwrap();
        }

        let result = storage.select(&metric.to_string(), 0, 40).unwrap();

        let expected = data_points.clone();
        assert_eq!(result, expected);

        fs::remove_dir_all(data_path).unwrap();
    }

    #[test]
    fn test_storage_retention() {
        let mut storage = Storage::new(Config {
            partition: PartitionConfig {
                duration: 10,
                hot_partitions: 1,
                max_partitions: 1,
            },
            ..Default::default()
        })
        .unwrap();

        let metric = "hello";
        let data_points = [
            DataPoint {
                timestamp: 10,
                value: 1.0,
            },
            DataPoint {
                timestamp: 20, // start of 2nd partition, 1st should be removed at this point
                value: 2.0,
            },
        ];

        storage
            .insert(&Row {
                metric,
                data_point: data_points[0],
            })
            .unwrap();
        let result = storage.select(&metric.to_string(), 0, 50).unwrap();
        assert_eq!(result, vec![data_points[0]]);

        storage
            .insert(&Row {
                metric,
                data_point: data_points[1],
            })
            .unwrap();
        let result = storage.select(&metric.to_string(), 0, 50).unwrap();
        assert_eq!(result, vec![data_points[1]]);
    }

    #[test]
    fn test_storage_invalid_config() {
        let storage = Storage::new(Config {
            partition: PartitionConfig {
                duration: 10,
                hot_partitions: 2,
                max_partitions: 10,
            },
            disk: Some(DiskConfig {
                data_path: String::from(""),
                ..Default::default()
            }),
            insert_window: 100,
            ..Default::default()
        });
        assert!(storage.is_err());
    }

    #[test]
    fn test_invalid_config_no_hot_partitions() {
        let config = Config {
            partition: PartitionConfig {
                duration: 10,
                hot_partitions: 0,
                max_partitions: 0,
            },
            insert_window: 0,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::HotPartitionsError)
        ));
    }

    #[test]
    fn test_invalid_config_writable_window() {
        let config = Config {
            partition: PartitionConfig {
                duration: 10,
                hot_partitions: 10,
                max_partitions: 10,
            },
            insert_window: 200,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InsertWindowError)
        ));
    }

    #[test]
    fn test_invalid_config_disk() {
        let config = Config {
            partition: PartitionConfig {
                duration: 10,
                hot_partitions: 9,
                max_partitions: 10,
            },
            insert_window: 20,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::DiskConfigError)
        ));
    }

    #[test]
    fn test_storage_docs_example() {
        let mut storage = Storage::new(Config {
            partition: PartitionConfig {
                duration: 100,
                hot_partitions: 2,
                max_partitions: 2,
            },
            ..Default::default()
        })
        .unwrap();

        storage
            .insert(&Row {
                metric: "metric1",
                data_point: DataPoint {
                    timestamp: 1600000000,
                    value: 0.1,
                },
            })
            .unwrap();

        let points = storage.select("metric1", 1600000000, 1600000001).unwrap();
        for p in points {
            println!("timestamp: {}, value: {}", p.timestamp, p.value);
            // => timestamp: 1600000000, value: 0.1
        }
    }

    #[test]
    fn test_storage_with_existing_partitions() {
        let storage = Storage::new(Config {
            partition: PartitionConfig {
                duration: 10,
                hot_partitions: 2,
                max_partitions: 10,
            },
            disk: Some(DiskConfig {
                data_path: String::from("tests/fixtures/test_open_existing_partitions"),
                ..Default::default()
            }),
            insert_window: 20,
            ..Default::default()
        })
        .unwrap();

        assert_eq!(storage.partitions.len(), 2);

        let result = storage.select("metric1", 0, 300).unwrap();
        assert_eq!(result.len(), 6);
    }
}
