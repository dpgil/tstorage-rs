use crate::{
    metric::{DataPoint, Row},
    partition::{
        disk::{self, open, open_all, DiskPartition},
        memory::MemoryPartition,
        Boundary, Partition, PartitionError, PointPartitionOrdering,
    },
    window::{Bounds, InsertWindow},
    EncodeStrategy,
};
use anyhow::Result;
use log::error;
use std::{
    num::TryFromIntError,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
    time::Duration,
};
use thiserror::Error;

#[derive(Default)]
pub struct Config {
    pub partition: PartitionConfig,
    pub disk: Option<DiskConfig>,
    // Insert window relative to the maximum timestamp in the database.
    pub insert_bounds: Option<Bounds>,
    pub sweep_interval: Option<u64>,
}

#[derive(Default)]
pub struct PartitionConfig {
    // Number of partitions to keep in memory, remaining will be flushed to disk.
    pub hot_partitions: i64,
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
    #[error("hot_partitions is greater than max_partitions")]
    NumPartitionsError,
    #[error("error converting hot_partitions to i64")]
    HotPartitionsFormatError(TryFromIntError),
    #[error("insert window is larger than writable window")]
    InsertWindowError,
    #[error("hot partitions must be greater than zero to support writing data")]
    HotPartitionsError,
    #[error("disk config required when hot_partitions != max_partitions")]
    DiskConfigError,
    #[error("error converting writable_window to u64")]
    WritableWindowFormatError(TryFromIntError),
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.partition.hot_partitions == 0 {
            return Err(ConfigError::HotPartitionsError);
        }
        if self.partition.hot_partitions > self.partition.max_partitions {
            return Err(ConfigError::NumPartitionsError);
        }
        if self.partition.hot_partitions < self.partition.max_partitions && self.disk.is_none() {
            return Err(ConfigError::DiskConfigError);
        }
        if let Some(insert_bounds) = &self.insert_bounds {
            if let Some(past) = insert_bounds.past {
                let writable_window: u64 = (self.partition.hot_partitions
                    * self.partition.duration)
                    .try_into()
                    .map_err(|e| ConfigError::WritableWindowFormatError(e))?;
                if past > writable_window {
                    return Err(ConfigError::InsertWindowError);
                }
            }
        }
        Ok(())
    }
}

pub type StoragePartition = dyn Partition + Send + Sync;
pub type PartitionList = Vec<Box<StoragePartition>>;

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

pub struct Storage {
    inner: Arc<StorageInner>,
    shutdown: Arc<AtomicBool>,
}

impl Storage {
    pub fn new(config: Config) -> Result<Self, StorageError> {
        let osweep_interval = config.sweep_interval;

        let storage = StorageInner::new(config)?;
        let inner = Arc::new(storage);
        let shutdown = Arc::new(AtomicBool::new(false));

        if let Some(sweep_interval) = osweep_interval {
            let inner_clone = inner.clone();
            let shutdown_clone = shutdown.clone();
            std::thread::spawn(move || {
                loop {
                    if shutdown_clone.load(Ordering::SeqCst) {
                        break;
                    }
                    std::thread::sleep(Duration::from_secs(sweep_interval));
                    // TODO: add timeouts
                    if let Err(e) = inner_clone.remove_expired_partitions() {
                        error!("error removing expired partitions: {}", e);
                    }
                    if let Err(e) = inner_clone.flush_partitions() {
                        error!("error flushing partitions: {}", e);
                    }
                }
            });
        }

        Ok(Self { inner, shutdown })
    }

    pub fn insert(&self, row: &Row) -> Result<(), StorageError> {
        self.inner.insert(row)
    }

    pub fn select(&self, name: &str, start: i64, end: i64) -> Result<Vec<DataPoint>, StorageError> {
        self.inner.select(name, start, end)
    }

    pub fn close(&self) -> Result<(), StorageError> {
        self.shutdown.store(true, Ordering::SeqCst);
        self.inner.remove_expired_partitions()?;
        // TODO: flush everything
        self.inner.flush_partitions()
    }
}

struct StorageInner {
    partitions: RwLock<PartitionList>,
    insert_window: InsertWindow,
    partition_config: PartitionConfig,
    disk_config: DiskConfig,
}

impl StorageInner {
    pub fn new(config: Config) -> Result<Self, StorageError> {
        config.validate()?;

        let partitions: PartitionList = match config.disk.as_ref() {
            Some(disk_config) => open_all(&disk_config.data_path)?,
            None => vec![],
        };

        let default_past: u64 = (config.partition.hot_partitions * config.partition.duration)
            .try_into()
            .map_err(|e| ConfigError::WritableWindowFormatError(e))?;
        let insert_bounds = match config.insert_bounds {
            Some(bounds) => Bounds {
                past: Some(bounds.past.unwrap_or(default_past)),
                future: bounds.future,
            },
            None => Bounds {
                past: Some(default_past),
                future: None,
            },
        };

        Ok(Self {
            partitions: RwLock::new(partitions),
            insert_window: InsertWindow::new(insert_bounds),
            partition_config: config.partition,
            disk_config: config.disk.unwrap_or_default(),
        })
    }

    pub fn select(&self, name: &str, start: i64, end: i64) -> Result<Vec<DataPoint>, StorageError> {
        match self.partitions.read() {
            Ok(partitions) => self.select_inner(partitions, name, start, end),
            Err(_) => Err(StorageError::LockFailure),
        }
    }

    fn select_inner(
        &self,
        partitions: RwLockReadGuard<PartitionList>,
        name: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>, StorageError> {
        let mut result = vec![];
        for (i, partition) in partitions.iter().enumerate() {
            // TODO: gracefully handle unwrap
            if (partitions.len() - i) > self.partition_config.max_partitions.try_into().unwrap() {
                // Ignore expired partitions.
                continue;
            }
            let points = &mut partition
                .select(name, start, end)
                .map_err(|_| StorageError::FailedSelect)?;
            result.append(points);
        }
        Ok(result)
    }

    pub fn insert(&self, row: &Row) -> Result<(), StorageError> {
        if !self.insert_window.contains(row.data_point.timestamp) {
            return Err(StorageError::OutOfBounds);
        }
        self.insert_row(row)?;
        self.insert_window.update(row.data_point.timestamp);
        Ok(())
    }

    fn insert_row(&self, row: &Row) -> Result<(), StorageError> {
        match self.partitions.write().as_mut() {
            Ok(partitions) => self.insert_row_inner(partitions, row),
            Err(_) => Err(StorageError::LockFailure),
        }
    }

    fn insert_row_inner(
        &self,
        partitions: &mut RwLockWriteGuard<PartitionList>,
        row: &Row,
    ) -> Result<(), StorageError> {
        match partitions.last() {
            Some(p) => match p.ordering(row) {
                PointPartitionOrdering::Current => {
                    p.insert(row).map_err(StorageError::FailedInsert)
                }
                PointPartitionOrdering::Future => self.create_partition_with_row(partitions, row),
                PointPartitionOrdering::Past => self.cascade_past_insert(row, partitions),
            },
            None => self.create_partition_with_row(partitions, row),
        }
    }

    fn create_partition_with_row(
        &self,
        partitions: &mut RwLockWriteGuard<PartitionList>,
        row: &Row,
    ) -> Result<(), StorageError> {
        let mut min_timestamp = match partitions.split_last() {
            Some((last, _)) => last.boundary().max_timestamp(),
            None => row.data_point.timestamp,
        };
        loop {
            let partition = MemoryPartition::new(self.partition_config.duration, min_timestamp);
            min_timestamp = partition.boundary().max_timestamp();
            match partition.ordering(row) {
                // Keep generating partitions until one fits this point.
                PointPartitionOrdering::Future => partitions.push(Box::new(partition)),
                PointPartitionOrdering::Current => {
                    partition
                        .insert(row)
                        .map_err(|e| StorageError::FailedInsert(e))?;
                    partitions.push(Box::new(partition));
                    return Ok(());
                }
                PointPartitionOrdering::Past => return Err(StorageError::PartitionNotFound),
            }
        }
    }

    fn cascade_past_insert(
        &self,
        row: &Row,
        partitions: &mut RwLockWriteGuard<PartitionList>,
    ) -> Result<(), StorageError> {
        match partitions.split_last() {
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

    pub fn flush_partitions(&self) -> Result<(), StorageError> {
        // Grab the read lock to figure out which partitions need to be flushed.
        let partitions_to_swap: Vec<Option<Box<DiskPartition>>> = match self.partitions.read() {
            Ok(partitions) => {
                Ok(partitions
                    .iter()
                    .enumerate()
                    .map(|(i, p)| {
                        // TODO: gracefully handle unwrap
                        if (partitions.len() - i)
                            <= self.partition_config.hot_partitions.try_into().unwrap()
                        {
                            // Hot partitions are not flushed.
                            return None;
                        }
                        let dir_path = get_dir_path(&self.disk_config.data_path, p.boundary());
                        if let Err(_) = p.flush(&dir_path, self.disk_config.encode_strategy) {
                            // TODO:improvement: handle flush problems here, log them at the very least.
                            // If partitions repeatedly have issues flushing, we'll take up more and more
                            // memory with partitions that are supposed to be on disk.
                            // Make sure to ignore "Unflushable" errors, as those typically mean the partition
                            // is already flushed.
                            return None;
                        }
                        match open(&dir_path) {
                            Ok(disk_partition) => Some(Box::new(disk_partition)),
                            // TODO:improvement: same note about flush issues here. If we don't handle this error,
                            // we could possibly be re-flushing the same partition multiple times.
                            Err(_) => None,
                        }
                    })
                    .collect())
            }
            Err(_) => Err(StorageError::LockFailure),
        }?;

        // Swap the in memory partitions with the flushed partitions.
        //
        // IMPORTANT: We can do this after not having the lock because
        // this is the only place that touches the tail of the partition list.
        // The only other place in the code that modifies the partitions list
        // only adds new partitions to the head and nothing else, so we can
        // safely assume nothing has changed in the list at this point.
        match self.partitions.write().as_mut() {
            Ok(partitions) => {
                partitions_to_swap
                    .into_iter()
                    .enumerate()
                    .for_each(|(i, p)| match p {
                        Some(p) => partitions[i] = p,
                        None => {}
                    });
            }
            Err(_) => return Err(StorageError::LockFailure),
        };

        Ok(())
    }

    pub fn remove_expired_partitions(&self) -> Result<(), StorageError> {
        // Grab the read lock to figure out which partitions are expired.
        let remove_result: Vec<Option<Result<(), PartitionError>>> = match self.partitions.read() {
            Ok(partitions) => {
                // TODO: handle error
                let max_partitions: i64 = self.partition_config.max_partitions.try_into().unwrap();
                let retention_boundary = max_partitions * self.partition_config.duration;
                let remove_before = match partitions.last() {
                    Some(p) => Ok(p.boundary().max_timestamp() - retention_boundary),
                    None => Err(StorageError::EmptyPartitionList),
                }?;
                Ok(partitions
                    .iter()
                    .map(|p| {
                        if p.boundary().max_timestamp() <= remove_before {
                            Some(p.clean())
                        } else {
                            // Ignore partitions that aren't expired.
                            None
                        }
                    })
                    .collect())
            }
            Err(_) => Err(StorageError::LockFailure),
        }?;

        // Remove all the successfully cleaned partitions from the partition list.
        match self.partitions.write() {
            Ok(mut partitions) => {
                let new_partitions: PartitionList = std::mem::take(&mut *partitions)
                    .into_iter()
                    .enumerate()
                    .map(|(i, p)| {
                        match remove_result.get(i) {
                            // TODO: flatten this
                            Some(result) => match result {
                                Some(result) => match result {
                                    Ok(_) => None,     // successfully removed
                                    Err(_) => Some(p), // failed to be removed, keep it to try again
                                },
                                None => Some(p), // wasn't considered to be removed
                            },
                            None => Some(p), // wasn't considered to be removed
                        }
                    })
                    .filter(|p| p.is_some())
                    .map(|p| p.unwrap())
                    .collect();
                *partitions = new_partitions;
                Ok(())
            }
            Err(_) => return Err(StorageError::LockFailure),
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
        window::Bounds,
    };

    use super::{Config, StorageInner};

    #[test]
    fn test_storage_insert_multiple_partitions() {
        let storage = StorageInner::new(Config {
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
        assert_eq!(storage.partitions.read().unwrap().len(), 3);

        let result = storage.select(&metric.to_string(), 0, 7).unwrap();
        assert_eq!(result, data_points);
    }

    #[test]
    fn test_storage_insert_window() {
        let storage = StorageInner::new(Config {
            partition: PartitionConfig {
                duration: 100,
                hot_partitions: 2,
                max_partitions: 2,
            },
            insert_bounds: Some(Bounds {
                past: Some(5),
                future: None,
            }),
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
    fn test_storage_insert_window_future() {
        let storage = StorageInner::new(Config {
            partition: PartitionConfig {
                duration: 100,
                hot_partitions: 2,
                max_partitions: 2,
            },
            insert_bounds: Some(Bounds {
                past: None,
                future: Some(10),
            }),
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
                timestamp: 31, // Outside of future bounds (31-20)>=10
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
    }

    #[test]
    // The first timestamp sets the base minimum timestamp for the storage.
    // Not my favorite solution but before we have more partitions, we drop
    // any delayed timestamps.
    fn test_storage_martian_point() {
        let storage = StorageInner::new(Config {
            partition: PartitionConfig {
                duration: 100,
                hot_partitions: 2,
                max_partitions: 2,
            },
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
        let storage = StorageInner::new(Config {
            partition: PartitionConfig {
                duration: 2,
                hot_partitions: 2,
                max_partitions: 2,
            },
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
        let storage = StorageInner::new(Config {
            partition: PartitionConfig {
                duration: 10,
                hot_partitions: 2,
                max_partitions: 10,
            },
            disk: Some(DiskConfig {
                data_path: data_path.clone(),
                ..Default::default()
            }),
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
                timestamp: 20, // start of 3rd partition
                value: 2.0,
            },
            DataPoint {
                timestamp: 30, // start of 4th partition
                value: 3.0,
            },
        ];

        for data_point in data_points {
            storage.insert(&Row { metric, data_point }).unwrap();
        }

        storage.flush_partitions().unwrap();
        let result = storage.select(&metric.to_string(), 0, 40).unwrap();

        let expected = data_points.clone();
        assert_eq!(result, expected);

        fs::remove_dir_all(data_path).unwrap();
    }

    #[test]
    fn test_storage_retention() {
        let storage = StorageInner::new(Config {
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
                timestamp: 20, // start of 2nd partition, 1st will be marked as expired
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

        assert_eq!(2, storage.partitions.read().unwrap().len());
        storage.remove_expired_partitions().unwrap();
        assert_eq!(1, storage.partitions.read().unwrap().len());

        // Ensure results are unchanged
        let result = storage.select(&metric.to_string(), 0, 50).unwrap();
        assert_eq!(result, vec![data_points[1]]);
    }

    #[test]
    fn test_storage_partition_bounds() {
        let storage = StorageInner::new(Config {
            partition: PartitionConfig {
                duration: 10,
                hot_partitions: 3,
                max_partitions: 3,
            },
            ..Default::default()
        })
        .unwrap();

        let metric = "hello";
        let data_points = [
            DataPoint {
                timestamp: 0, // start of 1st partition
                value: 0.0,
            },
            DataPoint {
                timestamp: 12, // start of 2nd partition
                value: 0.0,
            },
            DataPoint {
                timestamp: 10, // within insert window, but inserted "in between" partitions
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
    fn test_storage_invalid_config() {
        let storage = StorageInner::new(Config {
            partition: PartitionConfig {
                duration: 10,
                hot_partitions: 2,
                max_partitions: 10,
            },
            disk: Some(DiskConfig {
                data_path: String::from(""),
                ..Default::default()
            }),
            insert_bounds: Some(Bounds {
                past: Some(100),
                future: None,
            }),
            ..Default::default()
        });
        assert!(storage.is_err());
    }

    #[test]
    fn test_storage_skip_partitions() {
        let storage = StorageInner::new(Config {
            partition: PartitionConfig {
                duration: 10,
                hot_partitions: 3,
                max_partitions: 3,
            },
            ..Default::default()
        })
        .unwrap();

        let metric = "hello";
        let data_points = [
            DataPoint {
                timestamp: 0, // start of 1st partition
                value: 0.0,
            },
            DataPoint {
                timestamp: 10, // start of 2nd partition
                value: 0.0,
            },
            DataPoint {
                timestamp: 90, // start of 10th partition, should trigger a bunch of partitions in between
                value: 1.0,
            },
        ];

        for data_point in data_points {
            storage.insert(&Row { metric, data_point }).unwrap();
        }
        assert_eq!(storage.partitions.read().unwrap().len(), 10);

        // select ignores expired partitions
        let result = storage.select("hello", 0, 100).unwrap();
        assert_eq!(result, vec![data_points[2]]);

        storage.remove_expired_partitions().unwrap();
        assert_eq!(storage.partitions.read().unwrap().len(), 3);
    }

    #[test]
    fn test_invalid_config_no_hot_partitions() {
        let config = Config {
            partition: PartitionConfig {
                duration: 10,
                hot_partitions: 0,
                max_partitions: 0,
            },
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
            insert_bounds: Some(Bounds {
                past: Some(200),
                future: None,
            }),
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
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::DiskConfigError)
        ));
    }

    #[test]
    fn test_storage_docs_example() {
        let storage = StorageInner::new(Config {
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
        let storage = StorageInner::new(Config {
            partition: PartitionConfig {
                duration: 10,
                hot_partitions: 2,
                max_partitions: 10,
            },
            disk: Some(DiskConfig {
                data_path: String::from("tests/fixtures/test_open_existing_partitions"),
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();

        assert_eq!(storage.partitions.read().unwrap().len(), 2);

        let result = storage.select("metric1", 0, 300).unwrap();
        assert_eq!(result.len(), 6);
    }
}
