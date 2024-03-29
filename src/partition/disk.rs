use anyhow::Result;
use chrono::{serde::ts_seconds, DateTime, Utc};
use memmap::Mmap;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Seek, Write},
    path::{Path, PathBuf},
};
use thiserror::Error;

use crate::{
    encode::{decode_points, encode_points, EncodeStrategy},
    metric::DataPoint,
    storage::PartitionList,
    Row,
};

use super::{memory::MemoryPartition, Boundary, Partition, PartitionError};

pub const DATA_FILE_NAME: &str = "data";
pub const META_FILE_NAME: &str = "meta.json";

#[derive(Error, Debug)]
pub enum Error {
    #[error("error opening file")]
    FileError(#[from] io::Error),
    #[error("no data points in data file")]
    NoDataPointsError,
    #[error("error unmarshaling meta.json")]
    UnmarshalMetaFileError(#[from] serde_json::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetricMetadata {
    pub name: String,
    pub start_offset: usize,
    pub end_offset: usize,
    pub boundary: Boundary,
    pub num_data_points: usize,
}

#[derive(Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub boundary: Boundary,
    pub num_data_points: usize,
    pub metrics: HashMap<String, MetricMetadata>,
    #[serde(with = "ts_seconds")]
    pub created_at: DateTime<Utc>,
    pub encode_strategy: EncodeStrategy,
}

// Partition that uses local disk as storage.
// Metadata is split into two files, the `data` file and the `meta` file.
pub struct DiskPartition {
    metadata: PartitionMetadata,
    mapped_file: Mmap,
    dir_path: PathBuf,
}

impl Partition for DiskPartition {
    fn select(&self, name: &str, start: i64, end: i64) -> Result<Vec<DataPoint>> {
        let meta = match self.metadata.metrics.get(name) {
            Some(meta) => meta,
            None => return Ok(vec![]),
        };

        let mut points: Vec<DataPoint> = decode_points(
            &self.mapped_file[meta.start_offset..meta.end_offset],
            meta.num_data_points,
            self.metadata.encode_strategy,
        )?;
        points.retain(|dp| dp.timestamp >= start && dp.timestamp < end);
        Ok(points)
    }

    fn insert(&self, _: &Row) -> Result<(), PartitionError> {
        Err(PartitionError::Unwritable)
    }

    fn ordering(&self, row: &Row) -> super::PointPartitionOrdering {
        self.boundary().ordering(row.data_point.timestamp)
    }

    fn flush(
        &self,
        _dir_path: &Path,
        _encode_strategy: EncodeStrategy,
    ) -> Result<(), PartitionError> {
        Err(PartitionError::Unflushable)
    }

    fn boundary(&self) -> Boundary {
        self.metadata.boundary
    }

    fn clean(&self) -> Result<(), PartitionError> {
        fs::remove_dir_all(&self.dir_path).map_err(|_| PartitionError::Clean)
    }
}

pub fn open(dir_path: &Path) -> Result<DiskPartition, Error> {
    let meta_file_path = Path::new(dir_path).join(META_FILE_NAME);
    let meta_file = File::open(meta_file_path)?;
    let meta: PartitionMetadata = serde_json::from_reader(BufReader::new(meta_file))?;

    let data_file_path = Path::new(dir_path).join(DATA_FILE_NAME);
    let data = File::open(data_file_path)?;
    let data_info = data.metadata()?;
    if data_info.len() == 0 {
        return Err(Error::NoDataPointsError);
    }

    // N.B. has to be unsafe because the memory mapped file contents can change
    // underneath the running program, and modifying an immutable variable
    // can lead to undefined behavior.
    let mmap = unsafe { memmap::Mmap::map(&data)? };

    Ok(DiskPartition {
        metadata: meta,
        mapped_file: mmap,
        dir_path: dir_path.to_path_buf(),
    })
}

pub fn open_all(dir_path: &str) -> Result<PartitionList, Error> {
    let mut partitions: PartitionList = vec![];
    fs::create_dir_all(dir_path).map_err(|e| Error::FileError(e))?;
    let paths = fs::read_dir(dir_path).map_err(|e| Error::FileError(e))?;
    for path in paths {
        let dir_entry = match path {
            Ok(p) => Ok(p),
            Err(e) => Err(Error::FileError(e)),
        }?;
        let disk_partition = open(&dir_entry.path())?;
        partitions.push(Box::new(disk_partition));
    }
    Ok(partitions)
}

pub fn flush(
    partition: &MemoryPartition,
    dir_path: &Path,
    encode_strategy: EncodeStrategy,
) -> Result<()> {
    fs::create_dir_all(dir_path.clone())?;

    let data_file_path = Path::new(&dir_path).join(DATA_FILE_NAME);
    let data = File::create(data_file_path)?;
    let mut writer = BufWriter::new(data);
    let mut metrics = HashMap::<String, MetricMetadata>::new();
    let mut total_data_points = 0;
    let min_timestamp = partition.boundary().min_timestamp();
    let max_timestamp = partition.boundary().max_timestamp();
    for x in partition.map.iter() {
        let (name, metric_entry) = x.pair();
        // Find the current offset in the file, since we don't know how much
        // the encoder moved the pointer.
        let start_offset = writer.seek(std::io::SeekFrom::Current(0)).unwrap();
        encode_points(&mut writer, &metric_entry.data_points, encode_strategy)?;
        let end_offset = writer.seek(std::io::SeekFrom::Current(0)).unwrap();
        let num_data_points = metric_entry.data_points.len();
        total_data_points += num_data_points;
        metrics.insert(
            name.clone(),
            MetricMetadata {
                name: name.clone(),
                start_offset: start_offset.try_into().unwrap(),
                end_offset: end_offset.try_into().unwrap(),
                boundary: Boundary {
                    min_timestamp: metric_entry.min_timestamp(),
                    max_timestamp: metric_entry.max_timestamp(),
                },
                num_data_points,
            },
        );
    }
    writer.flush()?;

    let partition_metadata = PartitionMetadata {
        boundary: Boundary {
            min_timestamp,
            max_timestamp,
        },
        num_data_points: total_data_points,
        metrics,
        created_at: Utc::now(),
        encode_strategy,
    };
    let meta_string = serde_json::to_string(&partition_metadata)?;
    let meta_file_path = Path::new(&dir_path).join(META_FILE_NAME);
    let mut meta = File::create(meta_file_path)?;
    meta.write_all(meta_string.as_bytes())?;
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use std::{fs, path::Path};

    use crate::{
        encode::EncodeStrategy,
        metric::{DataPoint, Row},
        partition::{
            disk::{flush, PartitionMetadata, DATA_FILE_NAME, META_FILE_NAME},
            memory::MemoryPartition,
            Partition,
        },
        storage::get_dir_path,
    };

    use super::open;

    #[test]
    fn test_open() {
        let dir_path = Path::new("tests/fixtures/test_csv_disk_partition");
        let partition = open(dir_path).unwrap();
        assert_eq!(partition.metadata.num_data_points, 6);
        assert_eq!(partition.metadata.boundary.min_timestamp, 10);
        assert_eq!(partition.metadata.boundary.max_timestamp, 110);
    }

    #[test]
    fn test_select() {
        let dir_path = Path::new("tests/fixtures/test_csv_disk_partition");
        let partition = open(dir_path).unwrap();
        assert_eq!(
            partition.select("hello", 11, 100).unwrap(),
            vec![
                DataPoint {
                    timestamp: 15,
                    value: 0.052
                },
                DataPoint {
                    timestamp: 20,
                    value: 1.0
                }
            ]
        );
    }

    #[test]
    fn test_select_no_data_points() {
        let dir_path = Path::new("tests/fixtures/test_csv_disk_partition");
        let partition = open(dir_path).unwrap();
        assert_eq!(partition.select("random_metric", 11, 100).unwrap(), vec![]);
    }

    #[test]
    fn test_flush() {
        let metric = "hello";
        let rows = [
            DataPoint {
                timestamp: 10,
                value: 0.0,
            },
            DataPoint {
                timestamp: 15,
                value: 0.052,
            },
            DataPoint {
                timestamp: 20,
                value: 1.0,
            },
        ]
        .map(|d| Row {
            metric,
            data_point: d,
        });
        let partition = MemoryPartition::new(100, rows[0].data_point.timestamp);
        for row in rows {
            partition.insert(&row).unwrap();
        }

        let data_path = "./test_flush_data";
        let dir_path = get_dir_path(data_path, partition.boundary());
        flush(&partition, &dir_path, EncodeStrategy::CSV).unwrap();

        let data = fs::read_to_string(dir_path.join(DATA_FILE_NAME)).unwrap();
        assert_eq!(data, String::from("10,0\n15,0.052\n20,1\n"));

        let meta = fs::read_to_string(dir_path.join(META_FILE_NAME)).unwrap();
        let meta_obj: PartitionMetadata = serde_json::from_str(&meta).unwrap();
        assert_eq!(meta_obj.boundary.min_timestamp, 10);
        assert_eq!(meta_obj.boundary.max_timestamp, 110);
        assert_eq!(meta_obj.num_data_points, 3);

        fs::remove_dir_all(data_path).unwrap();
    }
}
