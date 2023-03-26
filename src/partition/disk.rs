use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufReader},
    path::Path,
};

use anyhow::Result;
use memmap::Mmap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    encode::encode::{get_decoder, Decoder, EncodeStrategy},
    metric::DataPoint,
};

pub const DATA_FILE_NAME: &str = "data";
pub const META_FILE_NAME: &str = "meta.json";

#[derive(Error, Debug)]
pub enum Error {
    #[error("empty directory path")]
    EmptyDirPathError,
    #[error("error opening file")]
    FileError(#[from] io::Error),
    #[error("not data points in data file")]
    NoDataPointsError,
    #[error("error unmarshaling meta.json")]
    UnmarshalMetaFileError(#[from] serde_json::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetricMetadata {
    pub name: String,
    pub start_offset: usize,
    pub end_offset: usize,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub num_data_points: usize,
}

#[derive(Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub num_data_points: usize,
    pub metrics: HashMap<String, MetricMetadata>,
    pub created_at: i64, // TODO:minor: time.Time
    pub encode_strategy: EncodeStrategy,
}

// Partition that uses local disk as storage.
// Metadata is split into two files, the `data` file and the `meta` file.
pub struct DiskPartition {
    metadata: PartitionMetadata,
    mapped_file: Mmap,
}

impl DiskPartition {
    pub fn select(&self, name: &str, start: i64, end: i64) -> Result<Vec<DataPoint>> {
        let meta = self
            .metadata
            .metrics
            .get(name)
            .ok_or(Error::NoDataPointsError)?;

        let mut decoder = get_decoder(
            self.metadata.encode_strategy,
            &self.mapped_file[meta.start_offset..meta.end_offset],
        );

        let mut points: Vec<DataPoint> = Vec::with_capacity(meta.num_data_points);
        for _ in 0..meta.num_data_points {
            let data_point = decoder.decode_point()?;
            if data_point.timestamp < start {
                continue;
            }
            if data_point.timestamp >= end {
                break;
            }
            points.push(data_point);
        }
        Ok(points)
    }
}

pub fn open(dir_path: &str) -> Result<DiskPartition, Error> {
    if dir_path.is_empty() {
        return Err(Error::EmptyDirPathError);
    }

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
    })
}

#[cfg(test)]
pub mod tests {
    use crate::metric::DataPoint;

    use super::open;

    #[test]
    fn test_open() {
        let partition = open("tests/fixtures/test_csv_disk_partition").unwrap();
        assert_eq!(partition.metadata.num_data_points, 6);
        assert_eq!(partition.metadata.min_timestamp, 10);
        assert_eq!(partition.metadata.max_timestamp, 110);
    }

    #[test]
    fn test_select() {
        let partition = open("tests/fixtures/test_csv_disk_partition").unwrap();
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
}
