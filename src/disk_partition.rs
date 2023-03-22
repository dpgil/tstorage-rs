use std::{
    collections::HashMap,
    fs::File,
    io::{self, Seek},
    path::Path,
};

use memmap::Mmap;
use thiserror::Error;

const DATA_FILE_NAME: &str = "data";
const META_FILE_NAME: &str = "meta.json";

#[derive(Error, Debug)]
pub enum Error {
    #[error("empty directory path")]
    EmptyDirPathError,
    #[error("error opening file")]
    FileError(#[from] io::Error),
    #[error("not data points in data file")]
    NoDataPointsError,
}

struct MetricMetadata {
    name: String,
    offset: i64,
    min_timestamp: i64,
    max_timestamp: i64,
    num_data_points: i64,
}

struct PartitionMetadata {
    min_timestamp: i64,
    max_timestamp: i64,
    num_data_points: i64,
    metrics: HashMap<String, MetricMetadata>,
    created_at: i64, // TODO:minor: time.Time
}

// Partition that uses local disk as storage.
// Metadata is split into two files, the `data` file and the `meta` file.
struct DiskPartition {
    metadata: PartitionMetadata,
    mapped_file: Mmap,
}

impl DiskPartition {}

fn open_disk_partition(dir_path: &str, _retention: i64) -> Result<DiskPartition, Error> {
    if dir_path.is_empty() {
        return Err(Error::EmptyDirPathError);
    }

    let meta_file_path = Path::new(dir_path).join(META_FILE_NAME);
    let meta = File::open(meta_file_path)?;

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
        metadata: PartitionMetadata {
            min_timestamp: 1,
            max_timestamp: 2,
            num_data_points: 3,
            metrics: HashMap::default(),
            created_at: 5,
        },
        mapped_file: mmap,
    })
}
