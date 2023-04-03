use std::path::Path;

use crate::{DataPoint, Row, EncodeStrategy};
use anyhow::Result;

pub mod disk;
pub mod memory;

pub trait Partition {
    fn select(&self, name: &str, start: i64, end: i64) -> Result<Vec<DataPoint>>;
    fn insert(&self, row: &Row);
    fn ordering(&self, row: &Row) -> PointPartitionOrdering;
    fn flush(&self, dir_path: &Path, encode_strategy: EncodeStrategy) -> Result<()>;
    fn boundary(&self) -> Boundary;
}

// Terrible naming, but this represents whether a point belongs in
// a particular parition, or it belongs in a later one (future) or
// an earlier one (past).
pub enum PointPartitionOrdering {
    Current,
    Future,
    Past,
}

#[derive(Debug, Clone, Copy)]
pub struct Boundary {
    min_timestamp: i64,
    max_timestamp: i64, // exclusive
}

impl Boundary {
    fn contains_range(&self, start: i64, end: i64) -> bool {
        !(start >= self.max_timestamp || end < self.min_timestamp)
    }

    fn contains_point(&self, timestamp: i64) -> bool {
        timestamp >= self.min_timestamp && timestamp < self.max_timestamp
    }

    fn ordering(&self, timestamp: i64) -> PointPartitionOrdering {
        if timestamp < self.min_timestamp {
            return PointPartitionOrdering::Past;
        }

        if timestamp >= self.max_timestamp {
            return PointPartitionOrdering::Future;
        }

        return PointPartitionOrdering::Current;
    }

    pub fn min_timestamp(&self) -> i64 {
        self.min_timestamp
    }

    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }
}
