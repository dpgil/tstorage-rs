use crate::{
    metric::{DataPoint, Row},
    partition::MemoryPartition,
};

pub struct Storage {
    partitions: Vec<MemoryPartition>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            partitions: vec![MemoryPartition::new()],
        }
    }

    pub fn select(&self, name: &str, start: i64, end: i64) -> Vec<DataPoint> {
        let partition = &self.partitions[self.partitions.len() - 1];
        partition.select(name, start, end)
    }

    pub fn insert(&self, rows: &[Row]) {
        let partition = &self.partitions[self.partitions.len() - 1];
        rows.iter().for_each(|r| partition.insert(r))
    }
}
