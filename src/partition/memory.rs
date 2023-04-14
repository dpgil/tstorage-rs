use crate::metric::{DataPoint, Row};
use anyhow::Result;
use std::path::Path;

use super::{disk::flush, Boundary, Partition, PartitionError, PointPartitionOrdering};

#[derive(Debug)]
pub struct MemoryPartition {
    pub map: dashmap::DashMap<String, MetricEntry>,
    partition_boundary: Boundary,
}

impl Partition for MemoryPartition {
    fn select(&self, name: &str, start: i64, end: i64) -> Result<Vec<DataPoint>> {
        Ok(match self.partition_boundary.contains_range(start, end) {
            true => match self.map.get(name) {
                Some(entry) => entry.select(start, end),
                None => vec![],
            },
            false => vec![],
        })
    }

    fn insert(&self, row: &Row) -> Result<(), PartitionError> {
        if !self
            .partition_boundary
            .contains_point(row.data_point.timestamp)
        {
            return Err(PartitionError::OutOfBounds);
        }
        match self.map.get_mut(row.metric) {
            Some(mut m) => {
                m.insert(row.data_point);
            }
            None => {
                self.map
                    .insert(row.metric.to_string(), MetricEntry::new(row.data_point));
            }
        };
        Ok(())
    }

    fn ordering(&self, row: &Row) -> PointPartitionOrdering {
        self.partition_boundary.ordering(row.data_point.timestamp)
    }

    fn flush(
        &self,
        dir_path: &Path,
        encode_strategy: crate::EncodeStrategy,
    ) -> Result<(), PartitionError> {
        flush(self, dir_path, encode_strategy).map_err(|_| PartitionError::Flush)
    }

    fn boundary(&self) -> Boundary {
        self.partition_boundary
    }

    fn clean(&self) -> Result<(), PartitionError> {
        // Memory held by this partition gets automatically removed
        // when the variable is dropped.
        Ok(())
    }
}

impl MemoryPartition {
    pub fn new(partition_duration: i64, min_timestamp: i64) -> Self {
        let partition_boundary = Boundary {
            min_timestamp,
            max_timestamp: min_timestamp + partition_duration,
        };

        Self {
            map: dashmap::DashMap::new(),
            partition_boundary,
        }
    }
}

#[derive(Debug)]
pub struct MetricEntry {
    pub data_points: Vec<DataPoint>,
}

impl MetricEntry {
    pub fn new(data_point: DataPoint) -> Self {
        Self {
            data_points: vec![data_point],
        }
    }

    pub fn min_timestamp(&self) -> i64 {
        if self.data_points.is_empty() {
            0
        } else {
            self.data_points[0].timestamp
        }
    }

    pub fn max_timestamp(&self) -> i64 {
        if self.data_points.is_empty() {
            0
        } else {
            self.data_points[self.data_points.len() - 1].timestamp
        }
    }

    pub fn select(&self, start: i64, end: i64) -> Vec<DataPoint> {
        if self.data_points.is_empty() {
            return vec![];
        }

        let min_timestamp = self.data_points[0].timestamp;
        let max_timestamp = self.data_points[self.data_points.len() - 1].timestamp;
        if min_timestamp > end || max_timestamp < start {
            // Out of range
            return vec![];
        }

        let start_idx = if start <= min_timestamp {
            0
        } else {
            self.data_points
                .binary_search_by(|dp| {
                    if dp.timestamp >= start {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                })
                .unwrap_or_else(|i| i)
        };

        let end_idx = if end >= max_timestamp {
            self.data_points.len()
        } else {
            self.data_points
                .binary_search_by(|dp| {
                    if dp.timestamp > end {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                })
                .unwrap_or_else(|i| i)
        };

        return self.data_points[start_idx..end_idx].to_vec();
    }

    pub fn insert(&mut self, data_point: DataPoint) {
        match self.data_points.is_empty() {
            true => self.data_points.push(data_point),
            false => {
                let max_timestamp = self.data_points[self.data_points.len() - 1].timestamp;
                if data_point.timestamp >= max_timestamp {
                    self.data_points.push(data_point)
                } else {
                    // Out-of-order insert
                    let pos = self
                        .data_points
                        .binary_search_by_key(&data_point.timestamp, |d| d.timestamp)
                        .unwrap_or_else(|i| i);
                    self.data_points.insert(pos, data_point);
                }
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use crate::{
        metric::{DataPoint, Row},
        partition::{memory::Partition, Boundary, PartitionError},
    };

    use super::MemoryPartition;

    fn create_partition_with_rows(partition_duration: i64, rows: &[Row]) -> MemoryPartition {
        assert!(!rows.is_empty());
        let partition = MemoryPartition::new(partition_duration, rows[0].data_point.timestamp);
        for row in rows {
            partition.insert(row).unwrap();
        }
        partition
    }

    #[test]
    fn test_partition_boundary() {
        let boundaries = Boundary {
            min_timestamp: 1000,
            max_timestamp: 2000,
        };
        assert!(boundaries.contains_range(1500, 1800));
        assert!(boundaries.contains_range(0, 3000));
        assert!(boundaries.contains_range(1999, 2000));

        assert!(!boundaries.contains_range(2000, 2001));
        assert!(!boundaries.contains_range(0, 999));
    }

    #[test]
    fn test_min_max_timestamp() {
        let partition = MemoryPartition::new(1000, 1234);
        assert_eq!(partition.boundary().min_timestamp(), 1234);
        assert_eq!(partition.boundary().max_timestamp(), 2234);
    }

    #[test]
    fn test_simple_select_in_range() {
        let metric = "hello";
        let data_point = DataPoint {
            timestamp: 1234,
            value: 4.20,
        };
        let row = Row { metric, data_point };
        let partition = MemoryPartition::new(60000, data_point.timestamp);
        partition.insert(&row).unwrap();
        let result = partition.select(metric, 1000, 2000).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(data_point, result[0]);
    }

    #[test]
    fn test_multiple_metrics() {
        let metric_a = "hello";
        let data_point_a = DataPoint {
            timestamp: 1000,
            value: 4.20,
        };
        let row_a = &Row {
            metric: metric_a,
            data_point: data_point_a,
        };

        let metric_b = "world";
        let data_point_b = DataPoint {
            timestamp: 1001,
            value: 1.50,
        };
        let row_b = &Row {
            metric: metric_b,
            data_point: data_point_b,
        };

        let partition = MemoryPartition::new(60000, row_a.data_point.timestamp);
        partition.insert(row_a).unwrap();
        partition.insert(row_b).unwrap();

        let result = partition.select(metric_a, 1000, 2000).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(data_point_a, result[0]);

        let result = partition.select(metric_b, 1000, 2000).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(data_point_b, result[0]);
    }

    #[test]
    fn test_simple_select_out_of_range() {
        let metric = "hello";
        let row = Row {
            metric,
            data_point: DataPoint {
                timestamp: 1234,
                value: 4.20,
            },
        };
        let partition = MemoryPartition::new(60000, row.data_point.timestamp);
        partition.insert(&row).unwrap();
        let result = partition.select(metric, 0, 1000).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_select_boundaries() {
        let metric = "hello";
        let data_points = [
            DataPoint {
                timestamp: 100,
                value: 0.0,
            },
            DataPoint {
                timestamp: 200,
                value: 0.0,
            },
            DataPoint {
                timestamp: 200,
                value: 1.0,
            },
            DataPoint {
                timestamp: 300,
                value: 0.0,
            },
            DataPoint {
                timestamp: 400,
                value: 0.0,
            },
            DataPoint {
                timestamp: 400,
                value: 1.0,
            },
            DataPoint {
                timestamp: 400,
                value: 2.0,
            },
            DataPoint {
                timestamp: 500,
                value: 0.0,
            },
        ];
        let rows: Vec<Row> = data_points
            .iter()
            .map(|dp| Row {
                metric,
                data_point: *dp,
            })
            .collect();

        let partition = create_partition_with_rows(60000, &rows);

        let result = partition.select(metric, 200, 400).unwrap();
        assert_eq!(result, data_points[1..7]);
    }

    #[test]
    fn test_complex_select() {
        let metric = "hello";
        let data_points = [
            DataPoint {
                timestamp: 100,
                value: 0.0,
            },
            DataPoint {
                timestamp: 200,
                value: 0.0,
            },
            DataPoint {
                timestamp: 200,
                value: 1.0,
            },
            DataPoint {
                timestamp: 300,
                value: 0.0,
            },
        ];

        let rows: Vec<Row> = data_points
            .iter()
            .map(|dp| Row {
                metric,
                data_point: *dp,
            })
            .collect();

        let partition = create_partition_with_rows(60000, &rows);

        let result = partition.select(metric, 101, 300).unwrap();
        assert_eq!(result, data_points[1..]);
    }

    #[test]
    fn test_past_writes() {
        let metric = "hello";
        let data_points = [
            DataPoint {
                timestamp: 200, // sets the timestamp baseline for the partition
                value: 2.0,
            },
            DataPoint {
                timestamp: 100, // should belong to a previous partition
                value: 1.0,
            },
            DataPoint {
                timestamp: 300,
                value: 3.0,
            },
        ];

        let rows: Vec<Row> = data_points
            .iter()
            .map(|dp| Row {
                metric,
                data_point: *dp,
            })
            .collect();

        let partition = MemoryPartition::new(60000, rows[0].data_point.timestamp);
        partition.insert(&rows[0]).unwrap();
        assert_eq!(
            partition.insert(&rows[1]).err(),
            Some(PartitionError::OutOfBounds)
        );
        partition.insert(&rows[2]).unwrap();

        let result = partition.select(metric, 0, 1000).unwrap();
        assert_eq!(result, vec![data_points[0], data_points[2]]);
    }

    #[test]
    fn test_out_of_order_writes() {
        let metric = "hello";
        let mut data_points = [
            DataPoint {
                timestamp: 100,
                value: 0.0,
            },
            DataPoint {
                timestamp: 300,
                value: 0.0,
            },
            DataPoint {
                timestamp: 200, // out of order
                value: 0.0,
            },
            DataPoint {
                timestamp: 400,
                value: 0.0,
            },
        ];

        let rows: Vec<Row> = data_points
            .iter()
            .map(|dp| Row {
                metric,
                data_point: *dp,
            })
            .collect();

        let partition = create_partition_with_rows(60000, &rows);
        data_points.sort_by_key(|d| d.timestamp);

        let result = partition.select(metric, 0, 1000).unwrap();
        assert_eq!(result, data_points);
    }
}
