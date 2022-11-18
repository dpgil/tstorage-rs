use std::sync::Mutex;

use crate::metric::{DataPoint, Row};

#[derive(Debug)]
struct MetricEntry {
    data_points: Mutex<Vec<DataPoint>>,
}

impl MetricEntry {
    pub fn new(data_point: DataPoint) -> Self {
        Self {
            data_points: Mutex::new(vec![data_point]),
        }
    }

    pub fn select(&self, start: i64, end: i64) -> Vec<DataPoint> {
        let data_points = self.data_points.lock().unwrap();
        if data_points.is_empty() {
            return vec![];
        }

        let min_timestamp = data_points[0].timestamp;
        let max_timestamp = data_points[data_points.len() - 1].timestamp;
        if min_timestamp > end || max_timestamp < start {
            // Out of range
            return vec![];
        }

        let start_idx = if start <= min_timestamp {
            0
        } else {
            match data_points.binary_search_by(|dp| {
                if dp.timestamp >= start {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }) {
                Ok(i) => i,
                Err(i) => i,
            }
        };

        let end_idx = if end >= max_timestamp {
            data_points.len()
        } else {
            match data_points.binary_search_by(|dp| {
                if dp.timestamp > end {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }) {
                Ok(i) => i,
                Err(i) => i,
            }
        };

        return data_points[start_idx..end_idx].to_vec();
    }

    pub fn insert(&mut self, data_point: DataPoint) {
        let data_points = self.data_points.get_mut().unwrap();
        match data_points.is_empty() {
            true => data_points.push(data_point),
            false => {
                let max_timestamp = data_points[data_points.len() - 1].timestamp;
                if data_point.timestamp >= max_timestamp {
                    data_points.push(data_point)
                }
                // TODO:major: Handle out-of-order insert
            }
        }
    }
}

// Terrible naming, but this represents whether a point belongs in
// a particular parition, or it belongs in a later one (future) or
// an earlier one (past).
pub enum PointPartitionOrdering {
    Current,
    Future,
    Past,
}

#[derive(Debug)]
struct PartitionBoundary {
    min_timestamp: i64,
    max_timestamp: i64, // exclusive
}

impl PartitionBoundary {
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
}

#[derive(Debug)]
pub struct MemoryPartition {
    map: dashmap::DashMap<String, MetricEntry>,
    partition_boundary: PartitionBoundary,
}

impl MemoryPartition {
    // Requiring a row to initialize the partition makes the initialization uglier,
    // but allows us to guarantee the partition boundaries in the rest of the code.
    pub fn new(partition_duration: Option<i64>, initial_row: &Row) -> Self {
        let partition_duration = partition_duration.unwrap_or(60000);
        let partition_boundary = PartitionBoundary {
            min_timestamp: initial_row.data_point.timestamp,
            max_timestamp: initial_row.data_point.timestamp + partition_duration,
        };

        let map = dashmap::DashMap::new();
        map.insert(
            initial_row.metric.clone(),
            MetricEntry::new(initial_row.data_point),
        );

        Self {
            map,
            partition_boundary,
        }
    }

    pub fn select(&self, name: &str, start: i64, end: i64) -> Vec<DataPoint> {
        match self.partition_boundary.contains_range(start, end) {
            true => match self.map.get(name) {
                Some(entry) => entry.select(start, end),
                None => vec![],
            },
            false => vec![],
        }
    }

    pub fn insert(&self, row: &Row) {
        if self
            .partition_boundary
            .contains_point(row.data_point.timestamp)
        {
            match self.map.get_mut(&row.metric) {
                Some(mut m) => {
                    m.insert(row.data_point);
                }
                None => {
                    self.map
                        .insert(row.metric.clone(), MetricEntry::new(row.data_point));
                }
            };
        }
    }

    pub fn ordering(&self, row: &Row) -> PointPartitionOrdering {
        self.partition_boundary.ordering(row.data_point.timestamp)
    }
}

#[cfg(test)]
pub mod tests {
    use crate::metric::{DataPoint, Row};

    use super::{MemoryPartition, PartitionBoundary};

    #[test]
    fn test_partition_boundary() {
        let boundaries = PartitionBoundary {
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
    fn test_simple_select_in_range() {
        let metric = "hello";
        let data_point = DataPoint {
            timestamp: 1234,
            value: 4.20,
        };
        let row = Row {
            metric: metric.to_string(),
            data_point,
        };
        let partition = MemoryPartition::new(None, &row);
        let result = partition.select(metric, 1000, 2000);
        assert_eq!(result.len(), 1);
        assert_eq!(data_point, result[0]);
    }

    #[test]
    fn test_simple_select_out_of_range() {
        let metric = "hello";
        let row = Row {
            metric: metric.to_string(),
            data_point: DataPoint {
                timestamp: 1234,
                value: 4.20,
            },
        };
        let partition = MemoryPartition::new(None, &row);
        let result = partition.select(metric, 0, 1000);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_select_boundaries() {
        let metric = "hello";
        let initial_data_point = DataPoint {
            timestamp: 100,
            value: 0.0,
        };
        let data_points = [
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

        let partition = MemoryPartition::new(
            None,
            &Row {
                metric: metric.to_string(),
                data_point: initial_data_point,
            },
        );

        for data_point in data_points {
            partition.insert(&Row {
                metric: metric.to_string(),
                data_point,
            });
        }

        let result = partition.select(metric, 200, 400);
        assert_eq!(result, data_points[0..6]);
    }

    #[test]
    fn test_complex_select() {
        let metric = "hello";
        let initial_data_point = DataPoint {
            timestamp: 100,
            value: 0.0,
        };
        let data_points = [
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

        let partition = MemoryPartition::new(
            None,
            &Row {
                metric: metric.to_string(),
                data_point: initial_data_point,
            },
        );

        for data_point in data_points {
            partition.insert(&Row {
                metric: metric.to_string(),
                data_point,
            });
        }

        let result = partition.select(metric, 101, 300);
        assert_eq!(result, data_points);
    }

    #[test]
    fn test_out_of_order_writes() {
        let metric = "hello";
        let initial_data_point = DataPoint {
            timestamp: 200,
            value: 2.0,
        };
        let data_points = [
            DataPoint {
                timestamp: 100,
                value: 1.0,
            },
            DataPoint {
                timestamp: 300,
                value: 3.0,
            },
        ];

        let partition = MemoryPartition::new(
            None,
            &Row {
                metric: metric.to_string(),
                data_point: initial_data_point,
            },
        );

        for data_point in data_points {
            partition.insert(&Row {
                metric: metric.to_string(),
                data_point,
            });
        }

        let result = partition.select(metric, 0, 1000);
        assert_eq!(result, vec![initial_data_point, data_points[1]]);
    }
}
