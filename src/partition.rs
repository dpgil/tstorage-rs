use crate::metric::{DataPoint, Row};

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
    pub map: dashmap::DashMap<String, MetricEntry>,
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

    pub fn min_timestamp(&self) -> i64 {
        self.partition_boundary.min_timestamp
    }

    pub fn max_timestamp(&self) -> i64 {
        self.partition_boundary.max_timestamp
    }
}

#[cfg(test)]
pub mod tests {
    use crate::metric::{DataPoint, Row};

    use super::{MemoryPartition, PartitionBoundary};

    fn create_partition_with_rows(
        partition_duration: Option<i64>,
        rows: &[Row],
    ) -> MemoryPartition {
        assert!(!rows.is_empty());
        let (first, rest) = rows.split_first().unwrap();
        let partition = MemoryPartition::new(partition_duration, first);
        for row in rest {
            partition.insert(row);
        }
        partition
    }

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
    fn test_min_max_timestamp() {
        let metric = "hello";
        let data_point = DataPoint {
            timestamp: 1234,
            value: 4.20,
        };
        let row = Row {
            metric: metric.to_string(),
            data_point,
        };
        let partition = MemoryPartition::new(Some(1000), &row);
        assert_eq!(partition.min_timestamp(), 1234);
        assert_eq!(partition.max_timestamp(), 2234);
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
    fn test_multiple_metrics() {
        let metric_a = "hello";
        let data_point_a = DataPoint {
            timestamp: 1000,
            value: 4.20,
        };
        let row_a = &Row {
            metric: metric_a.to_string(),
            data_point: data_point_a,
        };

        let metric_b = "world";
        let data_point_b = DataPoint {
            timestamp: 1001,
            value: 1.50,
        };
        let row_b = &Row {
            metric: metric_b.to_string(),
            data_point: data_point_b,
        };

        let partition = MemoryPartition::new(None, row_a);
        partition.insert(row_b);

        let result = partition.select(metric_a, 1000, 2000);
        assert_eq!(result.len(), 1);
        assert_eq!(data_point_a, result[0]);

        let result = partition.select(metric_b, 1000, 2000);
        assert_eq!(result.len(), 1);
        assert_eq!(data_point_b, result[0]);
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
                metric: metric.to_string(),
                data_point: *dp,
            })
            .collect();

        let partition = create_partition_with_rows(None, &rows);

        let result = partition.select(metric, 200, 400);
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
                metric: metric.to_string(),
                data_point: *dp,
            })
            .collect();

        let partition = create_partition_with_rows(None, &rows);

        let result = partition.select(metric, 101, 300);
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
                metric: metric.to_string(),
                data_point: *dp,
            })
            .collect();

        let partition = create_partition_with_rows(None, &rows);

        let result = partition.select(metric, 0, 1000);
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
                metric: metric.to_string(),
                data_point: *dp,
            })
            .collect();

        let partition = create_partition_with_rows(None, &rows);
        data_points.sort_by_key(|d| d.timestamp);

        let result = partition.select(metric, 0, 1000);
        assert_eq!(result, data_points);
    }
}
