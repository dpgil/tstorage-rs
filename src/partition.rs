use std::sync::Mutex;

use anyhow::anyhow;

use crate::metric::{DataPoint, Row};

#[derive(Debug)]
pub struct MemoryPartition {
    map: dashmap::DashMap<String, MetricEntry>,
}

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

    pub fn insert(&mut self, data_point: DataPoint) -> anyhow::Result<()> {
        let data_points = self.data_points.get_mut().unwrap();
        match data_points.is_empty() {
            true => Ok(data_points.push(data_point)),
            false => {
                let max_timestamp = data_points[data_points.len() - 1].timestamp;
                if data_point.timestamp >= max_timestamp {
                    Ok(data_points.push(data_point))
                } else {
                    // TODO: Handle out-of-order insert
                    Err(anyhow!("out-of-order insert"))
                }
            }
        }
    }
}

impl MemoryPartition {
    pub fn new() -> Self {
        Self {
            map: dashmap::DashMap::new(),
        }
    }

    pub fn select(&self, name: &str, start: i64, end: i64) -> Vec<DataPoint> {
        match self.map.get(name) {
            Some(entry) => entry.select(start, end),
            None => vec![], // TODO
        }
    }

    pub fn insert(&self, row: &Row) {
        match self.map.get_mut(&row.metric) {
            Some(mut m) => {
                m.insert(row.data_point);
            }
            None => {
                self.map
                    .insert(row.metric.clone(), MetricEntry::new(row.data_point));
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use crate::metric::{DataPoint, Row};

    use super::MemoryPartition;

    #[test]
    fn test_simple_select_in_range() {
        let partition = MemoryPartition::new();
        let metric = "hello";
        let data_point = DataPoint {
            timestamp: 1234,
            value: 4.20,
        };
        partition.insert(&Row {
            metric: metric.to_string(),
            data_point,
        });
        let result = partition.select(metric, 1000, 2000);
        assert_eq!(result.len(), 1);
        assert_eq!(data_point, result[0]);
    }

    #[test]
    fn test_simple_select_out_of_range() {
        let partition = MemoryPartition::new();
        let metric = "hello";
        partition.insert(&Row {
            metric: metric.to_string(),
            data_point: DataPoint {
                timestamp: 1234,
                value: 4.20,
            },
        });
        let result = partition.select(metric, 0, 1000);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_select_boundaries() {
        let partition = MemoryPartition::new();
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

        for data_point in data_points {
            partition.insert(&Row {
                metric: metric.to_string(),
                data_point,
            });
        }

        let result = partition.select(metric, 200, 400);
        assert_eq!(result, data_points[1..7]);
    }

    #[test]
    fn test_complex_select() {
        let partition = MemoryPartition::new();
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

        for data_point in data_points {
            partition.insert(&Row {
                metric: metric.to_string(),
                data_point,
            });
        }

        let result = partition.select(metric, 101, 300);
        assert_eq!(result, data_points[1..4]);
    }

    #[test]
    fn test_out_of_order_writes() {
        let partition = MemoryPartition::new();
        let metric = "hello";
        let data_points = [
            DataPoint {
                timestamp: 200,
                value: 2.0,
            },
            DataPoint {
                timestamp: 100,
                value: 1.0,
            },
            DataPoint {
                timestamp: 300,
                value: 3.0,
            },
        ];

        for data_point in data_points {
            partition.insert(&Row {
                metric: metric.to_string(),
                data_point,
            });
        }

        let result = partition.select(metric, 0, 1000);
        assert_eq!(result, vec![data_points[0], data_points[2]]);
    }
}
