use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub const DATA_FILE_NAME: &str = "data";
pub const META_FILE_NAME: &str = "meta.json";

#[derive(Debug, Serialize, Deserialize)]
pub struct MetricMetadata {
    pub name: String,
    pub offset: i64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub num_data_points: i64,
}

#[derive(Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub num_data_points: i64,
    pub metrics: HashMap<String, MetricMetadata>,
    pub created_at: i64, // TODO:minor: time.Time
}
