#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DataPoint {
    pub timestamp: i64,
    pub value: f64,
}

pub struct Row {
    pub metric: String,
    pub data_point: DataPoint,
}
