#[derive(Debug)]
pub struct DataPoint {
    pub timestamp: i64,
    pub value: f64,
}

pub struct Row {
    pub metric: String,
    pub data_point: DataPoint,
}

pub struct Storage {}

impl Storage {
    pub fn new() -> Self {
        Self {}
    }

    pub fn select(&self, _name: &str, _start: i64, _end: i64) -> Vec<DataPoint> {
        vec![]
    }

    pub fn insert(&self, _rows: &[Row]) {}
}
