#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DataPoint {
    pub timestamp: i64,
    pub value: f64,
}

#[derive(Debug)]
pub struct Row<'a> {
    pub metric: &'a str,
    pub data_point: DataPoint,
}
