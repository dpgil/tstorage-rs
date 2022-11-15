mod storage;

use crate::storage::{DataPoint, Row, Storage};

fn main() {
    let storage = Storage::new();
    storage.insert(&[Row {
        metric: "hello".to_string(),
        data_point: DataPoint {
            timestamp: 1234,
            value: 4.20,
        },
    }]);
    let result = storage.select("hello", 1000, 2000);
    println!("got result: {:?}", result);
}
