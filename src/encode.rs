use anyhow::Result;
use std::{fs::File, io::Write};

use crate::partition::MetricEntry;

pub type EncodeFn = fn(f: &mut File, metric_entry: &MetricEntry) -> Result<()>;

pub fn csv_encode(f: &mut File, metric_entry: &MetricEntry) -> Result<()> {
    for data_point in metric_entry.data_points.iter() {
        f.write_all(format!("{},{}\n", data_point.timestamp, data_point.value).as_bytes())?;
    }
    Ok(())
}

#[derive(Clone, Copy, Default)]
pub enum Encoder {
    #[default]
    CSV,
}

impl From<Encoder> for EncodeFn {
    fn from(encoder: Encoder) -> Self {
        match encoder {
            Encoder::CSV => csv_encode,
        }
    }
}
