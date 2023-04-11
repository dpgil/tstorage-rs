use crate::DataPoint;
use serde::{Deserialize, Serialize};
use std::io::{Result, Write};

mod csv;
mod gorilla;
mod test;

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
pub enum EncodeStrategy {
    #[default]
    CSV,
    Gorilla,
}

pub fn encode_points<W: Write>(
    writable: &mut W,
    data_points: &[DataPoint],
    encode_strategy: EncodeStrategy,
) -> Result<()> {
    match encode_strategy {
        EncodeStrategy::CSV => csv::encode_points(writable, data_points),
        EncodeStrategy::Gorilla => gorilla::encode_points(writable, data_points),
    }
}

pub fn decode_points(
    bytes: &[u8],
    n: usize,
    encode_strategy: EncodeStrategy,
) -> Result<Vec<DataPoint>> {
    match encode_strategy {
        EncodeStrategy::CSV => csv::decode_points(bytes, n),
        EncodeStrategy::Gorilla => gorilla::decode_points(bytes, n),
    }
}
