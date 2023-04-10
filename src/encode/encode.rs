use crate::metric::DataPoint;
use serde::{Deserialize, Serialize};
use std::io::{Read, Result, Write};

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
        EncodeStrategy::CSV => super::csv::encode_points(writable, data_points),
        EncodeStrategy::Gorilla => super::gorilla::encode_points(writable, data_points),
    }
}

pub fn decode_points<R: Read>(
    readable: R,
    n: usize,
    encode_strategy: EncodeStrategy,
) -> Result<Vec<DataPoint>> {
    match encode_strategy {
        EncodeStrategy::CSV => super::csv::decode_points(readable, n),
        EncodeStrategy::Gorilla => todo!(),
    }
}
