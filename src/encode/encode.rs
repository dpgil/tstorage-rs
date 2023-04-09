use crate::{
    encode::csv::{CsvDecoder, CsvEncoder},
    metric::DataPoint,
};
use serde::{Deserialize, Serialize};
use std::io::{Read, Result, Seek, Write};

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
pub enum EncodeStrategy {
    #[default]
    CSV,
}

pub trait Encoder {
    fn encode_points(&mut self, data_points: &[DataPoint]) -> Result<()>;
    fn get_current_offset(&mut self) -> Result<u64>;
    fn flush(&mut self) -> Result<()>;
}

pub trait Decoder {
    fn decode_points(&mut self, n: usize) -> Result<Vec<DataPoint>>;
}

pub fn get_encoder<W: Write + Seek>(encode_strategy: EncodeStrategy, writable: W) -> impl Encoder {
    match encode_strategy {
        EncodeStrategy::CSV => CsvEncoder::new(writable),
    }
}

pub fn get_decoder<R: Read>(encode_strategy: EncodeStrategy, readable: R) -> impl Decoder {
    match encode_strategy {
        EncodeStrategy::CSV => CsvDecoder::new(readable),
    }
}
