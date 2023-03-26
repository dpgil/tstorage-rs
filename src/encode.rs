use std::io::{Read, Result, Seek, Write};

use crate::metric::DataPoint;

use crate::csv::{CsvDecoder, CsvEncoder};

#[derive(Clone, Copy, Default)]
pub enum EncodeStrategy {
    #[default]
    CSV,
}

pub trait Encoder {
    fn encode_point(&mut self, data_point: &DataPoint) -> Result<()>;
    fn get_current_offset(&mut self) -> Result<u64>;
    fn flush(&mut self) -> Result<()>;
}

pub trait Decoder {
    fn decode_point(&mut self) -> Result<DataPoint>;
}

pub fn get_encoder<W: Write + Seek>(encode_strategy: EncodeStrategy, writeable: W) -> impl Encoder {
    match encode_strategy {
        EncodeStrategy::CSV => CsvEncoder::new(writeable),
    }
}

pub fn get_decoder<R: Read>(encode_strategy: EncodeStrategy, readable: R) -> impl Decoder {
    match encode_strategy {
        EncodeStrategy::CSV => CsvDecoder::new(readable),
    }
}
