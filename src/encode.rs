use std::io::{BufWriter, Result, Seek, Write};

use crate::metric::DataPoint;

#[derive(Clone, Copy, Default)]
pub enum EncodeStrategy {
    #[default]
    CSV,
}

pub trait Encoder {
    // TODO: Refactor encode function to return bytes rather than write
    // directly to the file, for easier testing.
    fn encode_point(&mut self, data_point: &DataPoint) -> Result<()>;
    // TODO: decode bytes to metric entry.
    // encode and decode should be inverse functions.
    // fn decode(&self, bytes: &[u8]) -> MetricEntry;
    // TODO: Does it make sense to expect the encoder to provide this?
    fn get_current_offset(&mut self) -> Result<u64>;
}

pub fn get_encoder<W: Write + Seek>(encode_strategy: EncodeStrategy, writeable: W) -> impl Encoder {
    match encode_strategy {
        EncodeStrategy::CSV => CsvEncoder::new(writeable),
    }
}

pub struct CsvEncoder<W: Write + Seek> {
    writer: BufWriter<W>,
}

impl<W: Write + Seek> CsvEncoder<W> {
    fn new(writeable: W) -> Self {
        Self {
            writer: BufWriter::new(writeable),
        }
    }
}

impl<W: Write + Seek> Encoder for CsvEncoder<W> {
    fn encode_point(&mut self, data_point: &DataPoint) -> Result<()> {
        self.writer
            .write_all(format!("{},{}\n", data_point.timestamp, data_point.value).as_bytes())
    }

    fn get_current_offset(&mut self) -> Result<u64> {
        self.writer.seek(std::io::SeekFrom::Current(0))
    }
}
