use std::io::{BufWriter, Seek, Write, Read, BufReader};

use tsz::{stream::BufferedWriter, DataPoint, Encode, StdEncoder};

use super::encode::{Encoder, Decoder};

pub struct GorillaEncoder<W: Write + Seek> {
    pub writer: BufWriter<W>,
}

impl<W: Write + Seek> GorillaEncoder<W> {
    pub fn new(writable: W) -> Self {
        Self {
            writer: BufWriter::new(writable),
        }
    }
}

pub fn encode_points<W: Write>(writable: &mut W, data_points: &[crate::DataPoint]) -> std::io::Result<()> {
    if data_points.is_empty() {
        return Ok(());
    }

    let tsz_writer = BufferedWriter::new();
    let mut tsz_encoder = StdEncoder::new(data_points[0].timestamp as u64, tsz_writer);
    for data_point in data_points {
        tsz_encoder.encode(DataPoint::new(
            data_point.timestamp as u64,
            data_point.value,
        ));
    }
    let bytes = tsz_encoder.close();
    writable.write_all(&bytes)
}

impl<W: Write + Seek> Encoder for GorillaEncoder<W> {
    fn encode_points(&mut self, data_points: &[crate::DataPoint]) -> std::io::Result<()> {
        if data_points.is_empty() {
            return Ok(());
        }

        let tsz_writer = BufferedWriter::new();
        let mut tsz_encoder = StdEncoder::new(data_points[0].timestamp as u64, tsz_writer);
        for data_point in data_points {
            tsz_encoder.encode(DataPoint::new(
                data_point.timestamp as u64,
                data_point.value,
            ));
        }
        let bytes = tsz_encoder.close();
        self.writer.write_all(&bytes)
    }

    fn get_current_offset(&mut self) -> std::io::Result<u64> {
        self.writer.seek(std::io::SeekFrom::Current(0))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

pub struct GorillaDecoder<R: Read> {
    reader: BufReader<R>,
}

impl <R: Read> Decoder for GorillaDecoder<R> {
    fn decode_points(&mut self, n: usize) -> std::io::Result<Vec<crate::DataPoint>> {
        todo!()
    }
}
