use std::io::{BufReader, BufWriter, Read, Seek, Write};

use tsz::{stream::BufferedWriter, DataPoint, Encode, StdEncoder};

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

pub fn encode_points<W: Write>(
    writable: &mut W,
    data_points: &[crate::DataPoint],
) -> std::io::Result<()> {
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

pub struct GorillaDecoder<R: Read> {
    reader: BufReader<R>,
}

impl<R: Read> Decoder for GorillaDecoder<R> {
    fn decode_points(&mut self, n: usize) -> std::io::Result<Vec<crate::DataPoint>> {
        todo!()
    }
}
