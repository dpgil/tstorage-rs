use std::io::{BufReader, Read, Result, Write};
use tsz::{stream::BufferedWriter, DataPoint, Encode, StdEncoder};

use crate::EncodeStrategy;

pub fn encode_points<W: Write>(writable: &mut W, data_points: &[crate::DataPoint]) -> Result<()> {
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

pub fn decode_points<R: Read>(
    readable: R,
    n: usize,
    encode_strategy: EncodeStrategy,
) -> Result<Vec<DataPoint>> {
    Ok(vec![])
}
