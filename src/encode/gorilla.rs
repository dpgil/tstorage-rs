use std::io::{Result, Write};
use tsz::{
    decode::Error,
    stream::{BufferedReader, BufferedWriter},
    DataPoint, Decode, Encode, StdDecoder, StdEncoder,
};

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

pub fn decode_points(bytes: &[u8], n: usize) -> Result<Vec<crate::DataPoint>> {
    let r = BufferedReader::new(bytes.into());
    let mut decoder = StdDecoder::new(r);

    let mut points = Vec::new();

    loop {
        if points.len() == n {
            break;
        }

        match decoder.next() {
            Ok(dp) => {
                // TODO: use u64 everywhere for timestamp
                let timestamp: i64 = dp.get_time().try_into().unwrap();
                points.push(crate::DataPoint {
                    timestamp,
                    value: dp.get_value(),
                });
            }
            Err(err) => {
                if err == Error::EndOfStream {
                    break;
                } else {
                    // TODO: handle other error types
                }
            }
        };
    }

    match points.len() == n {
        true => Ok(points),
        false => todo!(), // TODO: error
    }
}
