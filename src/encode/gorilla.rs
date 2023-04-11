use std::io::{Error, ErrorKind, Result, Write};
use tsz::{
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
                let timestamp: i64 = dp
                    .get_time()
                    .try_into()
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
                points.push(crate::DataPoint {
                    timestamp,
                    value: dp.get_value(),
                });
            }
            Err(err) => {
                if err == tsz::decode::Error::EndOfStream {
                    break;
                } else {
                    return Err(Error::new(ErrorKind::InvalidInput, err.to_string()));
                }
            }
        };
    }

    match points.len() == n {
        true => Ok(points),
        false => Err(Error::new(
            ErrorKind::InvalidData,
            "could not read n points, metadata does not match data file",
        )),
    }
}

#[cfg(test)]
pub mod tests {
    use std::io::Write;

    use crate::{
        encode::{
            gorilla::{decode_points, encode_points},
            test::fake_file::FakeFile,
        },
        DataPoint,
    };

    #[test]
    fn test_encode_decode() {
        let buf = Vec::new();
        let mut fake_file = FakeFile::new(buf);
        let expected_points = [DataPoint {
            timestamp: 123,
            value: 1.0,
        }];
        encode_points(&mut fake_file, &expected_points).unwrap();
        fake_file.flush().unwrap();

        let actual_points = decode_points(&fake_file.buf, 1).unwrap();
        assert_eq!(actual_points, expected_points);
    }
}
