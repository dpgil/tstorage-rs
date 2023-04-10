use crate::metric::DataPoint;
use std::io::{BufRead, BufReader, ErrorKind, Read, Result, Write};

pub fn encode_points<W: Write>(writable: &mut W, data_points: &[DataPoint]) -> Result<()> {
    for data_point in data_points {
        writable.write_all(format!("{},{}\n", data_point.timestamp, data_point.value).as_bytes())?
    }
    Ok(())
}

fn decode_point<R: Read>(readable: &mut BufReader<R>) -> Result<DataPoint> {
    let mut buf = String::new();
    readable.read_line(&mut buf)?;

    let parts: Vec<&str> = buf.trim_end().split(',').collect();
    let (ts, val) = match parts.len() {
        2 => Ok((parts[0], parts[1])),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("expected two rows per line: {:?}", parts),
        )),
    }?;

    let timestamp: i64 = ts
        .parse()
        .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
    let value: f64 = val
        .parse()
        .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;

    Ok(DataPoint { timestamp, value })
}

pub fn decode_points<R: Read>(readable: R, n: usize) -> Result<Vec<DataPoint>> {
    let mut reader = BufReader::new(readable);
    let mut points: Vec<DataPoint> = Vec::new();
    for _ in 0..n {
        points.push(decode_point(&mut reader)?);
    }
    Ok(points)
}

#[cfg(test)]
pub mod tests {
    use std::io::{Seek, Write};

    use crate::{
        encode::{
            csv::{decode_points, encode_points},
            test::fake_file::FakeFile,
        },
        metric::DataPoint,
    };

    #[test]
    fn test_encode() {
        let buf = Vec::new();
        let mut fake_file = FakeFile::new(buf);
        encode_points(
            &mut fake_file,
            &[DataPoint {
                timestamp: 123,
                value: 1.0,
            }],
        )
        .unwrap();
        fake_file.flush().unwrap();
        assert_eq!(fake_file.buf, b"123,1\n");
    }

    #[test]
    fn test_get_offset() {
        let buf = Vec::new();
        let mut fake_file = FakeFile::new(buf);
        encode_points(
            &mut fake_file,
            &[
                DataPoint {
                    timestamp: 123,
                    value: 1.0,
                },
                DataPoint {
                    timestamp: 456,
                    value: 2.0,
                },
            ],
        )
        .unwrap();
        fake_file.flush().unwrap();
        assert_eq!(fake_file.seek(std::io::SeekFrom::Current(0)).unwrap(), 12); // "123,1\n456,2\n".len()
        assert_eq!(fake_file.buf, b"123,1\n456,2\n");
    }

    #[test]
    fn test_decode() {
        let data_points = &[DataPoint {
            timestamp: 123,
            value: 1.0,
        }];
        let buf = Vec::new();
        let mut fake_file = FakeFile::new(buf);
        encode_points(&mut fake_file, data_points).unwrap();
        fake_file.flush().unwrap();
        fake_file.seek(std::io::SeekFrom::Start(0)).unwrap();
        let actual = decode_points(fake_file, 1).unwrap();
        assert_eq!(actual, data_points);
    }
}
