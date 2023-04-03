use crate::{
    encode::encode::{Decoder, Encoder},
    metric::DataPoint,
};
use std::{
    cmp::min,
    io::{BufRead, BufReader, BufWriter, ErrorKind, Read, Result, Seek, Write},
};

pub struct CsvEncoder<W: Write + Seek> {
    pub writer: BufWriter<W>,
}

impl<W: Write + Seek> CsvEncoder<W> {
    pub fn new(writeable: W) -> Self {
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

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()
    }
}

pub struct CsvDecoder<R: Read> {
    reader: BufReader<R>,
}

impl<R: Read> CsvDecoder<R> {
    pub fn new(readable: R) -> Self {
        Self {
            reader: BufReader::new(readable),
        }
    }
}

impl<R: Read> Decoder for CsvDecoder<R> {
    fn decode_point(&mut self) -> Result<DataPoint> {
        let mut buf = String::new();
        self.reader.read_line(&mut buf)?;

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
}

#[derive(Debug)]
struct FakeFile {
    pub buf: Vec<u8>,
    pos: u64,
}

impl Seek for FakeFile {
    fn seek(&mut self, seek_from: std::io::SeekFrom) -> Result<u64> {
        match seek_from {
            std::io::SeekFrom::Start(p) => self.pos = p,
            std::io::SeekFrom::End(p) => {
                let len: u64 = self.buf.len().try_into().unwrap();
                let delta: u64 = p.try_into().unwrap();
                self.pos = len - delta
            }
            std::io::SeekFrom::Current(_) => {}
        }
        Ok(self.pos)
    }
}

impl Write for FakeFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let num_bytes: usize = buf.len();
        self.pos += TryInto::<u64>::try_into(num_bytes).unwrap();
        self.buf.append(&mut buf.to_vec());
        Ok(num_bytes)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Read for FakeFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let curr_pos: usize = self.pos.try_into().unwrap();
        let count = min(buf.len(), self.buf.len() - curr_pos);
        for i in 0..count {
            buf[i] = self.buf[curr_pos + i];
        }
        self.pos = (curr_pos + count).try_into().unwrap();
        Ok(count)
    }
}

#[cfg(test)]
pub mod tests {
    use std::io::Seek;

    use crate::{
        encode::encode::{Decoder, Encoder},
        metric::DataPoint,
    };

    use super::{CsvDecoder, CsvEncoder, FakeFile};

    impl FakeFile {
        fn new(buf: Vec<u8>) -> Self {
            Self { buf, pos: 0 }
        }
    }

    #[test]
    fn test_encode() {
        let buf = Vec::new();
        let fake_file = FakeFile::new(buf);
        let mut encoder = CsvEncoder::new(fake_file);
        encoder
            .encode_point(&DataPoint {
                timestamp: 123,
                value: 1.0,
            })
            .unwrap();
        encoder.flush().unwrap();
        let fake_file = encoder.writer.into_inner().unwrap();
        assert_eq!(fake_file.buf, b"123,1\n");
    }

    #[test]
    fn test_get_offset() {
        let buf = Vec::new();
        let fake_file = FakeFile::new(buf);
        let mut encoder = CsvEncoder::new(fake_file);
        encoder
            .encode_point(&DataPoint {
                timestamp: 123,
                value: 1.0,
            })
            .unwrap();
        encoder
            .encode_point(&DataPoint {
                timestamp: 456,
                value: 2.0,
            })
            .unwrap();
        encoder.flush().unwrap();
        assert_eq!(encoder.get_current_offset().unwrap(), 12); // "123,1\n456,2\n".len()
        let fake_file = encoder.writer.into_inner().unwrap();
        assert_eq!(fake_file.buf, b"123,1\n456,2\n");
    }

    #[test]
    fn test_decode() {
        let data_point = DataPoint {
            timestamp: 123,
            value: 1.0,
        };
        let buf = Vec::new();
        let fake_file = FakeFile::new(buf);
        let mut encoder = CsvEncoder::new(fake_file);
        encoder.encode_point(&data_point).unwrap();
        encoder.flush().unwrap();
        let mut fake_file = encoder.writer.into_inner().unwrap();
        fake_file.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut decoder = CsvDecoder::new(fake_file);
        let actual = decoder.decode_point().unwrap();
        assert_eq!(actual, data_point);
    }
}
