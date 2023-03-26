use crate::{
    encode::{Decoder, Encoder},
    metric::DataPoint,
};
use std::io::{BufReader, BufWriter, Read, Result, Seek, Write};

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
        // TODO(dpgil)
        Ok(DataPoint {
            timestamp: 123,
            value: 456.,
        })
    }
}

#[derive(Debug)]
struct FakeFile {
    pub buf: Vec<u8>,
    pos: u64,
}

impl FakeFile {
    fn new(buf: Vec<u8>) -> Self {
        Self { buf, pos: 0 }
    }
}

impl Seek for FakeFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<u64> {
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

#[cfg(test)]
pub mod tests {
    use crate::{encode::Encoder, metric::DataPoint};

    use super::{CsvEncoder, FakeFile};

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
}
