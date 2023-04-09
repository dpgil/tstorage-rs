use std::io::{BufWriter, Seek, Write};

use tsz::{stream::BufferedWriter, DataPoint, Encode, StdEncoder};

use super::encode::Encoder;

pub struct GorillaEncoder<W: Write + Seek> {
    pub sink: BufWriter<W>,
    // tsz's StdEncoder has its own buffering and doesn't write points
    // until the encoder is closed. We could change the Encoder interface
    // to accept all points at once, but for now we just buffer the incoming
    // data points here and encode and write them at flush time.
    pub buf: Vec<crate::DataPoint>,
}

impl<W: Write + Seek> GorillaEncoder<W> {
    pub fn new(writable: W) -> Self {
        Self {
            sink: BufWriter::new(writable),
            buf: Vec::default(),
        }
    }
}

impl<W: Write + Seek> Encoder for GorillaEncoder<W> {
    fn encode_point(&mut self, data_point: &crate::DataPoint) -> std::io::Result<()> {
        self.buf.push(*data_point);
        Ok(())
    }

    fn get_current_offset(&mut self) -> std::io::Result<u64> {
        self.flush()?;
        self.sink.seek(std::io::SeekFrom::Current(0))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }

        let tsz_writer = BufferedWriter::new();
        let mut tsz_encoder = StdEncoder::new(self.buf[0].timestamp as u64, tsz_writer);
        for data_point in &self.buf {
            tsz_encoder.encode(DataPoint::new(
                data_point.timestamp as u64,
                data_point.value,
            ));
        }
        let bytes = tsz_encoder.close();

        self.sink.write_all(&bytes)?;
        self.buf.clear();
        Ok(())
    }
}
