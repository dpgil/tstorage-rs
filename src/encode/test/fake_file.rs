use std::{
    cmp::min,
    io::{Read, Result, Seek, Write},
};

#[derive(Debug)]
pub struct FakeFile {
    pub buf: Vec<u8>,
    pos: u64,
}

impl FakeFile {
    pub fn new(buf: Vec<u8>) -> Self {
        Self { buf, pos: 0 }
    }
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
