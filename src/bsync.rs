use std::io::prelude::*;
use std::io::SeekFrom;
use std::{fs::File, fs::OpenOptions, path::Path};
use xxhash_rust::xxh3::xxh3_64;

pub struct BlockFile {
    reader: File,
    blocksize: u64,
    next_block: u64,
    buf: Vec<u8>,
}

impl BlockFile {
    pub fn new(file: &str, blocksize: u64, writeable: bool) -> std::io::Result<Self> {
        let path = Path::new(file);
        let fd: File = if writeable {
            OpenOptions::new().write(true).read(true).open(path)?
        } else {
            File::open(path)?
        };
        Ok(Self {
            reader: fd,
            blocksize,
            next_block: 0,
            buf: vec![0; blocksize as usize],
        })
    }

    pub fn get_len(&mut self) -> std::io::Result<u64> {
        self.reader.seek(SeekFrom::End(0))?;
        let len = self.reader.stream_position()?;
        self.reader
            .seek(SeekFrom::Start(self.blocksize * self.next_block))?;
        Ok(len)
    }

    pub fn set_len(&mut self, len: u64) -> std::io::Result<()> {
        self.reader.set_len(len)
    }

    pub fn write_block(&mut self, block: u64, buf: &[u8]) -> std::io::Result<usize> {
        self.reader.seek(SeekFrom::Start(block * self.blocksize))?;
        self.reader.write_all(buf)?;
        self.reader
            .seek(SeekFrom::Start(self.blocksize * self.next_block))?;
        Ok(buf.len())
    }

    #[allow(dead_code)]
    pub fn get_hash_size() -> usize {
        8
    }

    #[allow(dead_code)]
    pub fn get_next_block(&self) -> u64 {
        self.next_block
    }

    #[allow(dead_code)]
    pub fn set_next_block(&mut self, next_block: u64) {
        self.next_block = next_block;
    }

    pub fn next_blockhash<'a>(&mut self) -> Option<u64> {
        match self.reader.read(&mut self.buf) {
            Ok(s) => {
                if s == 0 {
                    None
                } else {
                    let hash = xxh3_64(&self.buf[0..s]);
                    self.next_block += 1;
                    Some(hash)
                }
            }
            Err(e) => {
                eprintln!("next_blockhash: {:?}", e);
                None
            }
        }
    }

    #[allow(dead_code)]
    pub fn seek(&mut self, block: u64) -> std::io::Result<()> {
        self.reader.seek(SeekFrom::Start(self.blocksize * block))?;
        self.next_block = block;
        Ok(())
    }

    pub fn get_block<'a>(&mut self, block: u64, buf: &'a mut [u8]) -> std::io::Result<usize> {
        self.reader.seek(SeekFrom::Start(self.blocksize * block))?;
        let s: usize = self.reader.read(buf)?;
        self.reader
            .seek(SeekFrom::Start(self.next_block * self.blocksize))?;
        Ok(s)
    }
}
