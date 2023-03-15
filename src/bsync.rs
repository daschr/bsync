use base16ct::lower::encode;
use generic_array::GenericArray;
use sha3::{Digest, Sha3_256};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::{fs::File, fs::OpenOptions, path::Path};

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
            OpenOptions::new().write(true).open(path)?
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
        self.reader.write(buf)
    }

    pub fn get_hash_size() -> usize {
        Sha3_256::output_size()
    }

    pub fn get_next_block(&self) -> u64 {
        self.next_block
    }

    pub fn set_next_block(&mut self, next_block: u64) {
        self.next_block = next_block;
    }

    pub fn next_blockhash<'a>(&mut self, hashbuf: &'a mut [u8]) -> Option<&'a mut [u8]> {
        match self.reader.read(&mut self.buf) {
            Ok(s) => {
                if s == 0 {
                    None
                } else {
                    let mut hasher = Sha3_256::new();
                    hasher.update(&self.buf[0..s]);
                    hasher.finalize_into(GenericArray::from_mut_slice(hashbuf));
                    self.next_block += 1;
                    Some(hashbuf)
                }
            }
            Err(e) => {
                eprintln!("next_blockhash: {:?}", e);
                None
            }
        }
    }

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
