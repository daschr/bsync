use base16ct::lower::encode;
use sha3::{Digest, Sha3_256};
use std::io::prelude::*;
use std::io::{IoSliceMut, SeekFrom};
use std::{fs::File, path::Path};

pub struct bsyncReader {
    reader: File,
    blocksize: usize,
    next_block: usize,
    buf: Vec<u8>,
}

impl bsyncReader {
    pub fn new(file: &str, blocksize: usize) -> std::io::Result<Self> {
        let path = Path::new(file);
        let mut fd: File = File::open(path)?;

        Ok(bsyncReader {
            reader: fd,
            blocksize,
            next_block: 0,
            buf: vec![0; blocksize],
        })
    }

    pub fn get_hash_size() -> usize {
        Sha3_256::output_size()
    }

    pub fn get_next_block(&self) -> usize {
        self.next_block
    }

    pub fn set_next_block(&mut self, next_block: usize) {
        self.next_block = next_block;
    }

    pub fn next_blockhash<'a>(&mut self, hashbuf: &'a mut [u8]) -> Option<&'a mut [u8]> {
        match self.reader.read(&mut self.buf) {
            Ok(s) => {
                if s == 0 {
                    println!("[next_blockhash] read zero");
                    None
                } else {
                    println!(
                        "[next_blockhash] read {} bytes, eln of buf: {}",
                        s,
                        self.buf.len()
                    );
                    let mut hasher = Sha3_256::new();
                    hasher.update(&self.buf[0..s]);
                    let hash = hasher.finalize();
                    self.next_block += 1;
                    println!("hashbuf length: {}", hashbuf.len());
                    encode(&hash, hashbuf).expect("could not encode hash!");
                    Some(hashbuf)
                }
            }
            Err(e) => {
                eprintln!("next_blockhash: {:?}", e);
                None
            }
        }
    }

    pub fn seek(&mut self, block: usize) -> std::io::Result<()> {
        self.reader
            .seek(SeekFrom::Start((self.blocksize * block) as u64))?;
        self.next_block = block;
        Ok(())
    }

    pub fn get_block<'a>(&mut self, block: usize, buf: &'a mut [u8]) -> std::io::Result<usize> {
        self.reader
            .seek(SeekFrom::Start((self.blocksize * block) as u64))?;
        let s: usize = self.reader.read(buf)?;

        Ok(s)
    }
}
