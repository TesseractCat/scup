use kdam::tqdm;
use sha2::{Digest, Sha256};
use std::{io::Read, path::Path};

use crate::rollsum::Rollsum;
use crate::{ObjectId, rollsum};

pub(crate) struct ChunkReader<R: Read> {
    reader: R,
    rs: Rollsum,
    read_buf: Vec<u8>,
    read_pos: usize,
    read_len: usize,
    chunk_buf: Vec<u8>,
    done: bool,
}

impl<R: Read> Iterator for ChunkReader<R> {
    type Item = std::io::Result<(ObjectId, Vec<u8>, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            // Refill the read buffer when exhausted.
            if self.read_pos >= self.read_len {
                self.read_len = match self.reader.read(&mut self.read_buf) {
                    Ok(n) => n,
                    Err(e) => return Some(Err(e)),
                };
                self.read_pos = 0;
                if self.read_len == 0 {
                    // EOF: emit the final chunk if any data remains.
                    self.done = true;
                    return if self.chunk_buf.is_empty() {
                        None
                    } else {
                        let data = std::mem::replace(
                            &mut self.chunk_buf,
                            Vec::with_capacity(rollsum::AVERAGE_CHUNK_SIZE),
                        );
                        let id: ObjectId = <[u8; 32]>::from(Sha256::digest(&data)).into();
                        Some(Ok((id, data, self.rs.digest())))
                    };
                }
            }

            // Scan read_buf for a chunk boundary, deferring data copy until one is found.
            let start = self.read_pos;
            while self.read_pos < self.read_len {
                let byte = self.read_buf[self.read_pos];
                self.read_pos += 1;
                self.rs.roll(byte);

                if self.rs.digest() & rollsum::SPLIT_MASK == 0
                    || self.chunk_buf.len() + (self.read_pos - start) >= rollsum::MAX_CHUNK_SIZE
                {
                    self.chunk_buf
                        .extend_from_slice(&self.read_buf[start..self.read_pos]);
                    let data = std::mem::replace(
                        &mut self.chunk_buf,
                        Vec::with_capacity(rollsum::AVERAGE_CHUNK_SIZE),
                    );
                    let id: ObjectId = <[u8; 32]>::from(Sha256::digest(&data)).into();
                    return Some(Ok((id, data, self.rs.digest())));
                }
            }
            // No boundary in this buffer — bulk copy and refill.
            self.chunk_buf
                .extend_from_slice(&self.read_buf[start..self.read_pos]);
        }
    }
}

pub(crate) fn split_chunks<R: Read>(reader: R) -> ChunkReader<R> {
    ChunkReader {
        reader,
        rs: Rollsum::new(),
        read_buf: vec![0u8; 64 * 1024],
        read_pos: 0,
        read_len: 0,
        chunk_buf: Vec::new(),
        done: false,
    }
}

pub(crate) fn debug_chunk_file(path: &Path) {
    let file = std::fs::File::open(path).expect("failed to open file");
    let mut offset = 0usize;

    let size = file.metadata().unwrap().len() as usize;
    let chunks = size / rollsum::AVERAGE_CHUNK_SIZE;
    for chunk in tqdm!(
        split_chunks(file),
        desc = "Chunking",
        total = chunks,
        position = 1
    ) {
        let (_id, data, _digest) = chunk.expect("failed to read file");
        let end = offset + data.len();
        //println!("chunk [{offset}, {end}), len={}", data.len());
        offset = end;
    }
}
