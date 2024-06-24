use async_trait::async_trait;
use core::any::Any;
use pingora::{
    cache::{
        storage::HandleHit, trace::SpanHandle, CacheKey, Storage
    },
    prelude::*,
};
use std::{
    fs::File,
    io::{Read, Seek},
    path::PathBuf,
};

use super::super::error::{e_perror, perror};

pub struct FileHitHandler {
    fp: File,
    read_size: usize,
}

impl FileHitHandler {
    pub fn new(final_path: PathBuf, read_size: usize) -> Result<FileHitHandler> {
        let fp = File::open(&final_path).map_err(|err| perror("error opening data file", err))?;
        Ok(Self {
            fp: fp,
            read_size: read_size,
        })
    }
}

#[async_trait]
impl HandleHit for FileHitHandler {
    async fn read_body(&mut self) -> Result<Option<bytes::Bytes>> {
        let mut buf = vec![0; self.read_size];
        match self.fp.read(&mut buf) {
            Ok(n) => {
                if n > 0 {
                    let b = bytes::Bytes::from(buf);
                    Ok(Some(b.slice(..n)))
                } else {
                    Ok(None)
                }
            }
            Err(e) => e_perror("error reading from cache", e),
        }
    }

    async fn finish(
        self: Box<Self>,
        _storage: &'static (dyn Storage + Sync),
        _key: &CacheKey,
        _trace: &SpanHandle,
    ) -> Result<()> {
        Ok(())
    }

    fn can_seek(&self) -> bool {
        true
    }

    fn seek(&mut self, start: usize, _end: Option<usize>) -> Result<()> {
        if let Err(err) = self.fp.seek(std::io::SeekFrom::Start(start as u64)) {
            return e_perror("error seeking in cache", err);
        }
        Ok(())
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        todo!("as_any")
    }
}
