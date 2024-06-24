use async_trait::async_trait;
use core::any::Any;
use pingora::{
    cache::{
        storage::HandleHit, trace::SpanHandle, CacheKey, Storage
    },
    prelude::*,
};
use std::path::PathBuf;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use super::super::error::{e_perror, perror};

pub struct FileHitHandler {
    fp: File,
    buf: Vec<u8>,
}

impl FileHitHandler {
    pub async fn new(final_path: PathBuf, read_size: usize) -> Result<FileHitHandler> {
        let fp = File::open(&final_path)
            .await
            .map_err(|err| perror("error opening data file", err))?;
        let buf = vec![0; read_size];
        Ok(Self{fp, buf})
    }
}

#[async_trait]
impl HandleHit for FileHitHandler {
    async fn read_body(&mut self) -> Result<Option<bytes::Bytes>> {
        match self.fp.read(&mut self.buf).await {
            Ok(n) => {
                if n > 0 {
                    let b = bytes::Bytes::from(self.buf[..n].to_owned());
                    Ok(Some(b))
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
        tokio::runtime::Handle::current().block_on(async {
            self.fp.seek(std::io::SeekFrom::Start(start as u64))
            .await
            .map_err(|err| perror("error seeking in cache", err))
            .map(|_| ())
        })
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        todo!("as_any")
    }
}
