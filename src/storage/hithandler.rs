use async_trait::async_trait;
use bytes::BytesMut;
use core::any::Any;
use pingora::{
    cache::{storage::HandleHit, trace::SpanHandle, CacheKey, Storage},
    prelude::*,
};
use std::path::PathBuf;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use super::super::error::{e_perror, perror};

pub struct FileHitHandler {
    final_path: PathBuf,
    fp: File,
    read_size: usize,
}

impl FileHitHandler {
    pub async fn new(final_path: PathBuf, read_size: usize) -> Result<FileHitHandler> {
        log::info!("new hit handler {}", final_path.to_string_lossy());
        let fp = File::open(&final_path)
            .await
            .map_err(|err| perror("error opening data file", err))?;
        Ok(Self {
            final_path,
            fp,
            read_size,
        })
    }
}

#[async_trait]
impl HandleHit for FileHitHandler {
    async fn read_body(&mut self) -> Result<Option<bytes::Bytes>> {
        let mut buf = BytesMut::zeroed(self.read_size);
        match self.fp.read(buf.as_mut()).await {
            Ok(n) => {
                if n > 0 {
                    let buf = buf.freeze();
                    Ok(Some(buf.slice(..n)))
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
        log::info!("finish hit handler {}", self.final_path.to_string_lossy());
        Ok(())
    }

    fn can_seek(&self) -> bool {
        true
    }

    fn seek(&mut self, start: usize, _end: Option<usize>) -> Result<()> {
        tokio::runtime::Handle::current().block_on(async {
            self.fp
                .seek(std::io::SeekFrom::Start(start as u64))
                .await
                .map_err(|err| perror("error seeking in cache", err))
                .map(|_| ())
        })
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        todo!("as_any")
    }
}
