use async_trait::async_trait;
use bytes::BytesMut;
use core::any::Any;
use pingora::{
    cache::{storage::HandleHit, trace::SpanHandle, CacheKey, Storage},
    prelude::*,
};
use std::{io::ErrorKind, path::PathBuf, time::Duration};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    time::timeout,
};

use super::super::error::{e_perror, perror};

pub struct PartialFileHitHandler {
    final_path: PathBuf,
    is_final: bool,
    fp: File,
    read_size: usize,
    read_timeout: Duration,
}

impl PartialFileHitHandler {
    pub async fn new(partial_path: PathBuf, final_path: PathBuf, read_size: usize) -> Result<Self> {
        let mut is_final = false;
        let fp = match File::open(&partial_path).await {
            Ok(fp) => fp,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                // if partial file failed to open, try to open final file instead
                is_final = true;
                File::open(&final_path)
                    .await
                    .map_err(|err| perror("error opening data file", err))?
            }
            Err(err) => e_perror("error opening partial data file", err)?,
        };
        let read_timeout = Duration::from_millis(100);
        Ok(Self {
            final_path,
            is_final,
            fp,
            read_size,
            read_timeout,
        })
    }

    fn is_done(&mut self) -> bool {
        self.is_final = self.is_final || self.final_path.exists();
        self.is_final
    }
}

#[async_trait]
impl HandleHit for PartialFileHitHandler {
    async fn read_body(&mut self) -> Result<Option<bytes::Bytes>> {
        loop {
            let final_before_read = self.is_done();
            let mut buf = BytesMut::zeroed(self.read_size);
            match timeout(self.read_timeout, self.fp.read(buf.as_mut())).await {
                Ok(result) => {
                    match result {
                        Ok(n) => {
                            // we saw writing was done, then read nothing,
                            // so we know the file has been read completely
                            if n == 0 && final_before_read {
                                return Ok(None);
                            }
                            // we read something, so we can just return it
                            if n > 0 {
                                let buf = buf.freeze();
                                return Ok(Some(buf.slice(..n)))
                            }
                        }
                        Err(err) => return e_perror("error reading from cache", err),
                    }
                }
                Err(_) => {
                    // we saw writing was done, then read timed out,
                    // so we know the file has been read completely
                    if final_before_read {
                        return Ok(None);
                    }
                }
            }
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
