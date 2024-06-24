use async_trait::async_trait;
use core::any::Any;
use pingora::{
    cache::{
        storage::HandleHit, trace::SpanHandle, CacheKey, Storage
    },
    prelude::*,
};
use std::{
    io::{self, ErrorKind},
    path::PathBuf, time::Duration,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    time::timeout,
};

use super::super::error::{e_perror, perror};

pub struct PartialFileHitHandler {
    final_path: PathBuf,
    partial_path: PathBuf,
    is_final: bool,
    fp: File,
    read_size: usize,
}

impl PartialFileHitHandler {
    pub async fn new(final_path: PathBuf, partial_path: PathBuf, read_size: usize) -> Result<Self> {
        let mut is_final = false;
        let fp = match File::open(&partial_path).await {
            Ok(fp) => fp,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                // if partial file failed to open, try to open final file instead
                is_final = true;
                File::open(&final_path)
                .await
                .map_err(|err| perror("error opening data file", err))?
            }
            Err(err) => e_perror("error opening partial data file", err)?
        };
        Ok(Self {
            final_path,
            partial_path,
            is_final,
            fp,
            read_size,
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
        let dur = Duration::from_millis(50);
        let mut buf = vec![0; self.read_size];
        loop {
            let final_before_read = self.is_done();
            match timeout(dur, self.fp.read(&mut buf)).await {
                Ok(result) => {
                    match result {
                        Ok(n) => {
                            // we saw writing was done, then read nothing,
                            // so we know the file has been read completely
                            if n == 0 && final_before_read {
                                return Ok(None)
                            }
                            // we read something, so we can just return it
                            if n > 0 {
                                let b = bytes::Bytes::from(buf);
                                return Ok(Some(b.slice(..n)))
                            }
                        }
                        Err(err) => {
                            return e_perror("error reading from cache", err)
                        }
                    }
                }
                Err(_) => {
                    // we saw writing was done, then read timed out,
                    // so we know the file has been read completely
                    if final_before_read {
                        return Ok(None)
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
