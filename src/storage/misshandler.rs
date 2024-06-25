use async_trait::async_trait;
use log::error;
use pingora::{cache::storage::HandleMiss, prelude::*};
use std::{io::ErrorKind, path::PathBuf};
use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncWriteExt,
};

use super::super::error::{e_perror, perror};

pub struct FileMissHandler {
    partial_path: PathBuf,
    final_path: PathBuf,
    meta_path: PathBuf,
    fp: File,
    written: usize,
}

impl FileMissHandler {
    pub async fn new(
        partial_path: PathBuf,
        final_path: PathBuf,
        meta_path: PathBuf,
    ) -> Result<Self> {
        let fp = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&partial_path)
            .await
            .map_err(|err| perror("error creating partial file", err))?;
        Ok(Self {
            partial_path,
            final_path,
            meta_path,
            fp: fp,
            written: 0,
        })
    }
}

#[async_trait]
impl HandleMiss for FileMissHandler {
    async fn write_body(&mut self, data: bytes::Bytes, _eof: bool) -> Result<()> {
        match self.fp.write_all(&data).await {
            Ok(()) => {
                self.written += data.len();
                Ok(())
            }
            Err(err) => e_perror("error writing to cache", err),
        }
    }

    async fn finish(self: Box<Self>) -> Result<usize> {
        fs::rename(self.partial_path.as_path(), self.final_path.as_path())
            .await
            .map_err(|err| perror("cannot rename partial to final", err))?;
        Ok(self.written)
    }
}

macro_rules! delete_file_error {
    ($path:expr, $fmt:expr, $err:expr) => {
        error!($fmt, $path.to_string_lossy(), $err)
    };
}

macro_rules! delete_file {
    ($path:expr, $fmt:expr) => {
        if let Err(err) = fs::remove_file($path).await {
            delete_file_error!($path, $fmt, err)
        }
    };
}

async fn cleanup(partial_path: PathBuf, meta_path: PathBuf) {
    match fs::remove_file(&partial_path).await {
        // if the partial file is not found, no further action is needed
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        // some other error occurred, try to also delete meta file
        Err(err) => {
            delete_file_error!(
                &partial_path,
                "cannot remove unfinished partial file {}: {}",
                err
            );
            delete_file!(&meta_path, "cannot remove unfinished meta file {}: {}");
        }
        // if the partial file was deleted, we also need to remove the meta file
        _ => {
            delete_file!(&meta_path, "cannot remove unfinished meta file {}: {}");
        }
    }
}

impl Drop for FileMissHandler {
    fn drop(&mut self) {
        let partial_path = self.partial_path.clone();
        let meta_path = self.meta_path.clone();
        tokio::task::spawn(async move { cleanup(partial_path, meta_path) });
    }
}
