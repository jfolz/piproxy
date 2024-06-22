use crate::defaults::*;
use async_trait::async_trait;
use core::any::Any;
use log::error;
use once_cell::sync::OnceCell;
use pingora::{
    cache::{
        key::{CacheHashKey, CompactCacheKey},
        storage::{HandleHit, HandleMiss},
        trace::SpanHandle,
        CacheKey, CacheMeta, HitHandler, MissHandler, Storage,
    },
    prelude::*,
};
use std::{
    ffi::{OsStr, OsString},
    io::{self, ErrorKind},
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};
use std::{fs, io::Read};
use std::{fs::create_dir_all, io::Seek, path::Path};
use std::{
    fs::{File, OpenOptions},
    io::Write,
};

pub static READ_SIZE: OnceCell<usize> = OnceCell::new();

fn perror<S: Into<ImmutStr>, E: Into<Box<dyn ErrorTrait + Send + Sync>>>(
    context: S,
    cause: E,
) -> pingora::BError {
    pingora::Error::because(pingora::ErrorType::InternalError, context, cause)
}

fn e_perror<T, S: Into<ImmutStr>, E: Into<Box<dyn ErrorTrait + Send + Sync>>>(
    context: S,
    cause: E,
) -> Result<T> {
    Err(perror(context, cause))
}

fn append_to_path(p: impl Into<OsString>, s: impl AsRef<OsStr>) -> PathBuf {
    let mut p = p.into();
    p.push(s);
    p.into()
}

fn path_to_str(path: impl Into<OsString>) -> String {
    path.into().into_string().unwrap()
}

struct FileHitHandler {
    fp: File,
    read_size: usize,
}

impl FileHitHandler {
    fn new(fp: File) -> FileHitHandler {
        Self {
            fp: fp,
            read_size: *READ_SIZE.get().unwrap_or(&DEFAULT_CHUNK_SIZE),
        }
    }

    fn new_box(fp: File) -> Box<FileHitHandler> {
        Box::new(Self::new(fp))
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

struct FileMissHandler {
    path: PathBuf,
    fp: File,
    written: usize,
    finished: AtomicBool,
}

impl FileMissHandler {
    fn new(path: PathBuf, fp: File) -> Self {
        Self {
            path: path,
            fp: fp,
            written: 0,
            finished: false.into(),
        }
    }

    fn new_box(path: PathBuf, fp: File) -> Box<Self> {
        Box::new(Self::new(path, fp))
    }
}

#[async_trait]
impl HandleMiss for FileMissHandler {
    async fn write_body(&mut self, data: bytes::Bytes, _eof: bool) -> Result<()> {
        match self.fp.write_all(&data) {
            Ok(()) => {
                self.written += data.len();
                Ok(())
            }
            Err(err) => e_perror("error writing to cache", err),
        }
    }

    async fn finish(self: Box<Self>) -> Result<usize> {
        // remember that the write finished properly
        self.finished.store(true, Ordering::Relaxed);
        Ok(self.written)
    }
}

impl Drop for FileMissHandler {
    fn drop(&mut self) {
        let finished: bool = self.finished.load(Ordering::Relaxed);
        if !finished {
            if let Err(err) = fs::remove_file(&self.path) {
                error!(
                    "cannot remove unfinished file {}: {}",
                    path_to_str(&self.path),
                    err
                );
            }
            let meta_path = append_to_path(&self.path, ".meta");
            if let Err(err) = fs::remove_file(&meta_path) {
                error!(
                    "cannot remove unfinished file {}: {}",
                    path_to_str(&meta_path),
                    err
                );
            }
        }
    }
}

#[derive(Debug)]
pub struct FileStorage {
    path: PathBuf,
}

fn path_from_key (dir: &PathBuf, key: &CompactCacheKey, suffix: &str) -> PathBuf {
    dir.join(key.combined() + suffix)
}

impl FileStorage {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }
        Ok(Self { path: path })
    }

    fn data_path (&'static self, key: &CacheKey) -> PathBuf {
        self.data_path_from_compact(&key.to_compact())
    }
    fn data_path_from_compact(&'static self, key: &CompactCacheKey) -> PathBuf {
        path_from_key(&self.path, key, "")
    }
    fn meta_path(&'static self, key: &CacheKey) -> PathBuf {
        self.meta_path_from_compact(&key.to_compact())
    }
    fn meta_path_from_compact(&'static self, key: &CompactCacheKey) -> PathBuf {
        path_from_key(&self.path, key, ".meta")
    }
}

fn ensure_parent_dirs_exist(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            if let Err(err) = create_dir_all(parent) {
                return e_perror("error creating cache dir", err);
            }
        }
    }
    Ok(())
}

fn serialize_cachemeta(meta: &CacheMeta) -> Result<Vec<u8>> {
    match meta.serialize() {
        Ok((internal, header)) => {
            let prefix = (internal.len() as u64).to_be_bytes();
            let mut buf = Vec::with_capacity(prefix.len() + internal.len() + header.len());
            buf.write(&prefix).unwrap();
            buf.write(&internal).unwrap();
            buf.write(&header).unwrap();
            Ok(buf)
        }
        Err(err) => Err(err),
    }
}

fn deserialize_cachemeta(data: &[u8]) -> Result<CacheMeta> {
    let prefix_bytes = <[u8; 8]>::try_from(&data[..8]).unwrap();
    let prefix = u64::from_be_bytes(prefix_bytes) as usize;
    let internal = &data[prefix_bytes.len()..prefix_bytes.len() + prefix];
    let header = &data[prefix_bytes.len() + prefix..];
    CacheMeta::deserialize(internal, header)
}

fn store_cachemeta(meta: &CacheMeta, path: &Path) -> Result<()> {
    match serialize_cachemeta(meta) {
        Ok(meta_data) => {
            match OpenOptions::new()
                .create(true)
                .read(false)
                .write(true)
                .open(&path)
            {
                Ok(mut fp) => {
                    if let Err(err) = fp.write_all(&meta_data) {
                        e_perror("error writing cachemeta", err)
                    } else {
                        Ok(())
                    }
                }
                Err(err) => return e_perror("error storing cachemeta", err),
            }
        }
        Err(err) => return Err(err),
    }
}

fn load_cachemeta(path: &Path) -> Option<Result<CacheMeta>> {
    match File::open(&path) {
        Ok(mut fp) => {
            let mut data = Vec::new();
            match fp.read_to_end(&mut data) {
                Ok(_) => Some(deserialize_cachemeta(&data)),
                Err(err) => Some(e_perror("error reading cachemeta", err))
            }
        }
        Err(err) => {
            if err.kind() == ErrorKind::NotFound { None }
            else { Some(e_perror("error opening cachemeta", err)) }
        }
    }
}

#[async_trait]
impl Storage for FileStorage {
    async fn lookup(
        &'static self,
        key: &CacheKey,
        _trace: &SpanHandle,
    ) -> Result<Option<(CacheMeta, HitHandler)>> {
        match load_cachemeta(&self.meta_path(key)) {
            Some(Ok(meta)) => {
                let data_path = self.data_path(key);
                match File::open(&data_path) {
                    Ok(fp) => {
                        Ok(Some((meta, FileHitHandler::new_box(fp))))
                    }
                    Err(err) => {
                        e_perror("error accessing cached data", err)
                    }
                }
            }
            Some(Err(err)) => Err(err),
            None => Ok(None),
        }
    }

    async fn get_miss_handler(
        &'static self,
        key: &CacheKey,
        meta: &CacheMeta,
        _trace: &SpanHandle,
    ) -> Result<MissHandler> {
        let meta_path = self.meta_path(key);
        ensure_parent_dirs_exist(&meta_path)?;
        store_cachemeta(meta, &meta_path)?;

        let data_path = self.data_path(key);
        ensure_parent_dirs_exist(&data_path)?;

        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&data_path)
        {
            Ok(fp) => {
                Ok(FileMissHandler::new_box(data_path, fp))
            }
            Err(err) => {
                Err(pingora::Error::because(
                    pingora::ErrorType::InternalError,
                    "error opening cache",
                    err,
                ))
            }
        }
    }

    async fn purge(&'static self, key: &CompactCacheKey, _trace: &SpanHandle) -> Result<bool> {
        log::info!("purge {}", key.primary());
        let meta_path = self.meta_path_from_compact(key);
        let data_path = self.data_path_from_compact(key);
        let err_meta = fs::remove_file(&meta_path);
        let err_data = fs::remove_file(&data_path);
        match (err_meta, err_data) {
            (Ok(()), Ok(())) => Ok(true),
            (Err(e1), Ok(())) => e_perror(
                format!("Failed to remove meta file {}", meta_path.display()),
                e1,
            ),
            (Ok(()), Err(e2)) => e_perror(
                format!("Failed to remove data file {}", data_path.display()),
                e2,
            ),
            (Err(e1), Err(e2)) => e_perror(
                format!(
                    "Failed to remove meta and data files {}: {}, {}",
                    meta_path.display(),
                    e1,
                    data_path.display(),
                ),
                e2,
            ),
        }
    }

    async fn update_meta(
        &'static self,
        key: &CacheKey,
        meta: &CacheMeta,
        _trace: &SpanHandle,
    ) -> Result<bool> {
        let meta_path = self.meta_path(key);
        if let Err(err) = ensure_parent_dirs_exist(&meta_path) {
            return Err(err);
        }
        if let Err(err) = store_cachemeta(meta, &meta_path) {
            return Err(err);
        }
        Ok(true)
    }

    fn support_streaming_partial_write(&self) -> bool {
        false
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync + 'static) {
        self
    }
}
