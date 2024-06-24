use async_trait::async_trait;
use core::any::Any;
use log::error;
use pingora::{
    cache::{
        key::CompactCacheKey,
        storage::{HandleHit, HandleMiss},
        trace::SpanHandle,
        CacheKey, CacheMeta, HitHandler, MissHandler, Storage,
    },
    prelude::*,
};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind, Read, Seek, Write},
    path::{Path, PathBuf},
};

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

struct FileHitHandler {
    fp: File,
    read_size: usize,
}

impl FileHitHandler {
    fn new(final_path: PathBuf, read_size: usize) -> Result<FileHitHandler> {
        let fp = File::open(&final_path)
        .map_err(|err| perror("error opening data file", err))?;
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

struct FileMissHandler {
    partial_path: PathBuf,
    final_path: PathBuf,
    meta_path: PathBuf,
    fp: File,
    written: usize,
}

impl FileMissHandler {
    fn new(partial_path: PathBuf, final_path: PathBuf, meta_path: PathBuf) -> Result<Self> {
        let fp = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&partial_path)
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
        match self.fp.write_all(&data) {
            Ok(()) => {
                self.written += data.len();
                Ok(())
            }
            Err(err) => e_perror("error writing to cache", err),
        }
    }

    async fn finish(self: Box<Self>) -> Result<usize> {
        fs::rename(self.partial_path.as_path(), self.final_path.as_path())
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
        if let Err(err) = fs::remove_file($path) { delete_file_error!($path, $fmt, err) }
    };
}

impl Drop for FileMissHandler {
    fn drop(&mut self) {
        match fs::remove_file(&self.partial_path) {
            // if the partial file is not found, no further action is needed
            Err(err) if err.kind() == ErrorKind::NotFound => {},
            // some other error occurred, try to also delete meta file
            Err(err) => {
                delete_file_error!(self.partial_path, "cannot remove unfinished partial file {}: {}", err);
                delete_file!(&self.meta_path, "cannot remove unfinished meta file {}: {}");
            }
            // if the partial file was deleted, we also need to remove the meta file
            _ => {
                delete_file!(&self.meta_path, "cannot remove unfinished meta file {}: {}");
            }
        }
    }
}

#[derive(Debug)]
pub struct FileStorage {
    path: PathBuf,
    read_size: usize,
}

fn path_from_key(dir: &PathBuf, key: &CompactCacheKey, suffix: &str) -> PathBuf {
    let ser = rmp_serde::to_vec(&key).unwrap();
    let ser = const_hex::encode(ser);
    assert_eq!(ser.len() % 2, 0, "path encodes to odd length hex {}", ser);
    dir.join(ser + suffix)
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CacheMetaData<'a> {
    #[serde(with = "serde_bytes")]
    internal: &'a [u8],
    #[serde(with = "serde_bytes")]
    header: &'a [u8],
}

fn serialize_cachemeta(meta: &CacheMeta) -> Result<Vec<u8>> {
    match meta.serialize() {
        Ok((internal, header)) => {
            let wrapper = CacheMetaData {
                internal: &internal,
                header: &header,
            };
            rmp_serde::to_vec(&wrapper).map_err(|err| perror("cannot serialize cachemeta", err))
        }
        Err(err) => Err(err),
    }
}

fn deserialize_cachemeta(data: &[u8]) -> Result<CacheMeta> {
    //assert_eq!((42, "the Answer"), );
    match rmp_serde::from_slice::<CacheMetaData>(&data) {
        Ok(cmd) => CacheMeta::deserialize(cmd.internal, cmd.header),
        Err(err) => e_perror("cannot deserialize cachemeta", err),
    }
}

impl FileStorage {
    pub fn new(path: PathBuf, read_size: usize) -> io::Result<Self> {
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }
        Ok(Self {
            path: path,
            read_size: read_size,
        })
    }

    fn final_data_path(&'static self, key: &CompactCacheKey) -> PathBuf {
        path_from_key(&self.path, key, "")
    }

    fn partial_data_path(&'static self, key: &CompactCacheKey) -> PathBuf {
        path_from_key(&self.path, key, ".partial")
    }

    fn meta_path(&'static self, key: &CompactCacheKey) -> PathBuf {
        path_from_key(&self.path, key, ".meta")
    }

    fn get_cachemeta(&'static self, key: &CompactCacheKey) -> Option<Result<CacheMeta>> {
        let path = self.meta_path(key);
        match File::open(&path) {
            Ok(mut fp) => {
                let mut data = Vec::new();
                match fp.read_to_end(&mut data) {
                    Ok(_) => Some(deserialize_cachemeta(&data)),
                    Err(err) => Some(e_perror("error reading cachemeta", err)),
                }
            }
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    None
                } else {
                    Some(e_perror("error opening cachemeta", err))
                }
            }
        }
    }

    fn put_cachemeta(&'static self, key: &CompactCacheKey, meta: &CacheMeta) -> Result<()> {
        let data = serialize_cachemeta(meta)?;
        let path = self.meta_path(key);
        let mut fp = OpenOptions::new()
            .create(true)
            .read(false)
            .write(true)
            .open(&path)
            .map_err(|err| perror("error opening file", err))?;
        fp.write_all(&data)
            .map_err(|err| perror("error writing to file", err))?;
        Ok(())
    }

    fn pop_cachemeta(&'static self, key: &CompactCacheKey) -> Result<()> {
        let path = self.meta_path(key);
        fs::remove_file(&path).map_err(|err| perror("pop cachemeta", err))?;
        Ok(())
    }

    pub fn purge_sync(&'static self, key: &CompactCacheKey) -> Result<bool> {
        let data_path = self.final_data_path(key);
        let err_meta = self.pop_cachemeta(key);
        let err_data = fs::remove_file(&data_path);
        match (err_meta, err_data) {
            (Ok(()), Ok(())) => Ok(true),
            (Err(e1), Ok(())) => e_perror("Failed to remove cachemeta {}", e1),
            (Ok(()), Err(e2)) => e_perror(
                format!("Failed to remove data file {}", data_path.display()),
                e2,
            ),
            (Err(err_meta), Err(err_data)) => e_perror(
                format!(
                    "Failed to remove cachemeta {} and data: {}",
                    err_meta,
                    data_path.display(),
                ),
                err_data,
            ),
        }
    }
}

fn ensure_parent_dirs_exist(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            if let Err(err) = fs::create_dir_all(parent) {
                return e_perror("error creating cache dir", err);
            }
        }
    }
    Ok(())
}

#[async_trait]
impl Storage for FileStorage {
    async fn lookup(
        &'static self,
        key: &CacheKey,
        _trace: &SpanHandle,
    ) -> Result<Option<(CacheMeta, HitHandler)>> {
        let key = key.to_compact();
        // don't return hit if data is partial file
        if self.partial_data_path(&key).exists() {
            return Ok(None)
        }
        match self.get_cachemeta(&key) {
            Some(Ok(meta)) => {
                let final_path = self.final_data_path(&key);
                let h = FileHitHandler::new(final_path, self.read_size)?;
                Ok(Some((meta, Box::new(h))))
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
        let key = key.to_compact();
        self.put_cachemeta(&key, meta)?;
        let partial_path = self.partial_data_path(&key);
        let final_path = self.final_data_path(&key);
        let meta_path = self.meta_path(&key);
        ensure_parent_dirs_exist(&partial_path)?;
        ensure_parent_dirs_exist(&final_path)?;
        ensure_parent_dirs_exist(&meta_path)?;
        let h = FileMissHandler::new(partial_path, final_path, meta_path)?;
        Ok(Box::new(h))
    }

    async fn purge(&'static self, key: &CompactCacheKey, _trace: &SpanHandle) -> Result<bool> {
        self.purge_sync(key)
    }

    async fn update_meta(
        &'static self,
        key: &CacheKey,
        meta: &CacheMeta,
        _trace: &SpanHandle,
    ) -> Result<bool> {
        self.put_cachemeta(&key.to_compact(), meta)?;
        Ok(true)
    }

    fn support_streaming_partial_write(&self) -> bool {
        false
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync + 'static) {
        self
    }
}
