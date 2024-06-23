use crate::defaults::*;
use async_trait::async_trait;
use rusqlite::Connection;
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
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{atomic::{AtomicBool, Ordering}, Mutex},
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
                    self.path.to_string_lossy(),
                    err
                );
            }
            let meta_path = append_to_path(&self.path, ".meta");
            if let Err(err) = fs::remove_file(&meta_path) {
                error!(
                    "cannot remove unfinished file {}: {}",
                    meta_path.to_string_lossy(),
                    err
                );
            }
        }
    }
}

#[derive(Debug)]
pub struct FileStorage {
    path: PathBuf,
    conn: Mutex<Connection>,
}

fn path_from_key (dir: &PathBuf, key: &CompactCacheKey, suffix: &str) -> PathBuf {
    dir.join(key.combined() + suffix)
}

fn ioerror(err: rusqlite::Error) -> io::Error {
    io::Error::new(ErrorKind::Other, err)
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CacheMetaData<'a>{
    #[serde(with = "serde_bytes")]
    internal: &'a [u8],
    #[serde(with = "serde_bytes")]
    header: &'a [u8],
}

fn serialize_cachemeta(meta: &CacheMeta) -> Result<Vec<u8>> {
    match meta.serialize() {
        Ok((internal, header)) => {
            let wrapper = CacheMetaData{ internal: &internal, header: &header };
            rmp_serde::to_vec(&wrapper)
            .map_err(|err| perror("cannot serialize cachemeta", err))
        }
        Err(err) => Err(err),
    }
}

fn deserialize_cachemeta(data: &[u8]) -> Result<CacheMeta> {
    //assert_eq!((42, "the Answer"), );
    match rmp_serde::from_slice::<CacheMetaData>(&data) {
        Ok(cmd) => CacheMeta::deserialize(cmd.internal, cmd.header),
        Err(err) => e_perror("cannot deserialize cachemeta", err)
    }
}

impl FileStorage {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }
        let conn = Connection::open(path.join("state.sqlite"))
        .map_err(|err| ioerror(err))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS cachemeta (
                cachekey BLOB PRIMARY KEY,
                internal BLOB NOT NULL,
                header   BLOB NOT NULL
            )",
            (), // empty list of parameters.
        ).map_err(|err| ioerror(err))?;

        Ok(Self { path: path, conn: Mutex::new(conn) })
    }

    fn data_path(&'static self, key: &CompactCacheKey) -> PathBuf {
        path_from_key(&self.path, key, "")
    }

    fn meta_path(&'static self, key: &CompactCacheKey) -> PathBuf {
        path_from_key(&self.path, key, ".meta")
    }

    fn get_cachemeta(&'static self, key: &CompactCacheKey) -> Option<Result<CacheMeta>> {
        /*
        let cachekey = key.combined_bin();
        let conn = self.conn.lock().unwrap();
        let mut query = conn.prepare("SELECT internal, header FROM cachemeta WHERE cachekey = ?1").unwrap();
        let row: std::result::Result<(Vec<u8>, Vec<u8>), rusqlite::Error> = query.query_row(
            (&cachekey,),
            |row| Ok((row.get(0)?, row.get(1)?))
        );
        match row {
            Ok((internal, header)) => {
                match CacheMeta::deserialize(&internal, &header) {
                    Ok(meta) => return Some(Ok(meta)),
                    Err(err) => return Some(Err(err)),
                }
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => return None,
            Err(err) => return Some(e_perror("sqlite", err))
        }
        */
        let path = self.meta_path(key);
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

    fn put_cachemeta(&'static self, key: &CompactCacheKey, meta: &CacheMeta) -> Result<()> {
        /*
        let (internal, header) = meta.serialize()?;
        let cachekey = key.combined_bin();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO cachemeta VALUES (?1, ?2, ?3) 
            ON CONFLICT DOUPDATE SET 
            internal=excluded.internal, header=excluded.header",
            (&cachekey, internal, header),
        ).map_err(|err| perror("put cachemeta", err))?;
        Ok(())
        */
        let data = serialize_cachemeta(meta)?;
        let path = self.meta_path(key);
        let mut fp = OpenOptions::new().create(true).read(false).write(true).open(&path)
            .map_err(|err| perror("error opening file", err))?;
        fp.write_all(&data)
            .map_err(|err| perror("error writing to file", err))?;
        Ok(())
    }

    fn pop_cachemeta(&'static self, key: &CompactCacheKey) -> Result<()> {
        /*
        let cachekey = key.combined_bin();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM cachemeta WHERE cachekey = ?1",
            (&cachekey,),
        ).map_err(|err| perror("pop cachemeta", err))?;
        Ok(())
        */
        let path = self.meta_path(key);
        fs::remove_file(&path).map_err(|err| perror("pop cachemeta", err))?;
        Ok(())
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
        match self.get_cachemeta(&key.to_compact()) {
            Some(Ok(meta)) => {
                let data_path = self.data_path(&key.to_compact());
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
        self.put_cachemeta(&key.to_compact(), meta)?;

        let data_path = self.data_path(&key.to_compact());
        ensure_parent_dirs_exist(&data_path)?;

        match OpenOptions::new().create(true).read(true).write(true).open(&data_path) {
            Ok(fp) => Ok(FileMissHandler::new_box(data_path, fp)),
            Err(err) => e_perror("error opening cache", err),
        }
    }

    async fn purge(&'static self, key: &CompactCacheKey, _trace: &SpanHandle) -> Result<bool> {
        let data_path = self.data_path(key);
        let err_meta = self.pop_cachemeta(key);
        let err_data = fs::remove_file(&data_path);
        match (err_meta, err_data) {
            (Ok(()), Ok(())) => Ok(true),
            (Err(e1), Ok(())) => e_perror(
                "Failed to remove cachemeta {}",
                e1,
            ),
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
