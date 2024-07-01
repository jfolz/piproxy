use async_trait::async_trait;
use core::any::Any;
use pingora::{
    cache::{
        key::CompactCacheKey, trace::SpanHandle, CacheKey, CacheMeta, HitHandler, MissHandler,
        Storage,
    },
    prelude::*,
};
use std::{
    io::{self, ErrorKind},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::hithandler::FileHitHandler;
use super::misshandler::FileMissHandler;
use super::{
    super::error::{e_perror, perror},
    partialhithandler::PartialFileHitHandler,
};

#[derive(Debug)]
pub struct FileStorage {
    path: PathBuf,
    read_size: usize,
}

use blake2::{Blake2b, Digest};

pub(crate) type Blake2b32 = Blake2b<blake2::digest::consts::U3>;

fn path_from_key(dir: &Path, key: &CompactCacheKey, suffix: &str) -> Result<PathBuf> {
    let ser = rmp_serde::to_vec(&key).map_err(|err| perror("cannot serialize cachekey", err))?;
    let hex_ser = const_hex::encode(&ser);
    assert_eq!(hex_ser.len() % 2, 0, "path encodes to odd length hex {hex_ser}");

    let mut hasher = Blake2b32::new();
    hasher.update(ser);
    let hash = hasher.finalize();
    let prefix_dirs: Vec<String> = hash.iter().map(|b| const_hex::encode([*b])).collect();
    assert_eq!(prefix_dirs.len(), 3, "path should have 3 prefix dirs, not {}", prefix_dirs.len());

    let mut out = dir.to_owned();
    for prefix in prefix_dirs {
        out = out.join(prefix);
    }
    Ok(out.join(hex_ser + suffix))
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
    match rmp_serde::from_slice::<CacheMetaData>(data) {
        Ok(cmd) => CacheMeta::deserialize(cmd.internal, cmd.header),
        Err(err) => e_perror("cannot deserialize cachemeta", err),
    }
}

impl FileStorage {
    pub fn new(path: PathBuf, read_size: usize) -> io::Result<Self> {
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }
        Ok(Self { path, read_size })
    }

    fn final_data_path(&'static self, key: &CompactCacheKey) -> Result<PathBuf> {
        // TODO encode .data in type system?
        path_from_key(&self.path, key, ".data")
    }

    fn partial_data_path(&'static self, key: &CompactCacheKey) -> Result<PathBuf> {
        // TODO encode .partial in type system?
        path_from_key(&self.path, key, ".partial")
    }

    fn meta_path(&'static self, key: &CompactCacheKey) -> Result<PathBuf> {
        // TODO encode .meta in type system?
        path_from_key(&self.path, key, ".meta")
    }

    async fn get_cachemeta(&'static self, key: &CompactCacheKey) -> Result<Option<CacheMeta>> {
        let path = self.meta_path(key)?;
        match File::open(path).await {
            Ok(mut fp) => {
                let mut data = Vec::new();
                match fp.read_to_end(&mut data).await {
                    Ok(_) => Ok(Some(deserialize_cachemeta(&data)?)),
                    Err(err) => e_perror("error reading cachemeta", err),
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => e_perror("error opening cachemeta", err),
        }
    }

    async fn put_cachemeta(&'static self, key: &CompactCacheKey, meta: &CacheMeta) -> Result<()> {
        let data = serialize_cachemeta(meta)?;
        let path = self.meta_path(key)?;
        ensure_parent_dirs_exist(&path).await?;
        let mut fp = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(false)
            .write(true)
            .open(path)
            .await
            .map_err(|err| perror("error opening file", err))?;
        fp.write_all(&data)
            .await
            .map_err(|err| perror("error writing to file", err))?;
        Ok(())
    }

    fn pop_cachemeta_sync(&'static self, key: &CompactCacheKey) -> Result<()> {
        let path = self.meta_path(key)?;
        std::fs::remove_file(path).map_err(|err| perror("pop cachemeta", err))?;
        Ok(())
    }

    pub fn purge_sync(&'static self, key: &CompactCacheKey) -> Result<bool> {
        let data_path = self.final_data_path(key)?;
        let err_meta = self.pop_cachemeta_sync(key);
        let err_data = std::fs::remove_file(&data_path);
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

async fn ensure_parent_dirs_exist(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            if let Err(err) = fs::create_dir_all(parent).await {
                return e_perror("error creating cache dir", err);
            }
        }
    }
    Ok(())
}

fn content_length(meta: &CacheMeta) -> Option<u64> {
    meta.response_header()
        .headers
        .get("Content-Length")?
        .to_str()
        .ok()
        .and_then(|s| s.parse().ok())
}

fn is_chunked_encoding(meta: &CacheMeta) -> bool {
    meta.response_header()
    .headers
    .get("Transfer-Encoding").is_some_and(|h| b"chunked".eq_ignore_ascii_case(h.as_bytes()))
}

fn has_correct_size(meta: &CacheMeta, path: &Path) -> Result<bool> {
    let Some(header_size) = content_length(meta) else {
        // TODO determine content-length for responses with chunked encoding
        if is_chunked_encoding(meta) {
            return Ok(true)
        }
        return Err(Error::explain(
            ErrorType::InternalError,
            "cannot determine content-length",
        ));
    };
    let file_size = path
        .metadata()
        .map_err(|err| perror("cannot determine size of cached file", err))?
        .size();
    Ok(header_size == file_size)
}

#[async_trait]
impl Storage for FileStorage {
    async fn lookup(
        &'static self,
        key: &CacheKey,
        _trace: &SpanHandle,
    ) -> Result<Option<(CacheMeta, HitHandler)>> {
        let key = key.to_compact();
        match self.get_cachemeta(&key).await {
            Ok(Some(meta)) => {
                let meta_path = self.meta_path(&key)?;
                let final_path = self.final_data_path(&key)?;
                let partial_path = self.partial_data_path(&key)?;
                if final_path.exists() && has_correct_size(&meta, &final_path)? {
                    let h = Box::new(FileHitHandler::new(final_path, self.read_size).await?);
                    Ok(Some((meta, h)))
                } else if partial_path.exists() {
                    let h = Box::new(
                        PartialFileHitHandler::new(partial_path, final_path, self.read_size)
                            .await?,
                    );
                    Ok(Some((meta, h)))
                } else {
                    log::warn!(
                        "cachemeta {} exists, but no corresponding data file found",
                        meta_path.to_string_lossy()
                    );
                    Ok(None)
                }
            }
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn get_miss_handler(
        &'static self,
        key: &CacheKey,
        meta: &CacheMeta,
        _trace: &SpanHandle,
    ) -> Result<MissHandler> {
        let key = key.to_compact();
        self.put_cachemeta(&key, meta).await?;
        let partial_path = self.partial_data_path(&key)?;
        let final_path = self.final_data_path(&key)?;
        let meta_path = self.meta_path(&key)?;
        ensure_parent_dirs_exist(&partial_path).await?;
        ensure_parent_dirs_exist(&final_path).await?;
        ensure_parent_dirs_exist(&meta_path).await?;
        match fs::remove_file(&final_path).await {
            Ok(()) => log::warn!(
                "removed existing final data file {}",
                &final_path.to_string_lossy()
            ),
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => {
                return e_perror(
                    format!(
                        "final file {} already exists, but cannot be removed",
                        &final_path.to_string_lossy()
                    ),
                    err,
                )
            }
        }
        let h = FileMissHandler::new(partial_path, final_path, meta_path).await?;
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
        self.put_cachemeta(&key.to_compact(), meta).await?;
        Ok(true)
    }

    fn support_streaming_partial_write(&self) -> bool {
        true
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync + 'static) {
        self
    }
}
