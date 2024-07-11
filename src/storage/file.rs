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
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
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
    max_size: usize,
    read_size: usize,
}

use blake2::{Blake2b, Digest};

pub(crate) type Blake2b32 = Blake2b<blake2::digest::consts::U3>;

struct CachePath {
    base: PathBuf,
}

impl CachePath {
    fn new(dir: &Path, key: &CompactCacheKey) -> Result<Self> {
        let ser =
            rmp_serde::to_vec(&key).map_err(|err| perror("cannot serialize cachekey", err))?;
        let hex_ser = const_hex::encode(&ser);
        assert_eq!(
            hex_ser.len() % 2,
            0,
            "path encodes to odd length hex {hex_ser}"
        );

        let mut hasher = Blake2b32::new();
        hasher.update(ser);
        let hash = hasher.finalize();
        let prefix_dirs: Vec<String> = hash.iter().map(|b| const_hex::encode([*b])).collect();
        assert_eq!(
            prefix_dirs.len(),
            3,
            "path should have 3 prefix dirs, not {}",
            prefix_dirs.len()
        );

        let mut out = dir.to_owned();
        for prefix in prefix_dirs {
            out = out.join(prefix);
        }
        Ok(Self {
            base: out.join(hex_ser),
        })
    }

    fn meta(&self) -> PathBuf {
        self.base.with_extension("meta")
    }

    fn final_data(&self) -> PathBuf {
        self.base.with_extension("data")
    }

    fn partial_data(&self) -> PathBuf {
        self.base.with_extension("partial")
    }
}

async fn get_cachemeta(path: &Path) -> Result<Option<CacheMeta>> {
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

async fn put_cachemeta(meta: &CacheMeta, path: &Path) -> Result<()> {
    let data = serialize_cachemeta(meta)?;
    ensure_parent_dirs_exist(path).await?;
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
    pub fn new(path: PathBuf, max_size: usize, read_size: usize) -> io::Result<Self> {
        if !path.try_exists()? {
            std::fs::create_dir_all(&path)?;
        }
        Ok(Self {
            path,
            max_size,
            read_size,
        })
    }

    pub fn max_file_size_bytes(&self) -> usize {
        self.max_size
    }

    fn cache_path(&self, key: &CacheKey) -> Result<CachePath> {
        CachePath::new(&self.path, &key.to_compact())
    }

    fn cache_path_compact(&self, key: &CompactCacheKey) -> Result<CachePath> {
        CachePath::new(&self.path, key)
    }

    pub fn purge_sync(&'static self, key: &CompactCacheKey) -> Result<bool> {
        let cache_path = self.cache_path_compact(key)?;

        // error if partial data path exists
        let partial_data_path = cache_path.partial_data();
        if partial_data_path
            .try_exists()
            .map_err(|e| perror("cannot check if partial file exists", e))?
        {
            return Err(pingora::Error::explain(
                pingora::ErrorType::InternalError,
                format!(
                    "Cannot purge because partial data file exists {}",
                    partial_data_path.to_string_lossy()
                ),
            ));
        }

        let final_data_path = cache_path.final_data();
        let meta_path = cache_path.meta();
        log::info!("purge {}", final_data_path.to_string_lossy());
        let err_meta = std::fs::remove_file(meta_path);
        let err_data = std::fs::remove_file(&final_data_path);
        match (err_meta, err_data) {
            (Ok(()), Ok(())) => Ok(true),
            (Err(e1), Ok(())) => e_perror("Failed to remove cachemeta {}", e1),
            (Ok(()), Err(e2)) => e_perror(
                format!("Failed to remove data file {}", final_data_path.display()),
                e2,
            ),
            (Err(err_meta), Err(err_data)) => e_perror(
                format!(
                    "Failed to remove cachemeta {} and data: {}",
                    err_meta,
                    final_data_path.display(),
                ),
                err_data,
            ),
        }
    }
}

async fn ensure_parent_dirs_exist(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !exists(parent).await? {
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
        .get("Transfer-Encoding")
        .is_some_and(|h| b"chunked".eq_ignore_ascii_case(h.as_bytes()))
}

async fn has_correct_size(meta: &CacheMeta, path: &Path) -> Result<bool> {
    let Some(header_size) = content_length(meta) else {
        // TODO determine content-length for responses with chunked encoding
        if is_chunked_encoding(meta) {
            return Ok(true);
        }
        return Err(Error::explain(
            ErrorType::InternalError,
            "cannot determine content-length",
        ));
    };
    let file_size = fs::metadata(path)
        .await
        .map_err(|err| perror("cannot determine size of cached file", err))?
        .len();
    Ok(header_size == file_size)
}

async fn recently_updated(path: &Path) -> io::Result<bool> {
    let modified = fs::metadata(path).await?.modified()?;
    // TODO make recentness duration configurable
    Ok(SystemTime::now() < modified + Duration::from_secs(30))
}

async fn touch(path: &Path) -> Result<()> {
    match OpenOptions::new().create(true).truncate(true).write(true).open(path).await {
        Ok(_) => Ok(()),
        Err(e) => e_perror("cannot touch file", e),
    }
}

async fn exists(path: &Path) -> Result<bool> {
    fs::try_exists(path)
        .await
        .map_err(|e| perror("could not check if file exists", e))
}

#[async_trait]
impl Storage for FileStorage {
    async fn lookup(
        &'static self,
        key: &CacheKey,
        _trace: &SpanHandle,
    ) -> Result<Option<(CacheMeta, HitHandler)>> {
        let cp = self.cache_path(key)?;
        let meta_path = cp.meta();
        match get_cachemeta(&meta_path).await {
            Ok(Some(meta)) => {
                let final_path = cp.final_data();
                let partial_path = cp.partial_data();
                if exists(&final_path).await? && has_correct_size(&meta, &final_path).await? {
                    let h = Box::new(FileHitHandler::new(final_path, self.read_size).await?);
                    Ok(Some((meta, h)))
                } else if exists(&partial_path).await? {
                    // only return partial hit if partial file has been written to recently
                    let h = match recently_updated(&partial_path).await {
                        Ok(true) => Box::new(
                            PartialFileHitHandler::new(partial_path, final_path, self.read_size)
                                .await?,
                        ),
                        Ok(false) => {
                            log::warn!(
                                "partial data file {} exists, but has not been written to recently",
                                partial_path.to_string_lossy()
                            );
                            return Ok(None);
                        }
                        Err(err) => {
                            return e_perror("cannot determine modified time of partial file", err)
                        }
                    };
                    Ok(Some((meta, h)))
                } else {
                    // check again if final file now exists
                    if exists(&final_path).await? && has_correct_size(&meta, &final_path).await? {
                        let h = Box::new(FileHitHandler::new(final_path, self.read_size).await?);
                        Ok(Some((meta, h)))
                    } else {
                        log::warn!(
                            "cachemeta {} exists, but no corresponding data file found",
                            meta_path.to_string_lossy()
                        );
                        Ok(None)
                    }
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
        let cp = self.cache_path(key)?;
        let meta_path = cp.meta();
        let final_path = cp.final_data();
        let partial_path = cp.partial_data();
        ensure_parent_dirs_exist(&partial_path).await?;
        ensure_parent_dirs_exist(&final_path).await?;
        ensure_parent_dirs_exist(&meta_path).await?;
        // touch partial file before putting cachemeta to avoid missing partial file warning
        touch(&partial_path).await?;
        put_cachemeta(meta, &meta_path).await?;
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
        let cp = self.cache_path(key)?;
        put_cachemeta(meta, &cp.meta()).await?;
        Ok(true)
    }

    fn support_streaming_partial_write(&self) -> bool {
        true
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync + 'static) {
        self
    }
}
