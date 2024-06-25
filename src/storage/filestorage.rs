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
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind, Read, Write},
    path::{Path, PathBuf},
};

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

fn path_from_key(dir: &PathBuf, key: &CompactCacheKey, suffix: &str) -> Result<PathBuf> {
    let ser = rmp_serde::to_vec(&key).map_err(|err| perror("cannot serialize cachekey", err))?;
    let ser = const_hex::encode(ser);
    assert_eq!(ser.len() % 2, 0, "path encodes to odd length hex {}", ser);
    Ok(dir.join(ser + suffix))
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

    fn final_data_path(&'static self, key: &CompactCacheKey) -> Result<PathBuf> {
        path_from_key(&self.path, key, "")
    }

    fn partial_data_path(&'static self, key: &CompactCacheKey) -> Result<PathBuf> {
        path_from_key(&self.path, key, ".partial")
    }

    fn meta_path(&'static self, key: &CompactCacheKey) -> Result<PathBuf> {
        path_from_key(&self.path, key, ".meta")
    }

    fn get_cachemeta(&'static self, key: &CompactCacheKey) -> Result<Option<CacheMeta>> {
        let path = self.meta_path(key)?;
        match File::open(&path) {
            Ok(mut fp) => {
                let mut data = Vec::new();
                match fp.read_to_end(&mut data) {
                    Ok(_) => Ok(Some(deserialize_cachemeta(&data)?)),
                    Err(err) => e_perror("error reading cachemeta", err),
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => e_perror("error opening cachemeta", err),
        }
    }

    fn put_cachemeta(&'static self, key: &CompactCacheKey, meta: &CacheMeta) -> Result<()> {
        let data = serialize_cachemeta(meta)?;
        let path = self.meta_path(key)?;
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
        let path = self.meta_path(key)?;
        fs::remove_file(&path).map_err(|err| perror("pop cachemeta", err))?;
        Ok(())
    }

    pub fn purge_sync(&'static self, key: &CompactCacheKey) -> Result<bool> {
        let data_path = self.final_data_path(key)?;
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
        match self.get_cachemeta(&key) {
            Ok(Some(meta)) => {
                let final_path = self.final_data_path(&key)?;
                let partial_path = self.partial_data_path(&key)?;
                let h: HitHandler;
                if partial_path.exists() {
                    h = Box::new(
                        PartialFileHitHandler::new(partial_path, final_path, self.read_size)
                            .await?,
                    );
                } else {
                    h = Box::new(FileHitHandler::new(final_path, self.read_size).await?);
                }
                Ok(Some((meta, h)))
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
        self.put_cachemeta(&key, meta)?;
        let partial_path = self.partial_data_path(&key)?;
        let final_path = self.final_data_path(&key)?;
        let meta_path = self.meta_path(&key)?;
        ensure_parent_dirs_exist(&partial_path)?;
        ensure_parent_dirs_exist(&final_path)?;
        ensure_parent_dirs_exist(&meta_path)?;
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
        self.put_cachemeta(&key.to_compact(), meta)?;
        Ok(true)
    }

    fn support_streaming_partial_write(&self) -> bool {
        true
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync + 'static) {
        self
    }
}
