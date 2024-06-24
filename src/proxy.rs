use async_trait::async_trait;
use bstr::Finder;
use once_cell::sync::OnceCell;
use pingora::{
    cache::{
        cache_control::{CacheControl, InterpretCacheControl},
        eviction::{simple_lru::Manager, EvictionManager},
        key::CompactCacheKey,
        lock::CacheLock,
        CacheMeta, NoCacheReason, RespCacheable,
    },
    http::{ResponseHeader, StatusCode},
    prelude::*,
};
use std::{
    any::{Any, TypeId},
    fs::{self, DirEntry},
    io::{self, ErrorKind},
    os::unix::fs::MetadataExt,
    str,
};
use std::{
    path::PathBuf,
    time::{Duration, SystemTime},
};

use crate::storage;

static STORAGE: OnceCell<storage::FileStorage> = OnceCell::new();
static EVICTION: OnceCell<Manager> = OnceCell::new();
static CACHE_LOCK: OnceCell<CacheLock> = OnceCell::new();
const PYPI_ORG: &str = "pypi.org";
const HTTPS_PYPI_ORG: &str = "https://pypi.org";
const FILES_PYTHONHOSTED_ORG: &str = "files.pythonhosted.org";
const HTTPS_FILES_PYTHONHOSTED_ORG: &str = "https://files.pythonhosted.org";
const CONTENT_TYPE_TEXT_HTML: &str = "text/html";

pub fn setup(cache_path: PathBuf, cache_size: usize, cache_lock_timeout: u64, chunk_size: usize) {
    let storage = storage::FileStorage::new(cache_path, chunk_size).unwrap();
    STORAGE.set(storage).unwrap();

    let manager = Manager::new(cache_size);
    // TODO save and load manager state
    //manager.load(flags.get_cache_path().to_str().unwrap());
    if let Err(_) = EVICTION.set(manager) {
        panic!("eviction manager already set");
    }

    let timeout = Duration::from_secs(cache_lock_timeout);
    if let Err(_) = CACHE_LOCK.set(CacheLock::new(timeout)) {
        panic!("cache lock already set");
    }
}

fn has_extension(entry: &DirEntry, ext: &str) -> bool {
    entry.path().extension().is_some_and(|found| found == ext)
}

fn key_from_entry(entry: &DirEntry) -> io::Result<CompactCacheKey> {
    let filename = entry.file_name();
    let data = filename.as_encoded_bytes();
    assert_eq!(data.len() % 2, 0, "path has odd length {:?}", filename);
    let ser: Vec<u8> =
        const_hex::decode(data).map_err(|err| io::Error::new(ErrorKind::UnexpectedEof, err))?;
    rmp_serde::from_slice(&ser).map_err(|err| io::Error::new(ErrorKind::InvalidData, err))
}

pub fn populate_lru(cache_dir: &PathBuf) -> io::Result<()> {
    let manager = EVICTION.get().unwrap();
    let storage = STORAGE.get().unwrap();
    let mut todo = vec![cache_dir.clone()];
    // simple_lru manager does not use fresh_until, make sure this is actually simple_lru
    assert_eq!(
        manager.type_id(),
        TypeId::of::<pingora::cache::eviction::simple_lru::Manager>()
    );
    let fresh_until = SystemTime::now() + Duration::from_secs(356_000_000);
    loop {
        if let Some(next) = todo.pop() {
            for entry in fs::read_dir(next)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    todo.push(entry.path());
                } else if !has_extension(&entry, "meta") {
                    let metadata = entry.metadata()?;
                    let key = key_from_entry(&entry)?;
                    let size = metadata.size() as usize;
                    for key in manager.admit(key, size, fresh_until) {
                        storage
                            .purge_sync(&key)
                            .map_err(|err| io::Error::new(ErrorKind::Other, err))?;
                    }
                }
            }
        } else {
            break;
        }
    }
    Ok(())
}

pub struct PyPIProxy<'a> {
    finder_content_type: Finder<'a>,
    finder_pythonhosted: Finder<'a>,
    finder_pypi: Finder<'a>,
}

impl<'a> PyPIProxy<'a> {
    pub fn new() -> PyPIProxy<'a> {
        PyPIProxy {
            finder_content_type: Finder::new(CONTENT_TYPE_TEXT_HTML),
            finder_pythonhosted: Finder::new(HTTPS_FILES_PYTHONHOSTED_ORG),
            finder_pypi: Finder::new(HTTPS_PYPI_ORG),
        }
    }
}

fn request_path(session: &Session) -> &[u8] {
    session.req_header().raw_path()
}

pub struct CacheCTX {
    modify: bool,
    buffer: Vec<u8>,
}

impl CacheCTX {
    fn new() -> Self {
        Self {
            modify: false,
            buffer: Vec::new(),
        }
    }
}

fn remove_in_slice<B: Clone>(data: &[B], finder: &Finder) -> Vec<B>
where
    [B]: AsRef<[u8]>,
{
    let n = finder.needle().len();
    let mut src = data;
    let mut dst: Vec<B> = Vec::with_capacity(src.len());
    while let Some(pos) = finder.find(src) {
        dst.extend_from_slice(&src[..pos]);
        src = &src[pos + n..];
    }
    dst.extend_from_slice(src);
    dst
}

#[async_trait]
impl ProxyHttp for PyPIProxy<'_> {
    type CTX = CacheCTX;
    fn new_ctx(&self) -> Self::CTX {
        Self::CTX::new()
    }

    async fn upstream_peer(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let addr;
        // for pacakges use files.pythonhosted.org
        if request_path(session).starts_with(b"/packages/") {
            addr = (FILES_PYTHONHOSTED_ORG, 443);
        }
        // otherwise pypi.org
        else {
            addr = (PYPI_ORG, 443);
        }
        let peer = Box::new(HttpPeer::new(addr, true, addr.0.to_string()));
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        // for packages use files.pythonhosted.org
        if request_path(session).starts_with(b"/packages/") {
            upstream_request
                .insert_header("Host", FILES_PYTHONHOSTED_ORG)
                .unwrap();
        }
        // otherwise pypi.org
        else {
            upstream_request.insert_header("Host", PYPI_ORG).unwrap();
        }
        // server should not compress response
        upstream_request.remove_header("Accept-Encoding");
        // server should respond with default type
        upstream_request.remove_header("Accept");
        Ok(())
    }

    fn upstream_response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) {
        // rewrite header `Location` to point here
        if upstream_response.status.is_redirection()
            || upstream_response.status == StatusCode::CREATED
        {
            if let Some(loc) = upstream_response.headers.get("Location") {
                let loc = remove_in_slice(loc.as_bytes(), &self.finder_pypi);
                upstream_response.insert_header("Location", loc).unwrap();
            }
        }
        // only modify html pages
        if let Some(ct) = upstream_response.headers.get("Content-Type") {
            if let Some(_) = self.finder_content_type.find(ct.as_bytes()) {
                ctx.modify = true;
                // Remove content-length because the size of the new body is unknown
                upstream_response.remove_header("Content-Length");
                upstream_response
                    .insert_header("Transfer-Encoding", "Chunked")
                    .unwrap();
            }
        }
    }

    fn upstream_response_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<bytes::Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) {
        // store body in ctx and remove files.pythonhosted.org once all has been received
        if ctx.modify {
            if let Some(b) = body {
                ctx.buffer.extend(&b[..]);
                b.clear();
            }
            if end_of_stream {
                let out = remove_in_slice(ctx.buffer.as_slice(), &self.finder_pythonhosted);
                *body = Some(bytes::Bytes::from(out));
            }
        }
    }

    fn request_cache_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<()> {
        session.cache.enable(
            // storage: the cache storage backend that implements storage::Storage
            STORAGE.get().unwrap(),
            // eviction: optionally the eviction manager, without it, nothing will be evicted from the storage
            Some(EVICTION.get().unwrap()),
            // predictor: optionally a cache predictor. The cache predictor predicts whether something is likely to be cacheable or not.
            //            This is useful because the proxy can apply different types of optimization to cacheable and uncacheable requests.
            None,
            // cache_lock: optionally a cache lock which handles concurrent lookups to the same asset.
            //             Without it such lookups will all be allowed to fetch the asset independently.
            CACHE_LOCK.get(),
        );
        Ok(())
    }

    fn response_cache_filter(
        &self,
        _session: &Session,
        resp: &ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<RespCacheable> {
        if let Some(control) = CacheControl::from_resp_headers(resp) {
            use pingora::cache::cache_control::Cacheable;
            match control.is_cacheable() {
                Cacheable::Yes | Cacheable::Default => {
                    let now = SystemTime::now();
                    let age = match resp.headers.get("Age") {
                        Some(age) => age.to_str().unwrap_or("0"),
                        None => "0",
                    };
                    let age = u64::from_str_radix(age, 10).unwrap_or(0);
                    let age = Duration::from_secs(age);
                    let max_age = Duration::from_secs(control.fresh_sec().unwrap_or(600) as u64);
                    let fresh_until: SystemTime = now + max_age;
                    let created = now - age;
                    let revalidate_sec = control.serve_stale_while_revalidate_sec().unwrap_or(60);
                    let error_sec = control.serve_stale_if_error_sec().unwrap_or(60);
                    let meta = CacheMeta::new(
                        fresh_until,
                        created,
                        revalidate_sec,
                        error_sec,
                        resp.clone(),
                    );
                    Ok(RespCacheable::Cacheable(meta))
                }
                Cacheable::No => Ok(RespCacheable::Uncacheable(NoCacheReason::OriginNotCache)),
            }
        } else {
            Ok(RespCacheable::Uncacheable(NoCacheReason::OriginNotCache))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::remove_in_slice;
    use bstr::Finder;
    use pingora::cache::{key::CompactCacheKey, CacheKey};

    #[test]
    fn test_remove_in_slice() {
        let src = b"foobarfoobarfoobarfoo";
        assert_eq!(remove_in_slice(src, &Finder::new(b"bar")), b"foofoofoofoo");
        assert_eq!(remove_in_slice(src, &Finder::new(b"foo")), b"barbarbar");
    }

    #[test]
    fn test_compactcachekey_serde() {
        let key = CacheKey::new("", "testrestmest", "");
        let key = key.to_compact();
        let out = rmp_serde::to_vec(&key).unwrap();
        let out = const_hex::encode(out);
        println!("{}", out);
        let in_ = const_hex::decode(out).unwrap();
        let dekey: CompactCacheKey = rmp_serde::from_slice(&in_).unwrap();
        assert_eq!(key, dekey)
    }
}
