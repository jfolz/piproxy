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
    path::Path,
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
    let storage = match storage::FileStorage::new(cache_path, chunk_size) {
        Ok(storage) => storage,
        Err(err) => {
            panic!("cannot create cache storage: {err}");
        }
    };
    assert!(STORAGE.set(storage).is_ok(), "storage already set");

    let manager = Manager::new(cache_size);
    assert!(
        EVICTION.set(manager).is_ok(),
        "eviction manager already set"
    );

    let timeout = Duration::from_secs(cache_lock_timeout);
    let cache_lock = CacheLock::new(timeout);
    assert!(CACHE_LOCK.set(cache_lock).is_ok(), "cache lock already set");
}

fn has_extension<'a, I>(entry: &DirEntry, exts: I) -> bool
where
    I: IntoIterator<Item = &'a str>,
{
    entry
        .path()
        .extension()
        .is_some_and(|found| exts.into_iter().any(|ext| found == ext))
}

fn is_entry(entry: &DirEntry) -> bool {
    has_extension(entry, [".data"])
}

fn key_from_entry(entry: &DirEntry) -> io::Result<CompactCacheKey> {
    let path = entry.path();
    let filename = path.file_stem().ok_or(io::Error::new(
        ErrorKind::InvalidData,
        "given entry is not a data file",
    ))?;
    let data = filename.as_encoded_bytes();
    assert_eq!(data.len() % 2, 0, "path has odd length {filename:?}");
    let ser: Vec<u8> =
        const_hex::decode(data).map_err(|err| io::Error::new(ErrorKind::UnexpectedEof, err))?;
    rmp_serde::from_slice(&ser).map_err(|err| io::Error::new(ErrorKind::InvalidData, err))
}

type Admission = (CompactCacheKey, usize);

enum ParseResult {
    Dir(PathBuf),
    Entry(Admission),
    None,
    Warning(io::Error),
}

fn parse_entry_inner(entry: &DirEntry) -> io::Result<Admission> {
    let metadata = entry.metadata()?;
    let key = key_from_entry(entry)?;
    let size = usize::try_from(metadata.size())
        .map_err(|err| io::Error::new(ErrorKind::InvalidInput, err))?;
    Ok((key, size))
}

fn parse_entry(entry: io::Result<DirEntry>) -> ParseResult {
    let entry = match entry {
        Ok(entry) => entry,
        Err(err) => return ParseResult::Warning(err),
    };
    let ftype = match entry.file_type() {
        Ok(ftype) => ftype,
        Err(err) => return ParseResult::Warning(err),
    };
    if ftype.is_dir() {
        ParseResult::Dir(entry.path())
    } else if is_entry(&entry) {
        match parse_entry_inner(&entry) {
            Ok(entry) => ParseResult::Entry(entry),
            Err(err) => ParseResult::Warning(err),
        }
    } else {
        ParseResult::None
    }
}

pub fn populate_lru(cache_dir: &Path) -> io::Result<()> {
    let manager = EVICTION
        .get()
        .ok_or(io::Error::new(ErrorKind::Other, "eviction manager not set"))?;
    let storage = STORAGE
        .get()
        .ok_or(io::Error::new(ErrorKind::Other, "cache storage not set"))?;
    let mut todo = vec![cache_dir.to_owned()];
    // simple_lru manager does not use fresh_until, make sure this is actually simple_lru
    assert_eq!(
        manager.type_id(),
        TypeId::of::<pingora::cache::eviction::simple_lru::Manager>()
    );
    let fresh_until = SystemTime::now() + Duration::from_secs(356_000_000);
    while let Some(next) = todo.pop() {
        let entries = match fs::read_dir(&next) {
            Ok(entries) => entries,
            Err(err) => {
                log::error!(
                    "could not list directory {}: {}",
                    next.to_string_lossy(),
                    err
                );
                continue;
            }
        };
        for entry in entries {
            match parse_entry(entry) {
                ParseResult::Dir(path) => todo.push(path),
                ParseResult::Entry((key, size)) => {
                    for key in manager.admit(key, size, fresh_until) {
                        storage
                            .purge_sync(&key)
                            .map_err(|err| io::Error::new(ErrorKind::Other, err))?;
                    }
                }
                ParseResult::None => continue,
                ParseResult::Warning(err) => log::warn!("could not parse cache entry: {}", err),
            }
        }
    }
    Ok(())
}

pub struct PyPI<'a> {
    content_type_text_html: Finder<'a>,
    https_files_pythonhosted_org: Finder<'a>,
    https_pypi_org: Finder<'a>,
}

impl<'a> PyPI<'a> {
    pub fn new() -> PyPI<'a> {
        PyPI {
            content_type_text_html: Finder::new(CONTENT_TYPE_TEXT_HTML),
            https_files_pythonhosted_org: Finder::new(HTTPS_FILES_PYTHONHOSTED_ORG),
            https_pypi_org: Finder::new(HTTPS_PYPI_ORG),
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
impl ProxyHttp for PyPI<'_> {
    type CTX = CacheCTX;
    fn new_ctx(&self) -> Self::CTX {
        Self::CTX::new()
    }

    async fn upstream_peer(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let addr = if request_path(session).starts_with(b"/packages/") {
            // for pacakges use files.pythonhosted.org
            (FILES_PYTHONHOSTED_ORG, 443)
        } else {
            // otherwise pypi.org
            (PYPI_ORG, 443)
        };
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
            upstream_request.insert_header("Host", FILES_PYTHONHOSTED_ORG)?;
        }
        // otherwise pypi.org
        else {
            upstream_request.insert_header("Host", PYPI_ORG)?;
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
                let loc = remove_in_slice(loc.as_bytes(), &self.https_pypi_org);
                upstream_response.insert_header("Location", loc).unwrap();
            }
        }
        // only modify html pages
        if let Some(ct) = upstream_response.headers.get("Content-Type") {
            if self.content_type_text_html.find(ct.as_bytes()).is_some() {
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
                let out =
                    remove_in_slice(ctx.buffer.as_slice(), &self.https_files_pythonhosted_org);
                *body = Some(bytes::Bytes::from(out));
            }
        }
    }

    fn request_cache_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<()> {
        let Some(storage) = STORAGE.get() else {
            return Ok(());
        };
        session.cache.enable(
            // storage: the cache storage backend that implements storage::Storage
            storage,
            // eviction: optionally the eviction manager, without it, nothing will be evicted from the storage
            EVICTION
                .get()
                .map(|v| v as &'static (dyn pingora::cache::eviction::EvictionManager + Sync)),
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
                    let age = resp
                        .headers
                        .get("Age")
                        .map_or("0", |age| age.to_str().unwrap_or("0"));
                    let age: u64 = age.parse().unwrap_or(0);
                    let age = Duration::from_secs(age);
                    let max_age =
                        Duration::from_secs(u64::from(control.fresh_sec().unwrap_or(600)));
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
