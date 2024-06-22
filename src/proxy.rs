use async_trait::async_trait;
use bstr::{ByteSlice, Finder};
use once_cell::sync::OnceCell;
use pingora::{
    cache::{
        eviction::simple_lru::Manager, key::HashBinary, lock::CacheLock, CacheMeta, RespCacheable::{self, Cacheable}
    },
    http::{ResponseHeader, StatusCode},
    prelude::*,
};
use std::str;
use std::{
    path::PathBuf,
    time::{Duration, SystemTime},
};

use crate::storage;

const HOUR: Duration = Duration::from_secs(3600);

static STORAGE: OnceCell<storage::FileStorage> = OnceCell::new();
static EVICTION: OnceCell<Manager> = OnceCell::new();
static CACHE_LOCK: OnceCell<CacheLock> = OnceCell::new();
const PYPI_ORG: &str = "pypi.org";
const HTTPS_PYPI_ORG: &str = "https://pypi.org";
const FILES_PYTHONHOSTED_ORG: &str = "files.pythonhosted.org";
const HTTPS_FILES_PYTHONHOSTED_ORG: &str = "https://files.pythonhosted.org";
const CONTENT_TYPE_TEXT_HTML: &str = "text/html";

pub fn setup(cache_path: PathBuf, cache_size: usize, cache_lock_timeout: u64, chunk_size: usize) {
    let storage = storage::FileStorage::new(cache_path).unwrap();
    STORAGE.set(storage).unwrap();
    storage::READ_SIZE.set(chunk_size).unwrap();

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

pub struct PyPIProxy<'a> {
    finder_content_type: Finder<'a>,
    finder_url: Finder<'a>,
}

impl<'a> PyPIProxy<'a> {
    pub fn new() -> PyPIProxy<'a> {
        PyPIProxy {
            finder_content_type: Finder::new(CONTENT_TYPE_TEXT_HTML),
            finder_url: Finder::new(HTTPS_FILES_PYTHONHOSTED_ORG),
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
                let loc = loc.as_bytes();
                let loc = loc.replace(HTTPS_PYPI_ORG, b"");
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
                let out = remove_in_slice(ctx.buffer.as_slice(), &self.finder_url);
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
        session: &Session,
        resp: &ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<RespCacheable> {
        let now = SystemTime::now();

        let mut fresh_until: SystemTime = now + HOUR;

        let path = request_path(session);
        if path.ends_with(b".whl") || path.ends_with(b".tar.gz") {
            fresh_until = now + 9999 * HOUR;
        }

        Ok(Cacheable(CacheMeta::new(
            fresh_until,
            now,
            3600,
            3600,
            resp.clone(),
        )))
    }

    fn cache_vary_filter(
        &self,
        meta: &CacheMeta,
        _ctx: &mut Self::CTX,
        _req: &RequestHeader,
    ) -> Option<HashBinary> {
        if let Some(ct) = meta.response_header().headers.get("Content-Type") {
            log::info!("cache_vary_filter {:?}", ct);
            Some(pingora::cache::key::hash_key(ct.to_str().unwrap()))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::remove_in_slice;
    use bstr::Finder;

    #[test]
    fn test_remove_in_slice() {
        let src = b"foobarfoobarfoobarfoo";
        assert_eq!(remove_in_slice(src, &Finder::new(b"bar")), b"foofoofoofoo");
        assert_eq!(remove_in_slice(src, &Finder::new(b"foo")), b"barbarbar");
    }
}
