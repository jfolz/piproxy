use async_trait::async_trait;
use core::any::Any;
use once_cell::sync::OnceCell;
use pingora::{
    cache::{
        key::{CacheHashKey, CompactCacheKey},
        storage::{HandleHit, HandleMiss},
        trace::SpanHandle,
        CacheKey, CacheMeta, HitHandler, MissHandler,
        RespCacheable::{self, Cacheable},
        Storage,
    },
    http::ResponseHeader,
    prelude::*,
};
use std::str;
use std::time::{Duration, SystemTime};
use std::{
    ffi::{OsStr, OsString},
    path::PathBuf,
    sync::atomic::AtomicBool,
};
use std::{fs, io::Read};
use std::{fs::create_dir_all, io::Seek, path::Path};
use std::{
    fs::{File, OpenOptions},
    io::Write,
};

use log::{debug, error, LevelFilter, Metadata, Record};
use std::sync::atomic::Ordering;

struct SimpleLogger {}

impl SimpleLogger {
    fn install(&'static self) -> Result<(), log::SetLoggerError> {
        log::set_logger(self)
    }

    fn set_level(&self, level: LevelFilter) {
        log::set_max_level(level);
    }
}

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

static LOGGER: SimpleLogger = SimpleLogger {};

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

struct FileHandler {
    path: PathBuf,
    fp: File,
    written: usize,
    finished: AtomicBool,
}

impl FileHandler {
    fn new(path: PathBuf, fp: File) -> FileHandler {
        Self {
            path: path,
            fp: fp,
            written: 0,
            finished: false.into(),
        }
    }

    fn new_box(path: PathBuf, fp: File) -> Box<FileHandler> {
        Box::new(Self::new(path, fp))
    }

    fn path_str<'a>(&self) -> String {
        String::from(self.path.to_str().unwrap())
    }
}

fn append_to_path(p: impl Into<OsString>, s: impl AsRef<OsStr>) -> PathBuf {
    let mut p = p.into();
    p.push(s);
    p.into()
}

fn path_to_str(path: impl Into<OsString>) -> String {
    path.into().into_string().unwrap()
}

impl Drop for FileHandler {
    fn drop(&mut self) {
        let finished: bool = self.finished.load(Ordering::Relaxed);
        if !finished {
            if let Err(err) = fs::remove_file(&self.path) {
                error!("cannot remove unfinished file {}: {}", self.path_str(), err);
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

#[async_trait]
impl HandleHit for FileHandler {
    /// Read cached body
    ///
    /// Return `None` when no more body to read.
    async fn read_body(&mut self) -> Result<Option<bytes::Bytes>> {
        let mut buf = vec![0; 4096];
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

    /// Finish the current cache hit
    async fn finish(
        self: Box<Self>, // because self is always used as a trait object
        _storage: &'static (dyn Storage + Sync),
        _key: &CacheKey,
        trace: &SpanHandle,
    ) -> Result<()> {
        self.finished.store(true, Ordering::Relaxed);
        debug!(
            "{:p} HandleHit finish called {}, trace {:?}",
            self,
            self.path_str(),
            trace
        );
        Ok(())
    }

    /// Whether this storage allow seeking to a certain range of body
    fn can_seek(&self) -> bool {
        true
    }

    /// Try to seek to a certain range of the body
    ///
    /// `end: None` means to read to the end of the body.
    fn seek(&mut self, start: usize, _end: Option<usize>) -> Result<()> {
        // to prevent impl can_seek() without impl seek
        if let Err(err) = self.fp.seek(std::io::SeekFrom::Start(start as u64)) {
            return e_perror("error seeking in cache", err);
        }
        Ok(())
    }
    // TODO: fn is_stream_hit()

    /// Helper function to cast the trait object to concrete types
    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        todo!("as_any")
    }
}

#[async_trait]
impl HandleMiss for FileHandler {
    /// Write the given body to the storage
    async fn write_body(&mut self, data: bytes::Bytes, _eof: bool) -> Result<()> {
        match self.fp.write_all(&data) {
            Ok(()) => {
                self.written += data.len();
                Ok(())
            }
            Err(err) => e_perror("error writing to cache", err),
        }
    }

    /// Finish the cache admission
    ///
    /// When `self` is dropped without calling this function, the storage should consider this write
    /// failed.
    async fn finish(
        self: Box<Self>, // because self is always used as a trait object
    ) -> Result<usize> {
        self.finished.store(true, Ordering::Relaxed);
        //debug!("HandleMiss finish called {}", self.path_str());
        Ok(self.written)
    }
}

#[derive(Debug)]
struct FileStorage<'a> {
    path: &'a str,
}

impl FileStorage<'_> {
    fn data_path(&'static self, key: &CacheKey) -> PathBuf {
        [self.path, &key.primary()].iter().collect()
    }
    fn meta_path(&'static self, key: &CacheKey) -> PathBuf {
        [self.path, &(key.primary() + ".meta")].iter().collect()
    }
}

const HOUR: Duration = Duration::from_secs(3600);

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

fn load_cachemeta(path: &Path) -> Result<CacheMeta> {
    match File::open(&path) {
        Ok(mut fp) => {
            let mut data = Vec::new();
            match fp.read_to_end(&mut data) {
                Ok(_) => deserialize_cachemeta(&data),
                Err(err) => return e_perror("error reading cachemeta", err),
            }
        }
        Err(err) => return e_perror("error opening cachemeta", err),
    }
}

#[async_trait]
impl Storage for FileStorage<'_> {
    /// Lookup the storage for the given [CacheKey]
    async fn lookup(
        &'static self,
        key: &CacheKey,
        _trace: &SpanHandle,
    ) -> Result<Option<(CacheMeta, HitHandler)>> {
        let data_path = self.data_path(key);

        let meta = load_cachemeta(&self.meta_path(key))?;

        //debug!("lookup {}", data_path.to_str().unwrap_or(""));

        match File::open(&data_path) {
            Ok(fp) => {
                return Ok(Some((meta, FileHandler::new_box(data_path, fp))));
                /*match fp.metadata() {
                    Ok(attr) => {
                        let mtime = attr.modified().unwrap();
                        let fresh_until: SystemTime;
                        if data_path.extension().map_or(false, |ext| ext == "whl") {
                            fresh_until = mtime + 9999 * HOUR;
                        } else {
                            fresh_until = mtime + HOUR;
                        }

                        debug!("lookup OK {}", data_path.to_str().unwrap());

                        return Ok(Some((meta, FileHandler::new_box(data_path, fp))));
                    },
                    Err(err) => {
                        debug!("lookup error {} {}", data_path.to_str().unwrap(), err);
                        return Err(pingora::Error::because(
                            pingora::ErrorType::InternalError,
                            "error opening cache",
                            err,
                        ))
                    }
                }*/
            }
            Err(err) => {
                //debug!("lookup error {} {}", data_path.to_str().unwrap(), err);
                return e_perror("error opening cache", err);
            }
        }
    }

    /// Write the given [CacheMeta] to the storage. Return [MissHandler] to write the body later.
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
        ensure_parent_dirs_exist(&meta_path)?;

        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&data_path)
        {
            Ok(fp) => {
                //debug!("get_miss_handler file open {}", data_path.to_str().unwrap());
                Ok(FileHandler::new_box(data_path, fp))
            }
            Err(err) => {
                debug!(
                    "get_miss_handler error {} {}",
                    data_path.to_str().unwrap(),
                    err
                );
                Err(pingora::Error::because(
                    pingora::ErrorType::InternalError,
                    "error opening cache",
                    err,
                ))
            }
        }
    }

    /// Delete the cached asset for the given key
    ///
    /// [CompactCacheKey] is used here because it is how eviction managers store the keys
    async fn purge(&'static self, _key: &CompactCacheKey, _trace: &SpanHandle) -> Result<bool> {
        todo!("purge")
    }

    /// Update cache header and metadata for the already stored asset.
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

    /// Whether this storage backend supports reading partially written data
    ///
    /// This is to indicate when cache should unlock readers
    fn support_streaming_partial_write(&self) -> bool {
        false
    }

    /// Helper function to cast the trait object to concrete types
    fn as_any(&self) -> &(dyn Any + Send + Sync + 'static) {
        todo!("as_any")
    }
}

static STORAGE: OnceCell<FileStorage> = OnceCell::new();
const PYPI_ORG: &str = "pypi.org";
const FILES_PYTHONHOSTED_ORG: &str = "files.pythonhosted.org";
const PYTHONHOSTED: &str = "https://files.pythonhosted.org";

pub struct PyPI {}

impl PyPI {
    fn new() -> PyPI {
        PyPI {  }
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
    fn new() -> CacheCTX {
        CacheCTX { modify: false, buffer: Vec::new() }
    }
}

fn find_in_slice(data: &[u8], target: &[u8]) -> Option<usize> {
    data.windows(target.len())
        .position(|window| window == target)
}

#[async_trait]
impl ProxyHttp for PyPI {
    /// For this small example, we don't need context storage
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
        // for pacakges use files.pythonhosted.org
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

    /// Modify the response header from the upstream
    ///
    /// The modification is before caching, so any change here will be stored in the cache if enabled.
    ///
    /// Responses served from cache won't trigger this filter. If the cache needed revalidation,
    /// only the 304 from upstream will trigger the filter (though it will be merged into the
    /// cached header, not served directly to downstream).
    fn upstream_response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) {
        // only modify html pages
        if let Some(ct) = upstream_response.headers.get("Content-Type") {
            if let Some(_) = find_in_slice(ct.as_bytes(), b"text/html") {
                ctx.modify = true;
                // Remove content-length because the size of the new body is unknown
                upstream_response.remove_header("Content-Length");
                upstream_response
                    .insert_header("Transfer-Encoding", "Chunked")
                    .unwrap();
            }
        }
    }

    /// Similar to [Self::upstream_response_filter()] but for response body
    ///
    /// This function will be called every time a piece of response body is received. The `body` is
    /// **not the entire response body**.
    fn upstream_response_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<bytes::Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) {
        // TODO if path starts with `/simple/` replace links to pythonhosted.org with self
        // i.e. replace
        //     `href="https://files.pythonhosted.org` with
        //     `             href="http://pypi-cache` to preserve length of body
        // problem: fails on chunk boundaries
        // solution: two steps
        // 1) replace strings as above
        // 2) if end of chunk matches `<a...`
        //     replace with as much with `<a              href="http://pypi-cache` as possible,
        //     store remainder in ctx and apply to next chunk
        if ctx.modify {
            if let Some(b) = body {
                ctx.buffer.extend(&b[..]);
                // drop the body
                b.clear();
            }

            if end_of_stream {
                let mut src = ctx.buffer.as_slice();
                let mut dst: Vec<u8> = Vec::with_capacity(src.len());
                let todel = PYTHONHOSTED.as_bytes();
                while let Some(pos) = find_in_slice(src, todel) {
                    dst.extend_from_slice(&src[..pos]);
                    src = &src[pos + todel.len()..];
                }
                dst.extend(src);
                *body = Some(bytes::Bytes::from(dst));
            }
        }
    }

    fn request_cache_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<()> {
        let storage = STORAGE.get().unwrap();
        session.cache.enable(storage, None, None, None);
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
}

fn main() {
    LOGGER.install().unwrap();
    LOGGER.set_level(LevelFilter::Debug);

    let mut my_server = Server::new(None).unwrap();
    my_server.bootstrap();

    let path = Path::new("./cache");
    if !path.exists() {
        fs::create_dir_all(path).unwrap();
    }

    STORAGE
        .set(FileStorage {
            path: path.to_str().unwrap(),
        })
        .unwrap();

    let inner = PyPI::new();
    let mut pypi = http_proxy_service(&my_server.configuration, inner);
    pypi.add_tcp("0.0.0.0:6188");
    my_server.add_service(pypi);
    my_server.run_forever();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exploration() {
        assert_eq!(find_in_slice(b"abcdefg", b"hjk"), None);
        assert_eq!(find_in_slice(b"abcdefg", b"a"), Some(0));
        assert_eq!(find_in_slice(b"abcdefg", b"ab"), Some(0));
        assert_eq!(find_in_slice(b"abcdefg", b"abc"), Some(0));
        assert_eq!(find_in_slice(b"abcdefg", b"abcd"), Some(0));
        assert_eq!(find_in_slice(b"abcdefg", b"abcde"), Some(0));
        assert_eq!(find_in_slice(b"abcdefg", b"abcdef"), Some(0));
        assert_eq!(find_in_slice(b"abcdefg", b"abcdefg"), Some(0));
        assert_eq!(find_in_slice(b"abcdefg", b"bcdefg"), Some(1));
        assert_eq!(find_in_slice(b"abcdefg", b"cdefg"), Some(2));
        assert_eq!(find_in_slice(b"abcdefg", b"defg"), Some(3));
        assert_eq!(find_in_slice(b"abcdefg", b"efg"), Some(4));
        assert_eq!(find_in_slice(b"abcdefg", b"fg"), Some(5));
        assert_eq!(find_in_slice(b"abcdefg", b"g"), Some(6));
    }
}
