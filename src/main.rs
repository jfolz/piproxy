use async_trait::async_trait;
use bstr::Finder;
use core::{any::Any, panic};
use log::{error, LevelFilter, Metadata, Record};
use once_cell::sync::OnceCell;
use pingora::{
    cache::{
        eviction::lru::Manager,
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
use std::{io, str};
use std::{str::FromStr, sync::atomic::Ordering};

struct SimpleLogger {}

const DEFAULT_LOG_LEVEL: LevelFilter = LevelFilter::Info;

impl SimpleLogger {
    fn install(&'static self) -> Result<(), log::SetLoggerError> {
        let result = log::set_logger(self);
        self.set_level(DEFAULT_LOG_LEVEL);
        result
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

const DEFAULT_CACHE_SIZE: usize = 1024 * 1024 * 1024;
const DEFAULT_CHUNK_SIZE: usize = 128 * 1024;
static READ_SIZE: OnceCell<usize> = OnceCell::new();

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
        self: Box<Self>, // because self is always used as a trait object
        _storage: &'static (dyn Storage + Sync),
        _key: &CacheKey,
        _trace: &SpanHandle,
    ) -> Result<()> {
        Ok(())
    }

    fn can_seek(&self) -> bool {
        true
    }

    /// Try to seek to a certain range of the body
    ///
    /// `end: None` means to read to the end of the body.
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

    async fn finish(
        self: Box<Self>, // because self is always used as a trait object
    ) -> Result<usize> {
        // remember that the write finished properly
        self.finished.store(true, Ordering::Relaxed);
        //debug!("HandleMiss finish called {}", self.path_str());
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
struct FileStorage {
    path: PathBuf,
}

impl FileStorage {
    fn new(path: PathBuf) -> io::Result<Self> {
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }
        Ok(Self { path: path })
    }

    fn data_path(&'static self, key: &CacheKey) -> PathBuf {
        self.path.join(&key.primary())
    }
    fn meta_path(&'static self, key: &CacheKey) -> PathBuf {
        self.path.join(key.primary() + ".meta")
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
impl Storage for FileStorage {
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
                return Ok(Some((meta, FileHitHandler::new_box(fp))));
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
        ensure_parent_dirs_exist(&data_path)?;

        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&data_path)
        {
            Ok(fp) => {
                //debug!("get_miss_handler file open {}", data_path.to_str().unwrap());
                Ok(FileMissHandler::new_box(data_path, fp))
            }
            Err(err) => {
                /*debug!(
                    "get_miss_handler error {} {}",
                    data_path.to_str().unwrap(),
                    err
                );*/
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
    async fn purge(&'static self, key: &CompactCacheKey, _trace: &SpanHandle) -> Result<bool> {
        todo!("purge {:?}", key)
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
static EVICTION: OnceCell<Manager<16>> = OnceCell::new();
const PYPI_ORG: &str = "pypi.org";
const FILES_PYTHONHOSTED_ORG: &str = "files.pythonhosted.org";
const HTTPS_FILES_PYTHONHOSTED_ORG: &str = "https://files.pythonhosted.org";
const CONTENT_TYPE_TEXT_HTML: &str = "text/html";

pub struct PyPI<'a> {
    finder_content_type: Finder<'a>,
    finder_url: Finder<'a>,
}

impl<'a> PyPI<'a> {
    fn new() -> PyPI<'a> {
        PyPI {
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

    fn upstream_response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) {
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
        // TODO add

        session.cache.enable(
            STORAGE.get().unwrap(),
            Some(EVICTION.get().unwrap()),
            // predictor: optionally a cache predictor. The cache predictor predicts whether something is likely to be cacheable or not.
            //            This is useful because the proxy can apply different types of optimization to cacheable and uncacheable requests.
            None,
            // cache_lock: optionally a cache lock which handles concurrent lookups to the same asset.
            //             Without it such lookups will all be allowed to fetch the asset independently.
            None,
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
}

#[derive(Debug)]
struct Unit(usize);

impl FromStr for Unit {
    type Err = parse_size::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        parse_size::Config::new()
            .with_binary()
            .parse_size(s)
            .map(|v| Self(v as usize))
    }
}

mod flags {
    use super::*;

    xflags::xflags! {
        cmd toplevel {
            /// Bind address
            optional -a,--address address: String
            /// Path where cached files are stored
            optional -p,--cache-path cache_path: PathBuf
            /// Path where cached files are stored
            optional -s,--cache-size cache_size: Unit
            /// Path where cached files are stored
            optional -c,--chunk-size chunk_size: Unit
            /// Set the log level
            optional -l,--log-level log_level: LevelFilter
        }
    }

    macro_rules! getter {
        ($field:ident, $default:expr) => {
            paste::paste! {
                pub fn [<get_ $field>](&self) -> usize {
                    self.$field.as_ref().map_or($default, |v| v.0)
                }
            }
        };
    }

    impl Toplevel {
        pub fn get_address(&self) -> &str {
            self.address.as_deref().unwrap_or("localhost:6188")
        }
        pub fn get_cache_path(&self) -> PathBuf {
            self.cache_path
                .clone()
                .unwrap_or_else(|| PathBuf::from("cache"))
        }
        getter!(chunk_size, DEFAULT_CHUNK_SIZE);
        getter!(cache_size, DEFAULT_CACHE_SIZE);
        pub fn get_log_level(&self) -> LevelFilter {
            self.log_level.unwrap_or(DEFAULT_LOG_LEVEL)
        }
    }
}

fn main() {
    LOGGER.install().unwrap();
    let flags = flags::Toplevel::from_env_or_exit();
    LOGGER.set_level(flags.get_log_level());
    let storage = FileStorage::new(flags.get_cache_path()).unwrap();
    STORAGE.set(storage).unwrap();
    READ_SIZE.set(flags.get_chunk_size()).unwrap();
    if let Err(_) = EVICTION.set(Manager::with_capacity(flags.get_cache_size(), 16)) {
        panic!("eviction manager already set");
    }

    let mut my_server = Server::new(None).unwrap();
    my_server.bootstrap();
    let inner = PyPI::new();
    let mut pypi = http_proxy_service(&my_server.configuration, inner);
    pypi.add_tcp(flags.get_address());
    my_server.add_service(pypi);
    my_server.run_forever();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_in_slice() {
        let src = b"foobarfoobarfoobarfoo";
        assert_eq!(remove_in_slice(src, &Finder::new(b"bar")), b"foofoofoofoo");
        assert_eq!(remove_in_slice(src, &Finder::new(b"foo")), b"barbarbar");
    }
}
