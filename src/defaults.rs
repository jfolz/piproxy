use log::LevelFilter;

pub const DEFAULT_CACHE_SIZE: usize = 1024 * 1024 * 1024;
pub const DEFAULT_CHUNK_SIZE: usize = 128 * 1024;
pub const DEFAULT_CACHE_TIMEOUT: u64 = 60;
pub const DEFAULT_LOG_LEVEL: LevelFilter = LevelFilter::Info;
