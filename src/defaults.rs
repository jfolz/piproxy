use log::LevelFilter;

pub const DEFAULT_LOG_LEVEL: LevelFilter = LevelFilter::Info;
pub const DEFAULT_ADDRESS: &str = "localhost:8080";
pub const DEFAULT_PROMETHEUS_ADDRESS: &str = "localhost:9898";
pub const DEFAULT_CACHE_PATH: &str = "cache";
pub const DEFAULT_CACHE_SIZE: usize = 1024 * 1024 * 1024;
pub const DEFAULT_CACHE_RATIO: u8 = 10;
pub const DEFAULT_READ_SIZE: usize = 128 * 1024;
pub const DEFAULT_CACHE_TIMEOUT: u64 = 60;
