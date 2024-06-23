use crate::defaults::*;
use log::LevelFilter;
use std::path::PathBuf;
use std::str;
use std::str::FromStr;

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

xflags::xflags! {
    cmd piproxy {
        /// Whether this server should try to upgrade from an running old server
        optional -u,--upgrade
        /// Whether should run this server in the background
        optional -d,--daemon
        /// Test the configuration and exit
        optional -t,--test
        /// The path to the configuration file
        optional -c,--conf conf: String
        /// Bind address
        optional -a,--address address: String
        /// Path where cached files are stored
        optional -p,--cache-path cache_path: PathBuf
        /// Path where cached files are stored
        optional -s,--cache-size cache_size: Unit
        /// Path where cached files are stored
        optional -r,--chunk-size chunk_size: Unit
        /// Set the log level
        optional -l,--log-level log_level: LevelFilter
        /// Set the log level
        optional -t,--cache-lock-timeout cache_lock_timeout: u64
    }
}

macro_rules! getter_unit {
    ($field:ident, $default:expr) => {
        paste::paste! {
            pub fn [<get_ $field>](&self) -> usize {
                self.$field.as_ref().map_or($default, |v| v.0)
            }
        }
    };
}

macro_rules! getter_default {
    ($field:ident, $type:ident, $default:expr) => {
        paste::paste! {
            pub fn [<get_ $field>](&self) -> $type {
                self.$field.unwrap_or($default)
            }
        }
    };
}

impl Piproxy {
    pub fn get_address(&self) -> &str {
        self.address.as_deref().unwrap_or("localhost:6188")
    }
    pub fn get_cache_path(&self) -> PathBuf {
        self.cache_path
            .clone()
            .unwrap_or_else(|| PathBuf::from("cache"))
    }
    getter_unit!(chunk_size, DEFAULT_CHUNK_SIZE);
    getter_unit!(cache_size, DEFAULT_CACHE_SIZE);
    getter_default!(log_level, LevelFilter, DEFAULT_LOG_LEVEL);
    getter_default!(cache_lock_timeout, u64, DEFAULT_CACHE_TIMEOUT);
}
