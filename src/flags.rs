use log::LevelFilter;
use pingora::server::configuration::Opt;
use pingora::server::configuration::ServerConf;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::str;
use std::str::FromStr;

use crate::defaults::DEFAULT_ADDRESS;
use crate::defaults::DEFAULT_CACHE_PATH;
use crate::defaults::DEFAULT_CACHE_SIZE;
use crate::defaults::DEFAULT_CACHE_TIMEOUT;
use crate::defaults::DEFAULT_LOG_LEVEL;
use crate::defaults::DEFAULT_READ_SIZE;

#[derive(Debug)]
pub struct Unit(pub usize);

impl FromStr for Unit {
    type Err = io::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let v = parse_size::Config::new()
            .with_binary()
            .parse_size(s)
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        let v = usize::try_from(v).map_err(|_| io::ErrorKind::InvalidInput)?;
        Ok(Self(v))
    }
}

xflags::xflags! {
    cmd flags {
        /// Whether this server should try to upgrade from an running old server
        optional -u,--upgrade
        /// Whether should run this server in the background
        optional -d,--daemon
        /// Test the configuration and exit
        optional -t,--test
        /// Set the log level
        optional -l,--log-level log_level: LevelFilter
        /// The path to the configuration file
        optional -c,--conf conf: String
        /// Bind address
        optional -a,--address address: String
        /// Path where cached files are stored
        optional -p,--cache-path cache_path: PathBuf
        /// Maximum size of the cache
        optional -s,--cache-size cache_size: Unit
        /// Read size when reading from cache
        optional -r,--read-size read_size: Unit
        /// How long to wait for cache locks
        optional -t,--cache-lock-timeout cache_timeout: u64
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Piproxy {
    pub upgrade: bool,
    pub daemon: bool,
    pub test: bool,
    pub conf: Option<String>,
    pub log_level: LevelFilter,
    pub address: String,
    pub cache_path: PathBuf,
    pub cache_size: usize,
    pub read_size: usize,
    pub cache_timeout: u64,
}

impl Default for Piproxy {
    fn default() -> Self {
        Self {
            upgrade: false,
            daemon: false,
            test: false,
            conf: None,
            log_level: DEFAULT_LOG_LEVEL,
            address: DEFAULT_ADDRESS.to_owned(),
            cache_path: DEFAULT_CACHE_PATH.into(),
            cache_size: DEFAULT_CACHE_SIZE,
            read_size: DEFAULT_READ_SIZE,
            cache_timeout: DEFAULT_CACHE_TIMEOUT,
        }
    }
}

impl Piproxy {
    pub fn merge_flags(&mut self, flags: &Flags) {
        self.upgrade |= flags.upgrade;
        self.daemon |= flags.daemon;
        self.test |= flags.test;
        self.conf = flags.conf.clone();
        if let Some(v) = &flags.address {
            self.address = v.clone()
        }
        if let Some(v) = &flags.cache_path {
            self.cache_path = v.clone()
        }
        if let Some(v) = &flags.cache_size {
            self.cache_size = v.0
        }
        if let Some(v) = &flags.read_size {
            self.read_size = v.0
        }
        if let Some(v) = flags.log_level {
            self.log_level = v
        }
        if let Some(v) = flags.cache_lock_timeout {
            self.cache_timeout = v
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Config {
    pub piproxy: Piproxy,
    pub pingora: ServerConf,
}

fn eother<E>(error: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::Other, error.into())
}

impl Config {
    pub fn opt(&self) -> Opt {
        Opt {
            upgrade: self.piproxy.upgrade,
            daemon: self.piproxy.daemon,
            nocapture: false,
            test: self.piproxy.test,
            conf: self.piproxy.conf.clone(),
        }
    }

    pub fn load_from_yaml<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let conf_str = fs::read_to_string(&path)?;
        Self::from_yaml(&conf_str)
    }

    pub fn from_yaml(conf_str: &str) -> io::Result<Self> {
        let conf: Self = serde_yaml::from_str(conf_str).map_err(eother)?;
        // can't validate, because that would move conf.pingora
        // conf.pingora.validate().map_err(eother)?;
        Ok(conf)
    }
}

pub fn parse() -> io::Result<Config> {
    let flags = Flags::from_env_or_exit();
    let mut conf = if let Some(path) = &flags.conf {
        Config::load_from_yaml(path)?
    } else {
        Config::default()
    };
    conf.piproxy.merge_flags(&flags);
    Ok(conf)
}
