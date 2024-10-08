use clap::builder::TypedValueParser;
use clap::value_parser;
use clap::{builder::PossibleValuesParser, Parser};
use log::LevelFilter;
use pingora::server::configuration::Opt;
use pingora::server::configuration::ServerConf;
use serde::de::{self, Visitor};
use serde::Deserializer;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::str;
use std::str::FromStr;

use crate::defaults::{
    DEFAULT_ADDRESS, DEFAULT_CACHE_PATH, DEFAULT_CACHE_RATIO, DEFAULT_CACHE_SIZE,
    DEFAULT_CACHE_TIMEOUT, DEFAULT_LOG_LEVEL, DEFAULT_PROMETHEUS_ADDRESS, DEFAULT_READ_SIZE,
};

#[derive(Debug, Clone)]
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

impl From<Unit> for usize {
    fn from(value: Unit) -> Self {
        value.0
    }
}

struct UnitVisitor;

impl<'de> Visitor<'de> for UnitVisitor {
    type Value = Unit;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a number with unit (15M, 7G, 1T, ...)")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Unit::from_str(value).map_err(|_| E::invalid_value(de::Unexpected::Str(value), &self))
    }
}

impl<'de> Deserialize<'de> for Unit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UnitVisitor)
    }
}

#[derive(Parser, Debug)]
#[command(version)]
#[allow(clippy::struct_excessive_bools)]
pub struct Args {
    /// Path to config file
    #[arg(short, long)]
    pub conf: Option<PathBuf>,
    /// Perform upgrade from another instance
    #[arg(short, long)]
    pub upgrade: bool,
    /// Daemonize on launch
    #[arg(short, long)]
    pub daemon: bool,
    /// Send output to journal
    #[arg(short, long)]
    pub journal: bool,
    /// Test the configuration
    #[arg(short, long)]
    pub test: bool,
    #[arg(
        short,
        long,
        ignore_case = true,
        // unwrap is safe here, since only valid strings are passed on by the PossibleValuesParser
        value_parser = PossibleValuesParser::new(["off", "error", "warn", "info", "debug", "trace"]).map(|s| LevelFilter::from_str(&s).unwrap()),
    )]
    pub log_level: Option<LevelFilter>,
    /// Bind address and port
    #[arg(short, long)]
    pub address: Option<String>,
    /// Where to store cached files
    #[arg(short = 'p', long)]
    pub cache_path: Option<PathBuf>,
    /// The cache size in bytes or with unit, e.g. 15M, 7G, 1T, ...
    #[arg(short = 's', long)]
    pub cache_size: Option<Unit>,
    /// Max file size that can be admitted to the cache in percent [1..100] of cache size
    #[arg(long, value_parser = value_parser!(u8).range(1..=100))]
    pub cache_ratio: Option<u8>,
    /// (advanced usage) How long to wait in seconds to acquire a cache lock
    #[arg(long)]
    pub cache_timeout: Option<u64>,
    /// Size of chunks read from cached files
    #[arg(short, long)]
    pub read_size: Option<Unit>,
}

fn default_address() -> String {
    DEFAULT_ADDRESS.to_owned()
}
fn default_prometheus_address() -> String {
    DEFAULT_PROMETHEUS_ADDRESS.to_owned()
}
fn default_log_level() -> LevelFilter {
    DEFAULT_LOG_LEVEL
}
fn default_cache_path() -> PathBuf {
    DEFAULT_CACHE_PATH.into()
}
fn default_cache_size() -> usize {
    DEFAULT_CACHE_SIZE
}
fn default_cache_ratio() -> u8 {
    DEFAULT_CACHE_RATIO
}
fn default_read_size() -> usize {
    DEFAULT_READ_SIZE
}
fn default_cache_timeout() -> u64 {
    DEFAULT_CACHE_TIMEOUT
}

fn deserialize_unit<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    Unit::deserialize(deserializer).map(Unit::into)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct Config {
    #[serde(default)]
    pub upgrade: bool,
    #[serde(default)]
    pub daemon: bool,
    #[serde(default)]
    pub test: bool,
    #[serde(default)]
    pub journal: bool,
    #[serde(default = "default_log_level")]
    pub log_level: LevelFilter,
    #[serde(default = "default_address")]
    pub address: String,
    #[serde(default = "default_prometheus_address")]
    pub prometheus_address: String,
    #[serde(default = "default_cache_path")]
    pub cache_path: PathBuf,
    #[serde(default = "default_cache_size", deserialize_with = "deserialize_unit")]
    pub cache_size: usize,
    #[serde(default = "default_cache_ratio")]
    pub cache_ratio: u8,
    #[serde(default = "default_cache_timeout")]
    pub cache_timeout: u64,
    #[serde(default = "default_read_size", deserialize_with = "deserialize_unit")]
    pub read_size: usize,
    #[serde(default)]
    pub pingora: ServerConf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            upgrade: false,
            daemon: false,
            test: false,
            journal: false,
            log_level: DEFAULT_LOG_LEVEL,
            address: DEFAULT_ADDRESS.to_owned(),
            prometheus_address: DEFAULT_PROMETHEUS_ADDRESS.to_owned(),
            cache_path: DEFAULT_CACHE_PATH.into(),
            cache_size: DEFAULT_CACHE_SIZE,
            cache_ratio: DEFAULT_CACHE_RATIO,
            cache_timeout: DEFAULT_CACHE_TIMEOUT,
            read_size: DEFAULT_READ_SIZE,
            pingora: ServerConf::default(),
        }
    }
}

fn eother<E>(error: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::Other, error.into())
}

impl Config {
    pub fn new_from_env() -> io::Result<Self> {
        let args = Args::parse();
        let mut conf = Self::default();
        if let Some(path) = &args.conf {
            conf = Self::load_from_yaml(path)?;
        }
        conf.update_from_args(&args);
        Ok(conf)
    }

    pub fn update_from_args(&mut self, args: &Args) {
        // update switches
        self.upgrade |= args.upgrade;
        self.daemon |= args.daemon;
        self.test |= args.test;
        self.journal |= args.journal;

        // update optionals
        macro_rules! update_values {
            ($field:ident) => {
                if let Some(value) = args.$field.clone() {
                    self.$field = value.into();
                };
            };
            ($field:ident, $($fields:ident),+) => {
                update_values!($field);
                update_values!($($fields),+);
            };
        }
        update_values!(
            log_level,
            address,
            cache_path,
            cache_size,
            cache_ratio,
            cache_timeout,
            read_size
        );

        // update pingora server config
        self.pingora.merge_with_opt(&self.opt());
        // cannot validate pingora config, because it would move self.pingora
        // self.pingora = self.pingora.validate().map_err(eother)?;
    }

    pub fn load_from_yaml<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let conf_str = fs::read_to_string(&path)?;
        Self::from_yaml(&conf_str)
    }

    pub fn from_yaml(conf_str: &str) -> io::Result<Self> {
        let mut conf: Self = serde_yaml::from_str(conf_str).map_err(eother)?;
        conf.pingora = conf.pingora.validate().map_err(eother)?;
        Ok(conf)
    }

    pub fn opt(&self) -> Opt {
        Opt {
            upgrade: self.upgrade,
            daemon: self.daemon,
            nocapture: false,
            test: self.test,
            conf: None,
        }
    }
}
