use log::LevelFilter;
use pico_args::Arguments;
use pingora::server::configuration::Opt;
use pingora::server::configuration::ServerConf;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::process::exit;
use std::str;
use std::str::FromStr;

use crate::defaults::DEFAULT_ADDRESS;
use crate::defaults::DEFAULT_CACHE_PATH;
use crate::defaults::DEFAULT_CACHE_SIZE;
use crate::defaults::DEFAULT_CACHE_TIMEOUT;
use crate::defaults::DEFAULT_LOG_LEVEL;
use crate::defaults::DEFAULT_READ_SIZE;

const HELP: &str = "
piproxy

OPTIONS:
    -u, --upgrade
      Whether this server should try to upgrade from an running old server

    -d, --daemon
      Whether should run this server in the background

    -t, --test
      Test the configuration and exit

    -c, --conf <conf>
      The path to the configuration file

    -a, --address <address>
      Bind address

    -p, --cache-path <cache_path>
      Path where cached files are stored

    -s, --cache-size <cache_size>
      Maximum size of the cache

    -r, --read-size <read_size>
      Read size when reading from cache

    -l, --log-level <log_level>
      Set the log level

    -t, --cache-lock-timeout <cache_timeout>
      How long to wait for cache locks

    -h, --help
      Prints help information.
";

impl Default for Config {
    fn default() -> Self {
        Self {
            upgrade: false,
            daemon: false,
            test: false,
            log_level: DEFAULT_LOG_LEVEL,
            address: DEFAULT_ADDRESS.to_owned(),
            cache_path: DEFAULT_CACHE_PATH.into(),
            cache_size: DEFAULT_CACHE_SIZE,
            read_size: DEFAULT_READ_SIZE,
            cache_timeout: DEFAULT_CACHE_TIMEOUT,
            pingora: ServerConf::default(),
        }
    }
}

fn default_address() -> String { DEFAULT_ADDRESS.to_owned() }
fn default_log_level() -> LevelFilter { DEFAULT_LOG_LEVEL }
fn default_cache_path() -> PathBuf { DEFAULT_CACHE_PATH.into() }
fn default_cache_size() -> usize { DEFAULT_CACHE_SIZE }
fn default_read_size() -> usize { DEFAULT_READ_SIZE }
fn default_cache_timeout() -> u64 { DEFAULT_CACHE_TIMEOUT }

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub upgrade: bool,
    #[serde(default)]
    pub daemon: bool,
    #[serde(default)]
    pub test: bool,
    #[serde(default = "default_log_level")]
    pub log_level: LevelFilter,
    #[serde(default = "default_address")]
    pub address: String,
    #[serde(default = "default_cache_path")]
    pub cache_path: PathBuf,
    #[serde(default = "default_cache_size")]
    pub cache_size: usize,
    #[serde(default = "default_read_size")]
    pub read_size: usize,
    #[serde(default = "default_cache_timeout")]
    pub cache_timeout: u64,
    #[serde(default)]
    pub pingora: ServerConf,
}

fn eother<E>(error: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::Other, error.into())
}

fn from_unit_str(s: &str) -> std::result::Result<usize, parse_size::Error> {
    let v = parse_size::Config::new().with_binary().parse_size(s)?;
    usize::try_from(v).map_err(|_| parse_size::Error::PosOverflow)
}

impl Config {
    pub fn update_from_args(&mut self, flags: &mut Arguments) -> io::Result<()> {
        macro_rules! update_switch {
            ($field:ident, $keys:expr) => {
                self.$field |= flags.contains($keys);
            };
        }
        macro_rules! update_value {
            ($field:ident, $keys:expr, $fname:expr) => {
                self.$field = match flags.opt_value_from_fn($keys, $fname) {
                    Ok(Some(v)) => v,
                    Ok(None) => self.$field.clone(),
                    Err(err) => return Err(eother(err)),
                };
            };
            ($field:ident, $keys:expr) => {
                update_value!($field, $keys, FromStr::from_str);
            };
        }

        update_switch!(upgrade, ["-u", "--upgrade"]);
        update_switch!(daemon, ["-d", "--daemon"]);
        update_switch!(test, ["-t", "--test"]);
        update_value!(log_level, ["-l", "--log-level"]);
        update_value!(address, ["-a", "--address"]);
        update_value!(cache_path, ["-p", "--cache-path"]);
        update_value!(cache_size, ["-s", "--cache-size"], from_unit_str);
        update_value!(read_size, ["-s", "--read-size"], from_unit_str);
        update_value!(cache_timeout, "--cache_timeout");

        self.pingora.merge_with_opt(&self.opt());
        Ok(())
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

pub fn parse() -> io::Result<Config> {
    let mut args = pico_args::Arguments::from_env();
    if args.contains(["-h", "--help"]) {
        println!("{}", HELP);
        exit(0);
    }
    let conf_path: Result<String, pico_args::Error> = args.value_from_str(["-c", "--conf"]);
    let mut conf = if let Ok(path) = conf_path {
        Config::load_from_yaml(path)?
    } else {
        Config::default()
    };
    conf.update_from_args(&mut args)?;
    Ok(conf)
}
