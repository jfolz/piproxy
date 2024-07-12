# piproxy

Piproxy is a caching PyPI proxy, built with
[Cloudflare Pingora](https://www.pingorarust.com).
It follows the cache-control instructions returned by PyPI,
so index pages are valid for 10 minutes and package files never expire.
Cache eviction is LRU.

## Installation

Simply download the release archive, extract, and run.

There is also an [Ansible role](https://github.com/jfolz/ansible-role-piproxy)
to install and run as systemd service.

To build from source, you need a working Rust toolchain.
Simply run `cargo build` to build.
Binaries avilable from the release section are built with
`cargo build --profile prod`.
Currently, piproxy does not have an MSRV policy,
so please use the latest available version of Rust to compile.

## Configuration

Piproxy does not require much configuration.
Mostly you will want to set the cache size and path.
Config files use YAML syntax:

```yaml
---
# Whether to log to systemd journal
journal: false,
# Proxy address
address: localhost:8080
# Prometheus metrics endpoint address
prometheus_address: localhost:9898
# Path to cache directory
cache_path: cache
# Cache size limit, e.g. 15M, 7G, 1T
cache_size: 1G
# Max file size that can be admitted to the cache in percent [1..100] of cache size
cache_ratio: 10
# (advanced usage) How long to wait in seconds to acquire a cache lock
cache_timeout: 60
# Size of chunks read from cached files
read_size: 128k
# Set verbosity of logs
log_level: info
# Pingora-specific config
pingora:
  threads: 1
```

See [Pingora docs](https://www.pingorarust.com/user_guide/conf) for possible options under the `pingora` key.
Typically, you only want to set `threads` to however many CPUs you assign to piproxy.

Most options are also available from the command line:
```
Usage: piproxy-prod [OPTIONS]

Options:
  -c, --conf <CONF>                    Path to config file
  -u, --upgrade                        Perform upgrade from another instance
  -d, --daemon                         Daemonize on launch
  -j, --journal                        Send output to journal
  -t, --test                           Test the configuration
  -l, --log-level <LOG_LEVEL>          [possible values: off, error, warn, info, debug, trace]
  -a, --address <ADDRESS>              Bind address and port
  -p, --cache-path <CACHE_PATH>        Where to store cached files
  -s, --cache-size <CACHE_SIZE>        The cache size in bytes or with unit, e.g. 15M, 7G, 1T, ...
      --cache-ratio <CACHE_RATIO>      Max file size that can be admitted to the cache in percent [1..100] of cache size
      --cache-timeout <CACHE_TIMEOUT>  (advanced usage) How long to wait in seconds to acquire a cache lock
  -r, --read-size <READ_SIZE>          Size of chunks read from cached files
  -h, --help                           Print help
  -V, --version                        Print version
```

## Configure pip

We recommend environment variables to tell pip to use your cache.
The following assumes piproxy is reachable at `pypi-cache`:

```
export PIP_INDEX_URL=http://pypi-cache/simple
export PIP_TRUSTED_HOST=pypi-cache
export PIP_NO_CACHE_DIR=true
```

Alternatively, you can add the equivalent parameters on the command line:
```
pip install --index-url http://pypi-cache/simple --trusted-host pypi-cache --no-cache-dir ...
```

## Acknowledgements

Piproxy is supported by the [SustainML](https://sustainml.eu/) project.