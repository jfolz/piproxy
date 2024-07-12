use pingora::prelude::*;
use pingora::services::listening::Service;
use std::{io, process::exit};

mod defaults;
mod error;
mod flags;
mod logger;
mod metrics;
mod proxy;
mod storage;

fn main() -> io::Result<()> {
    let conf = match flags::Config::new_from_env() {
        Ok(conf) => conf,
        Err(err) => {
            println!("{err:?}");
            exit(1);
        }
    };

    if let Err(err) = logger::install(conf.journal) {
        if conf.journal {
            log::error!("Could not connect to the journal, falling back to stdout: {err}");
            logger::install(false)?;
        }
    };
    logger::set_level(conf.log_level);
    log::debug!("{conf:#?}");

    proxy::setup(
        conf.cache_path.clone(),
        conf.cache_size,
        conf.cache_ratio,
        conf.cache_timeout,
        conf.read_size,
    )?;
    proxy::populate_lru(&conf.cache_path)?;
    metrics::register_label(
        "piproxy_version",
        "Version string",
        "version",
        env!("CARGO_PKG_VERSION"),
    )?;
    metrics::unlazy();

    let mut server = Server::new_with_opt_and_conf(conf.opt(), conf.pingora);
    server.bootstrap();
    let inner = proxy::PyPI::new();
    let mut pypi = http_proxy_service(&server.configuration, inner);
    pypi.add_tcp(&conf.address);

    let mut prometheus_service_http = Service::prometheus_http_service();
    prometheus_service_http.add_tcp(&conf.prometheus_address);
    server.add_service(prometheus_service_http);

    server.add_service(pypi);
    server.run_forever();
}
