use pingora::prelude::*;
use std::process::exit;

mod defaults;
mod error;
mod flags;
mod logger;
mod proxy;
mod storage;

fn main() {
    logger::install().unwrap();
    let conf = match flags::Config::new_from_env() {
        Ok(conf) => conf,
        Err(err) => {
            println!("{err:?}");
            exit(1);
        }
    };
    logger::set_level(conf.log_level);
    proxy::setup(
        conf.cache_path.clone(),
        conf.cache_size,
        conf.cache_timeout,
        conf.read_size,
    );
    proxy::populate_lru(&conf.cache_path).unwrap();

    let mut my_server = Server::new_with_opt_and_conf(conf.opt(), conf.pingora);
    my_server.bootstrap();
    let inner = proxy::PyPI::new();
    let mut pypi = http_proxy_service(&my_server.configuration, inner);
    pypi.add_tcp(&conf.address);
    my_server.add_service(pypi);
    my_server.run_forever();
}
