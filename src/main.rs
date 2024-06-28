use pingora::prelude::*;

mod defaults;
mod error;
mod flags;
mod logger;
mod proxy;
mod storage;

fn main() {
    logger::install().unwrap();
    let conf = flags::parse().unwrap();
    logger::set_level(conf.piproxy.log_level);
    proxy::setup(
        conf.piproxy.cache_path.clone(),
        conf.piproxy.cache_size,
        conf.piproxy.cache_timeout,
        conf.piproxy.read_size,
    );
    proxy::populate_lru(&conf.piproxy.cache_path).unwrap();

    let mut my_server = Server::new(conf.opt()).unwrap();
    my_server.bootstrap();
    let inner = proxy::PyPI::new();
    let mut pypi = http_proxy_service(&my_server.configuration, inner);
    pypi.add_tcp(&conf.piproxy.address);
    my_server.add_service(pypi);
    my_server.run_forever();
}
