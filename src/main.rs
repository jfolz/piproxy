use pingora::prelude::*;

mod defaults;
mod error;
mod flags;
mod logger;
mod proxy;
mod storage;

fn main() {
    logger::install().unwrap();
    let flags = flags::Piproxy::from_env_or_exit();
    logger::set_level(flags.get_log_level());
    proxy::setup(
        flags.get_cache_path(),
        flags.get_cache_size(),
        flags.get_cache_lock_timeout(),
        flags.get_read_size(),
    );
    proxy::populate_lru(&flags.get_cache_path()).unwrap();

    let pingora_opts = Opt {
        upgrade: flags.upgrade,
        daemon: flags.daemon,
        nocapture: false,
        test: flags.test,
        conf: flags.conf.clone(),
    };
    let mut my_server = Server::new(pingora_opts).unwrap();
    my_server.bootstrap();
    let inner = proxy::PyPI::new();
    let mut pypi = http_proxy_service(&my_server.configuration, inner);
    pypi.add_tcp(flags.get_address());
    my_server.add_service(pypi);
    my_server.run_forever();
}
