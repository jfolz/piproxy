use pingora::prelude::*;

mod defaults;
mod logger;
mod flags;
mod storage;
mod proxy;

fn main() {
    logger::install().unwrap();
    let flags = flags::Piproxy::from_env_or_exit();
    logger::set_level(flags.get_log_level());
    proxy::setup(&flags);

    let pingora_opts = Opt {
        upgrade: flags.upgrade,
        daemon: flags.daemon,
        nocapture: false,
        test: flags.test,
        conf: flags.conf.to_owned(),
    };
    let mut my_server = Server::new(pingora_opts).unwrap();
    my_server.bootstrap();
    let inner = proxy::PyPIProxy::new();
    let mut pypi = http_proxy_service(&my_server.configuration, inner);
    pypi.add_tcp(flags.get_address());
    my_server.add_service(pypi);
    my_server.run_forever();
}
