use std::fmt::Display;
use std::io;

use once_cell::sync::Lazy;
use prometheus::{
    self, labels, opts, register, register_int_counter, register_int_gauge, IntCounter, IntGauge,
    PullingGauge,
};

pub fn register_metric(
    name: &str,
    help: &str,
    f: &'static (dyn Fn() -> f64 + Sync + Send),
) -> io::Result<()> {
    let metric = PullingGauge::new(name, help, Box::new(f)).unwrap();
    register(Box::new(metric)).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

pub fn register_constant<T>(name: &str, help: &str, value: T) -> io::Result<()>
where
    T: TryInto<i64> + Display + Copy,
{
    let g = register_int_gauge!(name, help).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    g.set(value.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{value} does not fit into i64"),
        )
    })?);
    Ok(())
}

pub fn register_label(name: &str, help: &str, label: &str, value: &str) -> io::Result<()> {
    let opts = opts!(name, help, labels! {label => value});
    let g = IntGauge::with_opts(opts).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    g.set(1);
    register(Box::new(g)).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    Ok(())
}

pub static METRIC_REQUEST_COUNT: Lazy<IntCounter> =
    Lazy::new(|| register_int_counter!("piproxy_request_count", "Number of requests").unwrap());

pub static METRIC_REQUEST_ERROR_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_request_error_count",
        "Number of requests with errors"
    )
    .unwrap()
});

pub static METRIC_UPSTREAM_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_upstream_bytes",
        "Number of bytes downloaded from upstream"
    )
    .unwrap()
});

pub static METRIC_DOWNSTREAM_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_downstream_bytes",
        "Number of bytes sent to downstream"
    )
    .unwrap()
});

pub static METRIC_CACHE_HITS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_cache_hit_count",
        "Number of full and partial cache hits"
    )
    .unwrap()
});

pub static METRIC_CACHE_HITS_FULL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("piproxy_cache_hit_full_count", "Number of full cache hits").unwrap()
});

pub static METRIC_CACHE_HITS_PARTIAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_cache_hit_partial_count",
        "Number of partial cache hits"
    )
    .unwrap()
});

pub static METRIC_CACHE_MISSES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("piproxy_cache_miss_count", "Number of cache misses").unwrap()
});

pub static METRIC_CACHE_PURGES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("piproxy_cache_purge_count", "Number of cache purges").unwrap()
});

pub static METRIC_CACHE_LOOKUP_ERRORS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_cache_lookup_error_count",
        "Number of cache lookup errors"
    )
    .unwrap()
});

pub static METRIC_CACHE_META_UPDATES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_cache_meta_update_count",
        "Number of cache meta updates"
    )
    .unwrap()
});

pub static METRIC_WARN_STALE_PARTIAL_EXISTS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_warn_stale_partial_count",
        "Number of warnings for existing stale partial files"
    )
    .unwrap()
});

pub static METRIC_WARN_MISSING_DATA_FILE: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "piproxy_warn_missing_data_file_count",
        "Number of warnings for missing data files"
    )
    .unwrap()
});

pub fn unlazy() {
    METRIC_REQUEST_COUNT.get();
    METRIC_REQUEST_ERROR_COUNT.get();
    METRIC_UPSTREAM_BYTES.get();
    METRIC_DOWNSTREAM_BYTES.get();
    METRIC_CACHE_HITS.get();
    METRIC_CACHE_HITS_FULL.get();
    METRIC_CACHE_HITS_PARTIAL.get();
    METRIC_CACHE_MISSES.get();
    METRIC_CACHE_PURGES.get();
    METRIC_CACHE_LOOKUP_ERRORS.get();
    METRIC_CACHE_META_UPDATES.get();
    METRIC_WARN_STALE_PARTIAL_EXISTS.get();
    METRIC_WARN_MISSING_DATA_FILE.get();
}
