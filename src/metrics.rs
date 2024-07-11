use once_cell::sync::Lazy;
use prometheus::{self, IntCounter};

use prometheus::register_int_counter;

pub static METRIC_CACHE_HITS: Lazy<IntCounter> = Lazy::new(||
    register_int_counter!("piproxy_cache_hit_count", "Number of full and partial cache hits").unwrap()
);

pub static METRIC_CACHE_HITS_FULL: Lazy<IntCounter> = Lazy::new(||
    register_int_counter!("piproxy_cache_hit_full_count", "Number of full cache hits").unwrap()
);

pub static METRIC_CACHE_HITS_PARTIAL: Lazy<IntCounter> = Lazy::new(||
    register_int_counter!("piproxy_cache_hit_partial_count", "Number of partial cache hits").unwrap()
);

pub static METRIC_CACHE_MISSES: Lazy<IntCounter> = Lazy::new(||
    register_int_counter!("piproxy_cache_miss_count", "Number of cache misses").unwrap()
);

pub static METRIC_CACHE_PURGES: Lazy<IntCounter> = Lazy::new(||
    register_int_counter!("piproxy_cache_purge_count", "Number of cache purges").unwrap()
);

pub static METRIC_CACHE_LOOKUP_ERRORS: Lazy<IntCounter> = Lazy::new(||
    register_int_counter!("piproxy_cache_lookup_error_count", "Number of cache lookup errors").unwrap()
);

pub static METRIC_CACHE_META_UPDATES: Lazy<IntCounter> = Lazy::new(||
    register_int_counter!("piproxy_cache_meta_update_count", "Number of cache meta updates").unwrap()
);

pub static METRIC_WARN_STALE_PARTIAL_EXISTS: Lazy<IntCounter> = Lazy::new(||
    register_int_counter!("piproxy_warn_stale_partial_count", "Count of stale partial files").unwrap()
);

pub static METRIC_WARN_MISSING_DATA_FILE: Lazy<IntCounter> = Lazy::new(||
    register_int_counter!("piproxy_warn_missing_data_file_count", "Number of cache meta updates").unwrap()
);

pub fn unlazy() {
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
