use lazy_static::lazy_static;

lazy_static! {
    pub static ref cache_hash_error: prometheus::Counter =
        prometheus::register_counter!("cache_hash_error", "cache file is not expect md5").unwrap();
    pub static ref file_error: prometheus::Counter =
        prometheus::register_counter!("file_error", "file operate error").unwrap();
    pub static ref disk_use: prometheus::Gauge =
        prometheus::register_gauge!("disk_use", "file operate error").unwrap();
}
