// implement basic usage for put,get,remove

#[tokio::main]
async fn main() {
    let cache_dir = ".test".to_string();
    let cache_db_dir = ".test-db";
    let flag = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0));
    let rocks_db = rcache::database::rocksdb::RocksDb::open(
        cache_db_dir,
        // this will be create simple gc at rocksdb compaction phase
        Some(Box::new(rcache::cache::v1::rocksdb_drop_strategy(
            None,
            flag.clone(),
        ))),
        flag,
    );
    let sys_file = rcache::cache::v1::SystemFile::build(cache_dir);
    let db =
        rcache::database::rocksdb::RocksdbDb::new(rocks_db, rcache::system_file_remove!(sys_file));
    let db = rcache::cache::v1::Cache::new(db, None, sys_file);
    let data = br#"{"code":0,"msg":"ok"}"#;
    let mut meta = rcache::ObjectMeta::build(30, "http://example.com/", data);
    meta.metadata
        .insert("status_code".to_string(), "200".to_string());
    db.put(meta.clone(), data.to_vec()).await.unwrap();
    let (meta, data) = db.get(&meta.hashkey()).await.unwrap().unwrap();
    println!("status_code={}\n{}", meta.metadata["status_code"], unsafe {
        std::str::from_utf8_unchecked(&data)
    });
    // db.put(, data)
}
