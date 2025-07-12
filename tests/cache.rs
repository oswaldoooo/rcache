use std::io::Write;

use md5::Digest;
use tokio::io::AsyncReadExt;
macro_rules! insert_and_verify {
    ($cache:ident,$meta:ident,$data:expr) => {{
        $cache.put($meta.clone(), $data.clone()).await?;
        let m2 = $cache.get($meta.hashkey().as_slice()).await?;
        assert!(m2.is_some(), "expect not nil,get nil");
        let (_meta, content) = m2.unwrap();
        assert!(content == $data, "content not match");
    }};
}
///基础功能测试
#[tokio::test]
async fn base() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    let sys_file = rcache::cache::v1::SystemFile::build(".test");
    #[cfg(feature = "_sled")]
    let db = {
        let db = sled::open(".test.db")?;
        rcache::database::sled::SledDatabase::new(db, rcache::system_file_remove!(sys_file), 50)
    };

    let flag = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0));

    #[cfg(feature = "_rocksdb")]
    let db = {
        rcache::database::rocksdb::RocksdbDb::new(
            rcache::database::rocksdb::RocksDb::open(".test-rocksdb", None, flag.clone()),
            rcache::system_file_remove!(sys_file),
        )
    };
    let cache = rcache::cache::v1::Cache::new(db, None, sys_file);
    let data = b"hello world!";
    let meta = rcache::ObjectMeta::build(3, "/hello/world".to_string(), data);
    cache.put(meta.clone(), data.to_vec()).await?;
    let m2 = cache.get(meta.hashkey().as_slice()).await?;
    assert!(m2.is_some(), "expect not nil,get nil");
    let (m2, d2) = m2.unwrap();
    let mut hsh = md5::Md5::new();
    let _ = hsh.write_all(&d2);
    let buff: [u8; 16] = hsh.finalize().into();
    assert!(
        meta.content_md5 == hex::encode(buff) && meta.content_md5 == m2.content_md5,
        "content md5 not match"
    );
    cache.remove(meta.hashkey().as_slice()).await?;
    let ret = cache.get(meta.hashkey().as_slice()).await?;
    assert!(ret.is_none(), "expect nil,but get not nil");
    //ttl verify
    let meta = rcache::ObjectMeta::build(3, "/hello/world".to_string(), data);
    cache.put(meta.clone(), data.to_vec()).await?;
    let m2 = cache.get(meta.hashkey().as_slice()).await?;
    assert!(m2.is_some(), "expect not nil,get nil");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    let ret = cache.get(meta.hashkey().as_slice()).await?;
    assert!(ret.is_none(), "expect nil,but get not nil");
    Ok(())
}

///淘汰机制测试
#[tokio::test]
async fn test_drop() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    let sys_file = rcache::cache::v1::SystemFile::build(".test");
    #[cfg(feature = "_sled")]
    let db = {
        let db = sled::open(".test.db")?;
        rcache::database::sled::SledDatabase::new(db, rcache::system_file_remove!(sys_file), 50)
    };

    let flag = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0));

    #[cfg(feature = "_rocksdb")]
    let db = {
        rcache::database::rocksdb::RocksdbDb::new(
            rcache::database::rocksdb::RocksDb::open(
                ".test-rocksdb",
                Some(Box::new(rcache::cache::v1::rocksdb_drop_strategy(
                    None,
                    flag.clone(),
                ))),
                flag,
            ),
            rcache::system_file_remove!(sys_file),
        )
    };
    let cache = rcache::cache::v1::Cache::new(db, None, sys_file.clone());
    let cache = std::sync::Arc::new(cache);
    sys_file
        .start_watch(3, 20 << 20, {
            let cache = cache.clone();
            move |_size| {
                let cache = cache.clone();
                async move {
                    cache.start_full_drop().await;
                }
            }
        })
        .await;
    rcache::cache::v1::start_update_score(cache.clone(), 3).await;
    let mut buff = vec![0u8; 10 << 20];
    random(&mut buff).await?;
    let mut meta = rcache::ObjectMeta::build(60, "/hello/world".to_string(), &buff);
    meta.score = 1000;
    cache.put(meta, buff).await?;
    let mut meta = rcache::ObjectMeta::build(60, "/hello/l0".to_string(), b"hello!");
    meta.flag |= rcache::database::F_L0;
    insert_and_verify!(cache, meta, b"hello!".to_vec());
    // cache.put(meta, b"hello!".to_vec()).await?;
    let mut buff = vec![0u8; 15 << 20];
    random(&mut buff).await?;
    let mut meta = rcache::ObjectMeta::build(60, "/hello/world2".to_string(), &buff);
    meta.score = 10;
    cache.put(meta, buff).await?;
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;
    let hashkey = rcache::build_hashkey("/hello/world2");
    assert!(
        cache.get(&hashkey).await?.is_none(),
        "expect nil,got not nil"
    );
    let hashkey = rcache::build_hashkey("/hello/world");
    assert!(
        cache.get(&hashkey).await?.is_some(),
        "expect not nil,got nil"
    );
    let hashkey = rcache::build_hashkey("/hello/l0");
    assert!(
        cache.get(&hashkey).await?.is_none(),
        "expect nil,got not nil"
    );
    Ok(())
}
///嵌入式gc
#[cfg(feature = "_rocksdb")]
#[tokio::test]
async fn test_inner_drop() {}

async fn random(buff: &mut [u8]) -> Result<usize, tokio::io::Error> {
    let mut fd = tokio::fs::OpenOptions::new()
        .read(true)
        .open("/dev/urandom")
        .await?;
    let size = fd.read_exact(buff).await?;
    Ok(size)
}
