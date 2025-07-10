use std::io::Write;

use md5::Digest;
use tokio::io::AsyncReadExt;
///基础功能测试
#[tokio::test]
async fn base() -> Result<(), Box<dyn std::error::Error>> {
    let db = sled::open(".test.db")?;
    let cache = rcache::cache::v1::Cache::new(
        ".test".to_string(),
        Box::pin(rcache::database::sled::SledDatabase::new(db)),
        None,
    )
    .await;
    let data = b"hello world!";
    let meta = rcache::ObjectMeta::build(3, "/hello/world".to_string(), data);
    let ret = cache.get(meta.hashkey().as_slice()).await?;
    assert!(ret.is_none(), "expect nil,but get not nil");
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
    let db = sled::open(".test.db")?;
    let cache = rcache::cache::v1::Cache::new(
        ".test".to_string(),
        Box::pin(rcache::database::sled::SledDatabase::new(db)),
        None,
    )
    .await;
    cache
        .set_remove_cb(Box::new(|meta| {
            Box::pin(async move {
                log::info!("remove metadata {}", meta.to_hash_str);
            })
        }))
        .await;
    let cache = std::sync::Arc::new(cache);
    rcache::cache::v1::start_drop_daemon(cache.clone(), 10 << 20).await;
    rcache::cache::v1::start_update_score(cache.clone(), 100).await;
    let mut buff = vec![0u8; 10 << 20];
    random(&mut buff).await?;
    let mut meta = rcache::ObjectMeta::build(10, "/hello/world".to_string(), &buff);
    meta.score = 1000;
    cache.put(meta, buff).await?;
    let mut buff = vec![0u8; 15 << 20];
    random(&mut buff).await?;
    let mut meta = rcache::ObjectMeta::build(10, "/hello/world2".to_string(), &buff);
    meta.score = 10;
    cache.put(meta, buff).await?;
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
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
    Ok(())
}

async fn random(buff: &mut [u8]) -> Result<usize, tokio::io::Error> {
    let mut fd = tokio::fs::OpenOptions::new()
        .read(true)
        .open("/dev/urandom")
        .await?;
    let size = fd.read_exact(buff).await?;
    Ok(size)
}
