use std::{io::Write, os::unix::fs::MetadataExt};

use md5::Digest;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
const PARTITION_SIZE: usize = 8;
pub struct Cache {
    ///禁止写入开关
    write_ban: std::sync::atomic::AtomicBool,
    parent: String,
    db: std::pin::Pin<Box<dyn crate::database::Database>>,
    count_collections:
        [tokio::sync::Mutex<std::collections::BTreeMap<u64, VisitCount>>; PARTITION_SIZE],
    update_collection_index: std::sync::atomic::AtomicUsize,
    ///淘汰分数线
    min_score: u64,
    remove_cb: tokio::sync::RwLock<
        Option<
            Box<
                dyn Send
                    + Sync
                    + Fn(
                        crate::database::ObjectMeta,
                    )
                        -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
            >,
        >,
    >,
}

impl Cache {
    pub async fn new(
        parent: String,
        db: std::pin::Pin<Box<dyn crate::database::Database>>,
        min_score: Option<u64>,
    ) -> Self {
        match tokio::fs::metadata(parent.as_str()).await {
            Ok(meta) => {
                assert!(meta.is_dir(), "{parent} is not directory");
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => {
                    tokio::fs::create_dir_all(parent.as_str())
                        .await
                        .expect("create dir failed");
                }
                _ => {
                    panic!("get metadata status error {err}");
                }
            },
        }
        Self {
            write_ban: std::sync::atomic::AtomicBool::default(),
            parent: parent,
            db: db,
            count_collections: [const {
                tokio::sync::Mutex::const_new(std::collections::BTreeMap::new())
            }; PARTITION_SIZE],
            min_score: min_score.unwrap_or(50),
            remove_cb: tokio::sync::RwLock::const_new(None),
            update_collection_index: Default::default(),
        }
    }
    pub async fn set_remove_cb(
        &self,
        cb: Box<
            dyn Send
                + Sync
                + Fn(
                    crate::database::ObjectMeta,
                )
                    -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
        >,
    ) {
        (*self.remove_cb.write().await) = Some(cb);
    }
    pub async fn metadata(
        &self,
        hashkey: &[u8],
    ) -> Result<Option<crate::database::ObjectMeta>, String> {
        let meta = self.db.get(hashkey).await?;
        match meta {
            Some(meta) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                if meta.expire_at <= now {
                    self.remove(hashkey)
                        .await
                        .unwrap_or_else(|err| log::error!("remove error {err}"));
                    Ok(None)
                } else {
                    Ok(Some(meta))
                }
            }
            None => Ok(None),
        }
    }
    pub async fn get(
        &self,
        hashkey: &[u8],
    ) -> Result<Option<(crate::database::ObjectMeta, Vec<u8>)>, String> {
        let meta = self.metadata(hashkey).await?;
        match meta {
            Some(meta) => {
                let ret =
                    get_file(&meta.file_path, &meta.content_md5, Some(meta.content_size)).await?;
                match ret {
                    Some(ret) => {
                        if meta.flag & crate::database::F_L0 == 0
                            && meta.flag & crate::database::F_PIN == 0
                        {
                            let hashkey = meta.hashkey();
                            let hkey = u64::from_be_bytes((&hashkey[..8]).try_into().unwrap());
                            let collect = &self.count_collections[hkey as usize % PARTITION_SIZE];
                            {
                                let mut l = collect.lock().await;
                                l.entry(hkey)
                                    .and_modify(|ele| {
                                        ele.total
                                            .fetch_add(1, std::sync::atomic::Ordering::Release);
                                    })
                                    .or_insert(VisitCount {
                                        key: hashkey,
                                        total: std::sync::atomic::AtomicU64::new(1),
                                        latest: 0,
                                    });
                            }
                        }
                        Ok(Some((meta, ret)))
                    }
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }
    pub async fn put(
        &self,
        mut meta: crate::database::ObjectMeta,
        data: Vec<u8>,
    ) -> Result<(), String> {
        self.alloc_file_path(&mut meta);
        let file_path = meta.file_path.clone();
        self.db.put(meta).await?;
        let ret = {
            let mut fd = tokio::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(file_path.as_str())
                .await
                .map_err(|err| {
                    crate::metrics::file_error.inc();
                    format!("open file {file_path} error {err}")
                })?;
            fd.write_all(&data).await
        };
        if let Err(err) = ret {
            crate::metrics::file_error.inc();
            log::error!("open file {file_path} error {err}");
            let _ = tokio::fs::remove_file(file_path).await;
        }
        Ok(())
    }
    pub async fn remove(&self, hashkey: &[u8]) -> Result<(), String> {
        self.db.remove(hashkey).await
    }
    ///开始淘汰
    pub async fn start_full_drop(&self) {
        log::info!("start full drop...");
        if let Err(_) = self.write_ban.compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::Release,
            std::sync::atomic::Ordering::Acquire,
        ) {
            log::warn!("write ban switch on, drop it!");
            return;
        }
        let _defer = crate::defer(|| {
            log::info!("full drop complete");
            if let Err(_) = self.write_ban.compare_exchange(
                true,
                false,
                std::sync::atomic::Ordering::Release,
                std::sync::atomic::Ordering::Acquire,
            ) {
                log::error!("write ban switch closed by other routine");
            }
        });
        self.db.gc().await;
    }
    async fn update_partition(&self) {
        if let Err(_) = self.write_ban.compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::Release,
            std::sync::atomic::Ordering::Acquire,
        ) {
            log::info!("write ban switch on.skip this update");
            return;
        }
        let _defer = crate::defer(|| {
            self.update_collection_index
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            if let Err(_) = self.write_ban.compare_exchange(
                true,
                false,
                std::sync::atomic::Ordering::Release,
                std::sync::atomic::Ordering::Acquire,
            ) {
                log::info!("write ban closed by other routine.");
                return;
            }
        });
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let collections = &self.count_collections[self
            .update_collection_index
            .load(std::sync::atomic::Ordering::Acquire)
            % PARTITION_SIZE];
        {
            let mut l = collections.lock().await;
            for (_, ele) in l.iter_mut() {
                let latest = ele.total.load(std::sync::atomic::Ordering::Acquire);
                let diff = latest - ele.latest;
                ele.latest = latest;
                match self.db.get(&ele.key).await {
                    Ok(meta) => {
                        if let None = meta {
                            log::error!("not found key {}", hex::encode(&ele.key));
                            continue;
                        }
                        let mut meta = meta.unwrap();
                        if meta.flag & crate::database::F_L0 > 0
                            || meta.flag & crate::database::F_PIN > 0
                        {
                            continue;
                        } else if meta.expire_at <= now {
                            self.db.remove(&meta.hashkey()).await.unwrap_or_else(|err| {
                                log::error!("remove metadata error {err}");
                            });
                        }
                        meta.score = (meta.score >> 1) + diff;
                        self.db
                            .put(meta)
                            .await
                            .unwrap_or_else(|err| log::error!("put metadata failed {err}"));
                    }
                    Err(err) => {
                        log::error!("get key error {err}");
                    }
                }
            }
        }
    }
    fn alloc_file_path(&self, meta: &mut crate::database::ObjectMeta) {
        let hkey = hex::encode(meta.hashkey());
        meta.file_path = std::path::Path::new(self.parent.as_str())
            .join(hkey.as_str())
            .to_str()
            .unwrap()
            .to_string();
    }
}
pub async fn start_update_score(src: std::sync::Arc<Cache>, interval: u64) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval));
        loop {
            tick.tick().await;
            src.update_partition().await;
        }
    });
}
pub async fn start_drop_daemon(src: std::sync::Arc<Cache>, disk_max_used: u64) {
    tokio::spawn({
        let src = src.clone();
        async move {
            watch_directory_daemon(
                src.parent.clone(),
                disk_max_used,
                3,
                Box::new({
                    let src = src.clone();
                    move |size: u64| {
                        let src = src.clone();
                        Box::pin(async move {
                            src.start_full_drop().await;
                            if let Ok(new_size) = stat_disk_use_size(&src.parent).await {
                                log::info!("complete full drop, disk size {size} -> {new_size}");
                            }
                        })
                    }
                }),
            )
            .await;
        }
    });
}
struct VisitCount {
    total: std::sync::atomic::AtomicU64,
    latest: u64,
    key: Vec<u8>,
}

async fn watch_directory_daemon<
    F: std::future::Future<Output = ()> + Send,
    T: 'static + Send + Sync + Fn(u64) -> F,
>(
    parent: String,
    max_size: u64,
    interval: u64,
    cb: T,
) {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval));
    tick.tick().await;
    let mut need_notify = true;
    let cb = std::sync::Arc::new(cb);
    loop {
        tick.tick().await;
        match stat_disk_use_size(&parent).await {
            Ok(size) => {
                log::info!(
                    "check directory size {size} max_size {max_size} need_notify {need_notify}"
                );
                crate::metrics::disk_use.set(size as f64);
                if size >= max_size {
                    if need_notify {
                        (*cb)(size).await;
                        need_notify = false;
                    }
                } else {
                    if !need_notify {
                        need_notify = true;
                    }
                }
            }
            Err(err) => {
                log::error!("stat disk use size failed {err} target={parent}");
            }
        }
    }
}
fn stat_disk_use_size<'a>(
    target: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, String>> + Send + 'a>> {
    Box::pin(async move {
        let finfo = tokio::fs::metadata(target)
            .await
            .map_err(|err| format!("get metadata {target} error {err}"))?;
        if finfo.is_dir() {
            let mut entries = tokio::fs::read_dir(target)
                .await
                .map_err(|err| format!("read dir {target} failed {err}"))?;
            let mut fsize = 0;
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|err| format!("read dir {target} error {err}"))?
            {
                let ftype = entry
                    .file_type()
                    .await
                    .map_err(|err| format!("get file type error {err}"))?;
                if ftype.is_dir() {
                    fsize += stat_disk_use_size(entry.path().to_str().unwrap()).await?;
                } else if ftype.is_symlink() {
                    continue;
                } else if ftype.is_file() {
                    let blkt = entry
                        .metadata()
                        .await
                        .map_err(|err| format!("get file {target} metadata error {err}"))?
                        .blocks();
                    fsize += blkt * 512;
                }
            }
            return Ok(fsize);
        } else if finfo.is_symlink() {
            return Ok(0);
        } else if finfo.is_file() {
            return Ok(finfo.blocks() * 512);
        }
        Ok(0)
    })
}

async fn get_file(
    file_path: &str,
    expect_md5: &str,
    fsize: Option<usize>,
) -> Result<Option<Vec<u8>>, String> {
    let fsize = match fsize {
        Some(fsize) => fsize,
        None => {
            let meta = tokio::fs::metadata(file_path).await.map_err(|err| {
                crate::metrics::file_error.inc();
                format!("get file {file_path} metadata error {err}")
            })?;
            meta.size() as usize
        }
    };
    let mut fd = tokio::fs::OpenOptions::new()
        .read(true)
        .open(file_path)
        .await
        .map_err(|err| format!("open file {file_path} error {err}"))?;
    let mut buff = vec![0u8; fsize];
    fd.read_exact(&mut buff).await.map_err(|err| {
        crate::metrics::file_error.inc();
        format!("read file error {err}")
    })?;
    let mut hsh = md5::Md5::new();
    let _ = hsh.write_all(&buff);
    let hsh: [u8; 16] = hsh.finalize().into();
    let hsh = hex::encode(hsh);
    if hsh != expect_md5 {
        crate::metrics::cache_hash_error.inc();
        return Ok(None);
    }
    Ok(Some(buff))
}

#[cfg(feature = "_rocksdb")]
pub fn rocksdb_drop_strategy(
    min_score: Option<u64>,
    flag: std::sync::Arc<std::sync::atomic::AtomicU8>,
) -> impl Fn(&[u8], &[u8]) -> Option<Option<Vec<u8>>> {
    let min_score = min_score.unwrap_or(50);
    move |_, val| {
        match bincode::deserialize::<crate::ObjectMeta>(val) {
            Ok(meta) => {
                if flag.load(std::sync::atomic::Ordering::Acquire) & super::CACHE_F_GC > 0
                    && meta.flag & crate::database::F_L0 > 0
                {
                    return None;
                }
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("get systime error")
                    .as_secs();

                if (meta.flag & crate::database::F_PIN == 0)
                    && (meta.expire_at <= now as i64 || meta.score <= min_score)
                {
                    return None;
                }
            }
            Err(err) => {
                log::error!("decode error {err}");
            }
        }
        Some(None)
    }
}
