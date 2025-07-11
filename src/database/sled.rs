use super::ObjectMeta;
pub struct SledDatabase<
    F: std::future::Future<Output = ()> + Send + Sync,
    T: Send + Sync + Fn(ObjectMeta) -> F,
> {
    db: std::sync::Arc<sled::Db>,
    remove_cb: T,
    min_score:u64,
}
impl<F: std::future::Future<Output = ()> + Send + Sync, T: Send + Sync + Fn(ObjectMeta) -> F>
    SledDatabase<F, T>
{
    pub fn new(db: sled::Db, remove_cb: T,min_score:u64) -> Self {
        Self {
            db: std::sync::Arc::new(db),
            remove_cb,
            min_score,
        }
    }
}
#[async_trait::async_trait]
impl<F: std::future::Future<Output = ()> + Send + Sync, T: Send + Sync + Fn(ObjectMeta) -> F>
    super::Database for SledDatabase<F, T>
{
    async fn get(&self, hashkey: &[u8]) -> Result<Option<ObjectMeta>, String> {
        let db = self.db.clone();
        let hashkey = hashkey.to_vec();
        let ret = tokio::task::spawn_blocking(move || db.get(hashkey))
            .await
            .map_err(|err| format!("sled get panic {err}"))?
            .map_err(|err| format!("sled get error {err}"))?;
        match ret {
            Some(ret) => Ok(Some(
                bincode::deserialize(&ret).map_err(|err| format!("decode error {err}"))?,
            )),
            None => Ok(None),
        }
    }
    async fn put(&self, meta: ObjectMeta) -> Result<(), String> {
        let content = bincode::serialize(&meta).map_err(|err| format!("encode failed {err}"))?;
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.insert(meta.hashkey(), content))
            .await
            .map_err(|err| format!("insert sled kv panic {err}"))?
            .map_err(|err| format!("sled insert failed {err}"))?;
        Ok(())
    }
    async fn remove(&self, hashkey: &[u8]) -> Result<(), String> {
        let db = self.db.clone();
        let hashkey = hashkey.to_vec();
        let ret = tokio::task::spawn_blocking(move || db.remove(hashkey))
            .await
            .map_err(|err| format!("sled remove panic {err}"))?
            .map_err(|err| format!("sled remove error {err}"))?;
        if let Some(ret) = ret {
            let data: ObjectMeta =
                bincode::deserialize(&ret).map_err(|err| format!("decode error {err}"))?;
            (self.remove_cb)(data).await;
        }
        Ok(())
    }
    async fn all<'a>(
        &self,
        mut cb: Box<dyn Send + FnMut(ObjectMeta) -> bool + 'a>,
    ) -> Result<(), String> {
        let db = self.db.clone();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::channel::<()>(1);
        tokio::task::spawn_blocking(move || {
            let mut iter = db.iter();
            while let Some(iter) = iter.next() {
                match iter {
                    Ok((_, val)) => match bincode::deserialize::<ObjectMeta>(&val) {
                        Ok(data) => {
                            let rt = tokio::runtime::Handle::current();
                            let _ = tx.send(data);
                            if let None = rt.block_on(async { rx2.recv().await }) {
                                return;
                            }
                        }
                        Err(err) => {
                            log::error!("decode error {err}");
                        }
                    },
                    Err(err) => {
                        log::error!("all failed {err}");
                    }
                }
            }
        });

        while let Some(data) = rx.recv().await {
            if cb(data) {
                let _ = tx2.send(()).await;
            } else {
                return Ok(());
            }
        }
        Ok(())
    }
    async fn gc(&self){
        let mut todelete = Vec::new();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        if let Err(err) = self
            .all(Box::new(|meta| {
                if meta.flag & crate::database::F_PIN > 0 {
                    return true;
                } else if meta.flag & crate::database::F_L0 > 0
                    || meta.expire_at <= now
                    || meta.score <= self.min_score
                {
                    todelete.push(meta);
                    return true;
                }
                true
            }))
            .await
        {
            log::error!("database all failed {err}");
        } else {
            for meta in todelete {
                if let Err(err) = self.remove(&meta.hashkey()).await {
                    log::error!("remove in drop error {err}");
                }
            }
        }
    }
}
