use super::ObjectMeta;
pub struct SledDatabase {
    db: std::sync::Arc<sled::Db>,
}
impl SledDatabase {
    pub fn new(db: sled::Db) -> Self {
        Self {
            db: std::sync::Arc::new(db),
        }
    }
}
#[async_trait::async_trait]
impl super::Database for SledDatabase {
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
    async fn remove(&self, hashkey: &[u8]) -> Result<Option<ObjectMeta>, String> {
        let db = self.db.clone();
        let hashkey = hashkey.to_vec();
        let ret = tokio::task::spawn_blocking(move || db.remove(hashkey))
            .await
            .map_err(|err| format!("sled remove panic {err}"))?
            .map_err(|err| format!("sled remove error {err}"))?;
        ret.map_or(Ok(None), |v| -> Result<Option<ObjectMeta>, String> {
            Ok(Some(
                bincode::deserialize(&v).map_err(|err| format!("decode error {err}"))?,
            ))
        })
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
}
