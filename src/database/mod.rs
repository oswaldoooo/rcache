use std::io::Write;

use md5::Digest;
#[cfg(feature = "_rocksdb")]
pub mod rocksdb;
#[cfg(feature = "_sled")]
pub mod sled;
///固定住数据，不参与淘汰机制
pub const F_PIN: u8 = 0b0001;
///不重要数据，淘汰机制触发时直接淘汰
pub const F_L0: u8 = 0b0010;
#[derive(serde::Deserialize, serde::Serialize, Clone)]
pub struct ObjectMeta {
    ///写入时间
    pub create_time: i64,
    ///过期时间
    pub expire_at: i64,
    ///计算hashkey的字符串
    pub to_hash_str: String,
    ///文件储存路径(绝对路径)
    pub file_path: String,
    ///内容hash
    pub content_md5: String,
    ///内容大小
    pub content_size: usize,
    ///访问频率反应
    pub score: u64,
    pub metadata: std::collections::BTreeMap<String, String>,
    pub flag: u8,
}
impl ObjectMeta {
    pub fn build<T:ToString>(ttl: u64, to_hash_str: T, content: &[u8]) -> Self {
        let to_hash_str=to_hash_str.to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let mut hsh = md5::Md5::new();
        let _ = hsh.write_all(content);
        let buff: [u8; 16] = hsh.finalize().into();
        Self {
            create_time: now,
            expire_at: now + ttl as i64,
            to_hash_str: to_hash_str,
            file_path: "".to_string(),
            content_md5: hex::encode(buff),
            content_size: content.len(),
            score: 0,
            flag: 0,
            metadata:Default::default(),
        }
    }
    pub fn hashkey(&self) -> Vec<u8> {
        let mut hsh = md5::Md5::new();
        let _ = hsh.write_all(self.to_hash_str.as_bytes());
        let buff: [u8; 16] = hsh.finalize().into();
        buff.to_vec()
    }
}
#[async_trait::async_trait]
pub trait Database: Send + Sync {
    async fn get(&self, hashkey: &[u8]) -> Result<Option<ObjectMeta>, String>;
    async fn put(&self, meta: ObjectMeta) -> Result<(), String>;
    async fn remove(&self, hashkey: &[u8]) -> Result<(), String>;
    async fn all<'a>(
        &self,
        cb: Box<dyn Send + FnMut(ObjectMeta) -> bool + 'a>,
    ) -> Result<(), String>;
    async fn gc(&self);
}
