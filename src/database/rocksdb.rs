pub struct RocksDb {
    db: *mut crate::binding::rocksdb_t,
}
#[repr(C)]
struct CompactFilter {
    cb: Box<dyn Fn(&[u8], &[u8]) -> Option<Option<Vec<u8>>>>,
    flag: std::sync::Arc<std::sync::atomic::AtomicU8>,
}
///返回1会丢弃kv
#[no_mangle]
unsafe extern "C" fn compact_filter_c(
    arg1: *mut ::std::os::raw::c_void, /*自定义参数 */
    _level: ::std::os::raw::c_int,     /*当前压缩等级 */
    key: *const ::std::os::raw::c_char,
    key_length: usize,
    existing_value: *const ::std::os::raw::c_char, /*当前value值 */
    value_length: usize,                           /*当前value长度 */
    new_value: *mut *mut ::std::os::raw::c_char,   /*如果需要改变value，传入新value 指针 */
    new_value_length: *mut usize,                  /*新value 长度 */
    value_changed: *mut ::std::os::raw::c_uchar,   /*是否改变了值 */
) -> u8 {
    let key = std::slice::from_raw_parts(key as *const u8, key_length);
    let val = std::slice::from_raw_parts(existing_value as *const u8, value_length);
    let cf = (arg1 as *mut CompactFilter).as_mut().unwrap();
    let ret = (cf.cb)(key, val);
    if let Some(ret) = ret {
        match ret {
            Some(ret) => {
                *new_value_length = ret.len();
                let ret = Box::into_raw(ret.into_boxed_slice());
                *new_value = ret as *mut u8 as *mut i8;
                *value_changed = 1;
            }
            None => {
                *value_changed = 0;
            }
        }
        0
    } else {
        1
    }
}
unsafe extern "C" fn compaction_filter_name(arg1: *mut ::std::os::raw::c_void) -> *const i8 {
    c"rocksdb_filter".as_ptr()
}
#[no_mangle]
unsafe extern "C" fn destroy_compact_filter(arg1: *mut ::std::os::raw::c_void) {
    let _ = Box::from_raw(arg1 as *mut CompactFilter);
}
impl RocksDb {
    pub fn open(
        target: &str,
        cb: Option<Box<dyn Fn(&[u8], &[u8]) -> Option<Option<Vec<u8>>>>>,
        flag: std::sync::Arc<std::sync::atomic::AtomicU8>,
    ) -> Self {
        use crate::binding::*;
        let db = unsafe {
            let opts = rocksdb_options_create();
            if opts.is_null() {
                panic!("rocksdb create options failed");
            }
            if let Some(cb) = cb {
                let cf = Box::new(CompactFilter { cb, flag });
                let compact_filter_opts = rocksdb_compactionfilter_create(
                    Box::into_raw(cf) as *mut std::ffi::c_void,
                    Some(destroy_compact_filter),
                    Some(compact_filter_c),
                    Some(compaction_filter_name),
                );
                rocksdb_options_set_compaction_filter(opts, compact_filter_opts);
            }
            rocksdb_options_set_create_if_missing(opts, 1);
            let mut err: *mut i8 = std::ptr::null_mut();
            let targetc = std::ffi::CString::new(target).unwrap();
            let db = rocksdb_open(
                opts,
                targetc.as_c_str().as_ptr(),
                (&mut err) as *mut *mut i8,
            );
            if !err.is_null() {
                let error = std::ffi::CStr::from_ptr(err).to_str().unwrap().to_string();
                libc::free(err as *mut std::ffi::c_void);
                panic!("open rocksdb error {error}");
            }
            if db.is_null() {
                panic!("rocksdb open {target} failed");
            }
            db
        };

        Self { db: db }
    }
}
unsafe impl Send for RocksDb {}
unsafe impl Sync for RocksDb {}
use crate::{binding::rocksdb_iter_destroy, ObjectMeta};
impl RocksDb {
    pub fn get(&self, hashkey: &[u8]) -> Result<Option<Vec<u8>>, String> {
        use crate::binding::*;
        let mut valsize = 0 as usize;
        let mut err: *mut std::ffi::c_char = std::ptr::null_mut();
        let val = unsafe {
            let opts = rocksdb_readoptions_create();
            rocksdb_get(
                self.db,
                opts,
                hashkey.as_ptr() as *const std::ffi::c_char,
                hashkey.len(),
                (&mut valsize) as *mut usize,
                (&mut err) as *mut *mut std::ffi::c_char,
            )
        };
        if !err.is_null() {
            let err2 = unsafe { std::ffi::CStr::from_ptr(err) };
            let ret = err2.to_str().unwrap().to_string();
            unsafe { libc::free(err as *mut std::ffi::c_void) };
            return Err(ret);
        }
        if val.is_null() {
            return Ok(None);
        }
        let ret = unsafe { std::slice::from_raw_parts(val as *const u8, valsize) };
        let data = ret.to_vec();
        unsafe { libc::free(val as *mut std::ffi::c_void) };
        Ok(Some(data))
    }
    pub fn put(&self, key: &[u8], val: &[u8]) -> Result<(), String> {
        use crate::binding::*;
        let mut err: *mut std::ffi::c_char = std::ptr::null_mut();
        unsafe {
            let opts = rocksdb_writeoptions_create();
            rocksdb_put(
                self.db,
                opts,
                key.as_ptr() as *const std::ffi::c_char,
                key.len(),
                val.as_ptr() as *const std::ffi::c_char,
                val.len(),
                (&mut err) as *mut *mut std::ffi::c_char,
            );
        }
        if !err.is_null() {
            let err2 = unsafe { std::ffi::CStr::from_ptr(err) };
            let ret = err2.to_str().unwrap().to_string();
            unsafe { libc::free(err as *mut std::ffi::c_void) };
            return Err(ret);
        }
        Ok(())
    }
    pub fn remove(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        use crate::binding::*;
        let ret = self.get(key)?;
        if let None = ret {
            return Ok(None);
        }
        let ret = ret.unwrap();
        let mut err: *mut std::ffi::c_char = std::ptr::null_mut();
        unsafe {
            let opts = rocksdb_writeoptions_create();
            rocksdb_delete(
                self.db,
                opts,
                key.as_ptr() as *const std::ffi::c_char,
                key.len(),
                (&mut err) as *mut *mut std::ffi::c_char,
            );
        }
        if !err.is_null() {
            let err2 = unsafe { std::ffi::CStr::from_ptr(err) };
            let ret = err2.to_str().unwrap().to_string();
            unsafe { libc::free(err as *mut std::ffi::c_void) };
            return Err(ret);
        }
        Ok(Some(ret))
    }
    pub fn iter(&self) -> RocksdbIterator {
        use crate::binding::*;
        let ret = RocksdbIterator {
            _parent: self,
            iter_ptr: unsafe { rocksdb_create_iterator(self.db, rocksdb_readoptions_create()) },
        };
        unsafe {
            rocksdb_iter_seek_to_first(ret.iter_ptr);
        }
        ret
    }
    pub fn compact(&self) {
        use crate::binding::*;
        unsafe { rocksdb_compact_range(self.db, std::ptr::null(), 0, std::ptr::null(), 0) };
    }
}
impl Drop for RocksDb {
    fn drop(&mut self) {
        use crate::binding::*;
        unsafe {
            rocksdb_close(self.db);
        }
    }
}
pub struct RocksdbIterator<'a> {
    _parent: &'a RocksDb,
    iter_ptr: *mut crate::binding::rocksdb_iterator_t,
}
impl<'a> Drop for RocksdbIterator<'a> {
    fn drop(&mut self) {
        unsafe { rocksdb_iter_destroy(self.iter_ptr) };
    }
}
unsafe impl<'a> Send for RocksdbIterator<'a> {}
unsafe impl<'a> Sync for RocksdbIterator<'a> {}
impl<'a> Iterator for RocksdbIterator<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        use crate::binding::*;
        let ret = unsafe {
            if rocksdb_iter_valid(self.iter_ptr) <= 0 {
                return None;
            }
            let mut keysize = 0 as usize;
            let key = rocksdb_iter_key(self.iter_ptr, (&mut keysize) as *mut usize);
            let key = if key.is_null() {
                vec![]
            } else {
                let ret = std::slice::from_raw_parts(key as *const u8, keysize).to_vec();
                ret
            };
            let mut valsize = 0 as usize;
            let val = rocksdb_iter_value(self.iter_ptr, (&mut valsize) as *mut usize);
            let val = if val.is_null() {
                vec![]
            } else {
                let ret = std::slice::from_raw_parts(val as *const u8, valsize).to_vec();
                ret
            };
            rocksdb_iter_next(self.iter_ptr);
            (key, val)
        };
        Some(ret)
    }
}

pub struct RocksdbDb<
    F: std::future::Future<Output = ()> + Send + Sync,
    T: Fn(ObjectMeta) -> F + Send + Sync,
> {
    db: std::sync::Arc<RocksDb>,
    remove_cb: T,
}
impl<F: std::future::Future<Output = ()> + Send + Sync, T: Fn(ObjectMeta) -> F + Send + Sync>
    RocksdbDb<F, T>
{
    pub fn new(db: RocksDb, remove_cb: T) -> Self {
        Self {
            db: std::sync::Arc::new(db),
            remove_cb: remove_cb,
        }
    }
}
#[async_trait::async_trait]
impl<F: std::future::Future<Output = ()> + Send + Sync, T: Fn(ObjectMeta) -> F + Send + Sync>
    super::Database for RocksdbDb<F, T>
{
    async fn get(&self, hashkey: &[u8]) -> Result<Option<ObjectMeta>, String> {
        let hashkey = hashkey.to_vec();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            Ok(match db.get(&hashkey)? {
                Some(data) => Some(
                    bincode::deserialize(&data)
                        .map_err(|err| format!("bincode decode error {err}"))?,
                ),
                None => None,
            })
        })
        .await
        .map_err(|err| format!("rocksdb get operate panic {err}"))?
    }
    async fn put(&self, meta: ObjectMeta) -> Result<(), String> {
        let hashkey = meta.hashkey();
        let content =
            bincode::serialize(&meta).map_err(|err| format!("bincode encode error {err}"))?;
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.put(&hashkey, &content))
            .await
            .map_err(|err| format!("rocksdb put operate panic {err}"))?
    }
    async fn remove(&self, hashkey: &[u8]) -> Result<(), String> {
        let hashkey = hashkey.to_vec();
        let db = self.db.clone();
        let ret = tokio::task::spawn_blocking(move || db.remove(&hashkey))
            .await
            .map_err(|err| format!("remove panic {err}"))??;
        if let Some(data) = ret {
            match bincode::deserialize::<ObjectMeta>(&data) {
                Ok(data) => {
                    (self.remove_cb)(data).await;
                }
                Err(err) => {
                    log::error!("decode error {err}");
                }
            }
        }
        Ok(())
    }
    async fn all<'a>(
        &self,
        mut cb: Box<dyn Send + FnMut(ObjectMeta) -> bool + 'a>,
    ) -> Result<(), String> {
        let db = self.db.clone();
        let (sx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ObjectMeta>();
        let (sx2, mut rx2) = tokio::sync::mpsc::channel::<()>(1);
        tokio::task::spawn_blocking(move || {
            while let Some((_, data)) = db.iter().next() {
                match bincode::deserialize::<ObjectMeta>(&data) {
                    Ok(meta) => {
                        sx.send(meta).unwrap();
                        if let None =
                            tokio::runtime::Handle::current().block_on(async { rx2.recv().await })
                        {
                            return;
                        }
                    }
                    Err(err) => {
                        log::error!("bincode decode error {err}");
                    }
                }
            }
        });
        while let Some(meta) = rx.recv().await {
            if !cb(meta) {
                return Ok(());
            }
            let _ = sx2.send(()).await;
        }
        Ok(())
    }
    async fn gc(&self) {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.compact();
        })
        .await
        .unwrap_or_else(|err| log::error!("rocksdb compact panic {err}"));
    }
}

#[cfg(test)]
mod rtest {
    #[test]
    fn base() -> Result<(), Box<dyn std::error::Error>> {
        let _ = std::fs::remove_dir_all(".test2");
        let db = super::RocksDb::open(
            ".test2",
            None,
            std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0)),
        );
        db.put(b"name", b"jackson")?;
        db.put(b"age", b"23")?;
        db.put(b"phone", b"911")?;
        let name = db.get(b"name")?;
        let country = db.get(b"country")?;
        assert!(
            name.is_some() && name.unwrap() == b"jackson",
            "name not match"
        );
        assert!(country.is_none(), "country not match");
        db.remove(b"name")?;
        let name = db.get(b"name")?;
        assert!(name.is_none(), "name not match");
        let mut hs = std::collections::HashMap::new();
        hs.insert(b"age".to_vec(), b"23".to_vec());
        hs.insert(b"phone".to_vec(), b"911".to_vec());
        db.iter().all(|(k, v)| {
            assert!(hs.contains_key(&k) && hs.get(&k).unwrap() == &v);
            hs.remove(&k);
            true
        });

        Ok(())
    }
    fn cf(key: &[u8], _val: &[u8]) -> Option<Option<Vec<u8>>> {
        if key.starts_with(b"delete_") {
            return None;
        }
        Some(None)
    }
    #[test]
    fn compact_filter() -> Result<(), Box<dyn std::error::Error>> {
        let _ = std::fs::remove_dir_all(".test3");
        let db = super::RocksDb::open(
            ".test3",
            Some(Box::new(cf)),
            std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0)),
        );
        db.put(b"delete_me", b"hello world!")?;
        db.put(b"pin_data", b"im pin data")?;
        db.compact();
        assert!(db.get(b"delete_me")?.is_none(), "expect nil, got not nil");
        let pin_data = db.get(b"pin_data")?;
        assert!(
            pin_data.is_some() && pin_data.unwrap() == b"im pin data",
            "pin_data not correct"
        );
        Ok(())
    }
}
