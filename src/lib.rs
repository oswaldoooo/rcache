mod binding;
pub mod cache;
pub mod database;
pub mod metrics;
use std::io::Write;

pub use database::ObjectMeta;
use md5::Digest;
#[macro_export]
macro_rules! system_file_remove {
    ($sys_file:ident) => {{
        let $sys_file = std::sync::Arc::new($sys_file.clone());
        move |meta| {
            let $sys_file = $sys_file.clone();
            async move {
                $sys_file
                    .remove_cb(meta)
                    .await
                    .unwrap_or_else(|err| log::error!("remove file error {err}"));
            }
        }
    }};
}
struct Defer<E: FnOnce()> {
    cb: Option<E>,
}
fn defer<E: FnOnce()>(src: E) -> Defer<E> {
    Defer { cb: Some(src) }
}
impl<E: FnOnce()> Drop for Defer<E> {
    fn drop(&mut self) {
        let cb = self.cb.take();
        if let Some(cb) = cb {
            cb();
        }
    }
}

pub fn build_hashkey<'a, T: Into<&'a str>>(src: T) -> Vec<u8> {
    let mut hsh = md5::Md5::new();
    let src: &'a str = src.into();
    let _ = hsh.write_all(src.as_bytes());
    let buff: [u8; 16] = hsh.finalize().into();
    buff.to_vec()
}
