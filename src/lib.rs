pub mod cache;
pub mod database;
pub mod metrics;
use std::io::Write;

pub use database::ObjectMeta;
use md5::Digest;
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
