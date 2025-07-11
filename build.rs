fn main() {
    #[cfg(feature = "_rocksdb")]
    {
        println!("cargo:rustc-link-search=native=/usr/local/lib");
        println!("cargo:rustc-link-lib=static=rocksdb");
        println!("cargo:rustc-link-lib=dylib=c++");
        #[cfg(target_os = "macos")]
        {
            println!("cargo:rustc-link-lib=framework=Foundation");
            println!("cargo:rustc-link-lib=framework=CoreServices");
        }
        println!("cargo:rustc-link-lib=dylib=z");
        println!("cargo:rustc-link-lib=dylib=bz2");
    }
}
