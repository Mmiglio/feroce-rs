fn main() {
    println!("cargo:rustc-link-lib=ibverbs")

    // bindgen::Builder::default()
    //   .header("/usr/include/infiniband/verbs.h")
    //   .generate()
    //   .expect("failed to generate bindings")
    //   .write_to_file("src/rdma/ffi.rs")
    //   .expect("failed to write bindings");
}
