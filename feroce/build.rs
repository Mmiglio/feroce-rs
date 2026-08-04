fn setup_ibverbs() {
    println!("cargo:rustc-link-lib=ibverbs");
    println!("cargo:rerun-if-changed=build.rs");

    let output = std::path::Path::new(&std::env::var("OUT_DIR").unwrap()).join("ffi_generated.rs");

    let bindings = bindgen::Builder::default()
        //.header("/usr/include/infiniband/verbs.h")
        .header_contents("wrapper.h", "#include <infiniband/verbs.h>")
        .allowlist_function("ibv_.*")
        .allowlist_function("_ibv_.*")
        .allowlist_type("ibv_.*")
        .allowlist_var("IBV_.*")
        .bitfield_enum("ibv_access_flags")
        .bitfield_enum("ibv_qp_attr_mask")
        .bitfield_enum("ibv_send_flags")
        .bitfield_enum("ibv_wc_flags")
        .default_enum_style(bindgen::EnumVariation::Rust {
            non_exhaustive: false,
        })
        .derive_default(true)
        .derive_debug(true)
        .derive_copy(true)
        .prepend_enum_name(false)
        .blocklist_type("ibv_gid")
        .blocklist_type("ibv_wc")
        .size_t_is_usize(true)
        .generate()
        .expect("failed to generate bindings");

    bindings
        .write_to_file(&output)
        .expect("failed to write bindings");

    // path for reading the bindings, gitignored
    let link = std::path::Path::new("src/rdma/ffi_generated.rs");
    let _ = std::fs::remove_file(link);
    let _ = std::os::unix::fs::symlink(&output, link);
}

#[cfg(feature = "gpu")]
fn link_cuda() {
    println!("cargo:rustc-link-lib=cuda");
}

fn main() {
    setup_ibverbs();

    #[cfg(feature = "gpu")]
    link_cuda();
}
