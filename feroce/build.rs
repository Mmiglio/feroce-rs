fn setup_ibverbs() {
    println!("cargo:rustc-link-lib=ibverbs");

    let header = "/usr/include/infiniband/verbs.h";
    let output = "src/rdma/ffi_generated.rs";

    // ensure that the header is present
    if !std::path::Path::new(header).exists() {
        panic!(
            "libibverbs-dev not found: {} missing. Install with e.g. apt install libibverbs-dev",
            header
        );
    }

    // generate rdma bindings only if they are not present
    if std::path::Path::new(output).exists() {
        println!("cargo:rerun-if-changed={}", header);
        println!("cargo:rerun-if-changed={}", output);
        return;
    }

    let bindings = bindgen::Builder::default()
        .header("/usr/include/infiniband/verbs.h")
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
        .write_to_file(output)
        .expect("failed to write bindings");
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
