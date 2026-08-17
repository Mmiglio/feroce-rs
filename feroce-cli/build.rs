fn main() {
    if std::env::var_os("CARGO_FEATURE_GPU").is_none() {
        return;
    }

    cc::Build::new()
        .cuda(true)
        .file("kernels/sum.cu")
        .compile("feroce_kernels");

    let cuda_path = std::env::var("CUDA_PATH").unwrap_or_else(|_| "/usr/local/cuda".to_string());
    println!("cargo:rustc-link-search=native={cuda_path}/lib64");
    println!("cargo:rustc-link-lib=cudart");
    println!("cargo:rerun-if-changed=kernels/sum.cu");
    println!("cargo:rerun-if-changed=build.rs");
}
