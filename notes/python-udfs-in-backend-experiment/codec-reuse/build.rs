// datafusion-python enables pyo3's `extension-module` feature, which stops pyo3 linking libpython;
// an embedding binary has to link it itself.
fn main() {
    let dir = std::env::var("PY_LIBDIR").expect("set PY_LIBDIR");
    let lib = std::env::var("PY_LIB").unwrap_or_else(|_| "python3.14".into());
    println!("cargo:rustc-link-search=native={dir}");
    println!("cargo:rustc-link-lib=dylib={lib}");
    println!("cargo:rerun-if-env-changed=PY_LIBDIR");
    println!("cargo:rerun-if-env-changed=PY_LIB");
}
