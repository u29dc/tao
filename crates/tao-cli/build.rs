use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::Path;

fn source_files(root: &Path, files: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(root).expect("read workspace source directory") {
        let path = entry.expect("read workspace source entry").path();
        if path.is_dir() {
            source_files(&path, files);
        } else if path
            .extension()
            .is_some_and(|ext| ext == "rs" || ext == "sql")
            || path.file_name().is_some_and(|name| name == "Cargo.toml")
        {
            files.push(path);
        }
    }
}

fn main() {
    let workspace = Path::new("../..");
    let mut files = vec![workspace.join("Cargo.toml"), workspace.join("Cargo.lock")];
    println!("cargo:rerun-if-changed=../../crates");
    source_files(&workspace.join("crates"), &mut files);
    files.sort();
    let mut hasher = DefaultHasher::new();
    for path in files {
        println!("cargo:rerun-if-changed={}", path.display());
        path.strip_prefix(workspace)
            .expect("workspace source path")
            .hash(&mut hasher);
        fs::read(path)
            .expect("read workspace source")
            .hash(&mut hasher);
    }
    println!("cargo:rustc-env=TAO_BUILD_ID={:016x}", hasher.finish());
}
