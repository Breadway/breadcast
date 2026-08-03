use std::path::{Path, PathBuf};

fn collect_cc_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| {
        panic!("failed to read {}: {e}", dir.display());
    }) {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            collect_cc_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "cc") {
            out.push(path);
        }
    }
}

fn main() {
    let vendor = Path::new("vendor/openscreen");
    let jsoncpp = pkg_config::probe_library("jsoncpp")
        .expect("jsoncpp not found (pacman: jsoncpp / apt: libjsoncpp-dev)");
    // Just libcrypto, not the full "openssl" .pc -- this vendored subset of
    // openscreen never touches libssl (no TLS; see vendor/openscreen/PATCHES.md).
    let libcrypto = pkg_config::probe_library("libcrypto").expect("libcrypto not found");

    let mut sources = Vec::new();
    collect_cc_files(vendor, &mut sources);
    sources.push(PathBuf::from("src/message_port_bridge.cc"));
    sources.push(PathBuf::from("src/session.cc"));
    sources.push(PathBuf::from("src/facade.cc"));

    let mut build = cc::Build::new();
    build
        .cpp(true)
        .std("c++20")
        .include(vendor)
        .include("src")
        // Force-included into every translation unit -- see
        // vendor/openscreen/patches/compat_shims.h for what this papers over
        // (a couple of missing includes and BoringSSL-only APIs system
        // OpenSSL doesn't expose).
        .flag(format!("-include{}", vendor.join("patches/compat_shims.h").display()))
        // This is a vendored, pruned third-party subset (see
        // vendor/openscreen/PATCHES.md) -- warnings in it aren't
        // breadcast's to fix, and upstream builds it with -w itself for the
        // same reason (see BoringSSL's "internal_config" in its own BUILD.gn).
        .warnings(false);

    for path in jsoncpp.include_paths.iter().chain(libcrypto.include_paths.iter()) {
        build.include(path);
    }

    for source in &sources {
        println!("cargo:rerun-if-changed={}", source.display());
        build.file(source);
    }
    println!("cargo:rerun-if-changed=src/facade.h");
    println!("cargo:rerun-if-changed=vendor/openscreen/PATCHES.md");

    build.compile("breadcast_caststream");
}
