// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::path::PathBuf;

fn push_existing_dir(search_dirs: &mut Vec<PathBuf>, dir: PathBuf) {
    if dir.is_dir() && !search_dirs.iter().any(|existing| existing == &dir) {
        search_dirs.push(dir);
    }
}

fn push_env_paths(search_dirs: &mut Vec<PathBuf>, name: &str) {
    if let Some(value) = env::var_os(name) {
        for dir in env::split_paths(&value) {
            push_existing_dir(search_dirs, dir);
        }
    }
}

fn has_library(search_dirs: &[PathBuf], candidates: &[&str]) -> bool {
    search_dirs.iter().any(|dir| {
        candidates.iter().any(|candidate| {
            ["a", "so", "dylib"]
                .into_iter()
                .map(|ext| dir.join(format!("lib{candidate}.{ext}")))
                .any(|path| path.exists())
        })
    })
}

fn emit_link_searches(search_dirs: &[PathBuf]) {
    for dir in search_dirs {
        println!("cargo:rustc-link-search=native={}", dir.display());
    }
}

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("missing CARGO_MANIFEST_DIR"));
    let mut search_dirs = Vec::new();

    let explicit_lib_dir = env::var("MOONCAKE_STORE_LIB_DIR").ok().map(PathBuf::from);
    if let Some(dir) = explicit_lib_dir.clone() {
        push_existing_dir(&mut search_dirs, dir);
    }

    if let Ok(build_dir) = env::var("MOONCAKE_BUILD_DIR") {
        let build_dir = PathBuf::from(build_dir);
        for dir in [
            build_dir.join("mooncake-store/src"),
            build_dir.join("mooncake-store/src/cachelib_memory_allocator"),
            build_dir.join("mooncake-transfer-engine/src"),
            build_dir.join("mooncake-transfer-engine/src/common/base"),
            build_dir.join("mooncake-asio"),
            build_dir.join("mooncake-common/etcd"),
        ] {
            push_existing_dir(&mut search_dirs, dir);
        }
    }

    let default_build_dir = manifest_dir.join("../../build");
    for dir in [
        default_build_dir.join("mooncake-store/src"),
        default_build_dir.join("mooncake-store/src/cachelib_memory_allocator"),
        default_build_dir.join("mooncake-transfer-engine/src"),
        default_build_dir.join("mooncake-transfer-engine/src/common/base"),
        default_build_dir.join("mooncake-asio"),
        default_build_dir.join("mooncake-common/etcd"),
        PathBuf::from("/usr/local/lib"),
        PathBuf::from("/usr/lib/x86_64-linux-gnu"),
        PathBuf::from("/lib/x86_64-linux-gnu"),
    ] {
        push_existing_dir(&mut search_dirs, dir);
    }

    push_env_paths(&mut search_dirs, "LD_LIBRARY_PATH");
    push_env_paths(&mut search_dirs, "LIBRARY_PATH");

    emit_link_searches(&search_dirs);

    for library in [
        "mooncake_store",
        "cachelib_memory_allocator",
        "transfer_engine",
        "base",
        "asio",
        "stdc++",
        "glog",
        "gflags",
        "pthread",
        "xxhash",
        "numa",
        "ibverbs",
        "jsoncpp",
        "zstd",
        "m",
    ] {
        println!("cargo:rustc-link-lib={library}");
    }

    for (link_name, candidates) in [
        ("etcd_wrapper", &["etcd_wrapper"] as &[&str]),
        ("curl", &["curl"]),
        ("uring", &["uring"]),
        ("asan", &["asan"]),
        ("gcov", &["gcov"]),
    ] {
        if has_library(&search_dirs, candidates) {
            println!("cargo:rustc-link-lib={link_name}");
        }
    }

    let include_dir = env::var("MOONCAKE_STORE_INCLUDE_DIR")
        .unwrap_or_else(|_| "../include".to_string());

    let header = format!("{include_dir}/store_c.h");

    println!("cargo:rerun-if-changed={header}");
    println!("cargo:rerun-if-env-changed=MOONCAKE_BUILD_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_STORE_LIB_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_STORE_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=LD_LIBRARY_PATH");
    println!("cargo:rerun-if-env-changed=LIBRARY_PATH");

    let bindings = bindgen::Builder::default()
        .header(&header)
        .allowlist_function("mooncake_store_.*")
        .allowlist_type("mooncake_.*")
        .generate()
        .expect("Unable to generate Mooncake Store bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write Mooncake Store bindings");
}
