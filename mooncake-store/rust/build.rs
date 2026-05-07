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
use std::process::Command;

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

fn emit_runtime_rpaths(search_dirs: &[PathBuf]) {
    for dir in search_dirs {
        let dir_str = dir.display();
        // Use rustc-link-arg (not -tests) so that the rpath is also applied
        // to the lib-test binary produced by `cargo test` for #[cfg(test)]
        // modules inside src/lib.rs.
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir_str}");
    }
}

fn compiler_candidates() -> Vec<String> {
    let mut tools = Vec::new();

    for env_var in ["CC", "CXX"] {
        if let Ok(value) = env::var(env_var) {
            let tool = value.trim();
            if !tool.is_empty() && !tools.iter().any(|existing| existing == tool) {
                tools.push(tool.to_string());
            }
        }
    }

    for tool in ["gcc", "cc", "clang", "c++"] {
        if !tools.iter().any(|existing| existing == tool) {
            tools.push(tool.to_string());
        }
    }

    tools
}

fn compiler_runtime_library(file_name: &str) -> Option<PathBuf> {
    for tool in compiler_candidates() {
        let output = Command::new(&tool)
            .arg(format!("-print-file-name={file_name}"))
            .output()
            .ok()?;

        if !output.status.success() {
            continue;
        }

        let path = String::from_utf8(output.stdout).ok()?;
        let path = PathBuf::from(path.trim());
        if path.as_os_str().is_empty() || path == PathBuf::from(file_name) || !path.exists() {
            continue;
        }

        return Some(path);
    }

    None
}

fn add_compiler_runtime_search_dir(search_dirs: &mut Vec<PathBuf>, file_name: &str) -> bool {
    let Some(path) = compiler_runtime_library(file_name) else {
        return false;
    };

    if let Some(parent) = path.parent() {
        push_existing_dir(search_dirs, parent.to_path_buf());
        return true;
    }

    false
}

fn main() {
    // -----------------------------------------------------------------------
    // Library search path
    //
    // When built via CMake (WITH_STORE_RUST=ON) the CMakeLists.txt injects
    // MOONCAKE_STORE_LIB_DIR pointing at the directory that contains
    // libmooncake_store.a/.so.  When cargo is invoked standalone the caller
    // should set the variable manually or rely on the default convention of a
    // sibling `build/` directory produced by a top-level CMake configure.
    // -----------------------------------------------------------------------
    let lib_dir = env::var("MOONCAKE_STORE_LIB_DIR")
        .unwrap_or_else(|_| "../../build/mooncake-store/src".to_string());

    println!("cargo:rustc-link-search=native={lib_dir}");

    // mooncake_store depends on libasio.so (shared) built in mooncake-common.
    let lib_path = PathBuf::from(&lib_dir);
    let build_dir = lib_path.ancestors().nth(2).map(PathBuf::from).unwrap_or_else(|| {
        println!("cargo:warning=MOONCAKE_STORE_LIB_DIR='{lib_dir}' does not have enough parent directories; using current directory");
        PathBuf::from(".")
    });
    println!(
        "cargo:rustc-link-search=native={}",
        build_dir.join("mooncake-common").display()
    );

    // transfer_engine is built in a sibling directory.
    println!(
        "cargo:rustc-link-search=native={}",
        build_dir.join("mooncake-transfer-engine/src").display()
    );

    // common/base library (contains mooncake::Status etc.)
    println!(
        "cargo:rustc-link-search=native={}",
        build_dir.join("mooncake-transfer-engine/src/common/base").display()
    );

    // CUDA runtime libraries (needed by transfer_engine RDMA transport).
    let cuda_home = env::var("CUDA_HOME")
        .or_else(|_| env::var("CUDA_PATH"))
        .unwrap_or_else(|_| "/usr/local/cuda".to_string());
    println!("cargo:rustc-link-search=native={}/targets/x86_64-linux/lib", cuda_home);

    // cachelib_memory_allocator is a static library built alongside mooncake_store.
    println!(
        "cargo:rustc-link-search=native={}",
        build_dir.join("mooncake-store/src/cachelib_memory_allocator").display()
    );

    println!("cargo:rustc-link-lib=mooncake_store");

    // Dependencies of mooncake_store that must be satisfied at link time.
    // The list mirrors what mooncake-store/src/CMakeLists.txt links against.
    println!("cargo:rustc-link-lib=transfer_engine");
    println!("cargo:rustc-link-lib=base"); // mooncake::Status etc.
    println!("cargo:rustc-link-lib=asio"); // shared library built by mooncake-common
    println!("cargo:rustc-link-lib=jsoncpp"); // transfer_engine dependency
    println!("cargo:rustc-link-lib=cachelib_memory_allocator"); // static
    println!("cargo:rustc-link-lib=stdc++");
    println!("cargo:rustc-link-lib=glog");
    println!("cargo:rustc-link-lib=gflags");
    println!("cargo:rustc-link-lib=numa");   // NUMA binding
    println!("cargo:rustc-link-lib=curl");    // HTTP metadata plugin
    println!("cargo:rustc-link-lib=ibverbs"); // RDMA transport
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=xxhash");

    // -----------------------------------------------------------------------
    // Header path for bindgen
    // -----------------------------------------------------------------------
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
        default_build_dir.join("mooncake-common"),
        default_build_dir.join("mooncake-common/etcd"),
        PathBuf::from("/usr/local/lib"),
        PathBuf::from("/usr/lib/x86_64-linux-gnu"),
        PathBuf::from("/lib/x86_64-linux-gnu"),
    ] {
        push_existing_dir(&mut search_dirs, dir);
    }

    push_env_paths(&mut search_dirs, "LD_LIBRARY_PATH");
    push_env_paths(&mut search_dirs, "LIBRARY_PATH");

    let asan_runtime_so = compiler_runtime_library("libasan.so");
    if let Some(path) = asan_runtime_so.as_ref() {
        if let Some(parent) = path.parent() {
            push_existing_dir(&mut search_dirs, parent.to_path_buf());
        }
    }
    let has_asan_runtime = asan_runtime_so.is_some()
        || add_compiler_runtime_search_dir(&mut search_dirs, "libasan.a");
    let has_gcov_runtime = add_compiler_runtime_search_dir(&mut search_dirs, "libgcov.a")
        || add_compiler_runtime_search_dir(&mut search_dirs, "libgcov.so");

    emit_link_searches(&search_dirs);
    emit_runtime_rpaths(&search_dirs);

    if has_asan_runtime || has_library(&search_dirs, &["asan"]) {
        println!("cargo:rustc-link-lib=asan");
    }

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
        ("hiredis", &["hiredis"]),
        ("curl", &["curl"]),
        ("cuda", &["cuda"]),
        ("cudart", &["cudart"]),
        ("uring", &["uring"]),
    ] {
        if has_library(&search_dirs, candidates) {
            println!("cargo:rustc-link-lib={link_name}");
        }
    }

    if has_gcov_runtime || has_library(&search_dirs, &["gcov"]) {
        println!("cargo:rustc-link-lib=gcov");
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
    println!("cargo:rerun-if-env-changed=CC");
    println!("cargo:rerun-if-env-changed=CXX");

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
