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

// Library discovery mirrors mooncake-store/rust/build.rs so both crates resolve
// a Mooncake build tree the same way.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
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

fn push_cmake_prefix_paths(search_dirs: &mut Vec<PathBuf>, value: &str) {
    for prefix in value.split(';').filter(|prefix| !prefix.is_empty()) {
        let prefix = PathBuf::from(prefix);
        push_existing_dir(search_dirs, prefix.join("lib"));
        push_existing_dir(search_dirs, prefix.join("lib64"));
    }
}

fn push_cmake_cache_library_dirs(search_dirs: &mut Vec<PathBuf>, build_dir: &Path) {
    let cache_path = build_dir.join("CMakeCache.txt");
    let Ok(cache) = fs::read_to_string(cache_path) else {
        return;
    };

    for line in cache.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };

        if key == "CMAKE_PREFIX_PATH:PATH" {
            push_cmake_prefix_paths(search_dirs, value);
            continue;
        }

        if key.ends_with("_DIR:PATH") {
            let package_dir = PathBuf::from(value);
            if package_dir.parent().and_then(|dir| dir.file_name())
                == Some(std::ffi::OsStr::new("cmake"))
            {
                if let Some(lib_dir) = package_dir.ancestors().nth(2) {
                    push_existing_dir(search_dirs, lib_dir.to_path_buf());
                }
            }
            continue;
        }

        if key.ends_with("_LIBRARY:FILEPATH") || key.ends_with("_LIBRARIES:FILEPATH") {
            let library_path = PathBuf::from(value);
            if let Some(parent) = library_path.parent() {
                push_existing_dir(search_dirs, parent.to_path_buf());
            }
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

fn compiler_runtime_library(file_name: &str) -> Option<PathBuf> {
    let mut tools: Vec<String> = Vec::new();
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

    for tool in tools {
        let Ok(output) = Command::new(&tool)
            .arg(format!("-print-file-name={file_name}"))
            .output()
        else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        let Ok(path) = String::from_utf8(output.stdout) else {
            continue;
        };
        let path = PathBuf::from(path.trim());
        if path.as_os_str().is_empty() || path == Path::new(file_name) || !path.exists() {
            continue;
        }
        return Some(path);
    }
    None
}

fn add_compiler_runtime_search_dir(search_dirs: &mut Vec<PathBuf>, file_name: &str) -> bool {
    match compiler_runtime_library(file_name).and_then(|path| path.parent().map(PathBuf::from)) {
        Some(parent) => {
            push_existing_dir(search_dirs, parent);
            true
        }
        None => false,
    }
}

fn main() {
    // -----------------------------------------------------------------------
    // Library search path
    //
    // When built via CMake (WITH_CONDUCTOR_RUST=ON) the CMakeLists.txt injects
    // MOONCAKE_CONDUCTOR_LIB_DIR pointing at the directory that contains
    // libconductor_cpp_core.a. When cargo is invoked standalone the caller
    // should set the variable manually or rely on the default convention of a
    // sibling `build/` directory produced by a top-level CMake configure.
    // -----------------------------------------------------------------------
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("missing CARGO_MANIFEST_DIR"));
    let lib_dir = env::var_os("MOONCAKE_CONDUCTOR_LIB_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest_dir.join("../../build/mooncake-conductor"));
    // Emitted even if missing so a wrong path surfaces as a link error.
    println!("cargo:rustc-link-search=native={}", lib_dir.display());

    let build_dir = env::var_os("MOONCAKE_BUILD_DIR")
        .map(PathBuf::from)
        .or_else(|| lib_dir.parent().map(PathBuf::from));

    let mut search_dirs = Vec::new();
    push_existing_dir(&mut search_dirs, lib_dir.clone());
    if let Some(build_dir) = build_dir.as_ref() {
        push_cmake_cache_library_dirs(&mut search_dirs, build_dir);
        // libasio.so of the top-level build (mooncake-common/src/CMakeLists.txt).
        push_existing_dir(&mut search_dirs, build_dir.join("mooncake-common"));
    }
    // A standalone mooncake-conductor build keeps its cache next to the library.
    push_cmake_cache_library_dirs(&mut search_dirs, &lib_dir);
    for dir in [
        "/usr/local/lib",
        "/usr/lib/x86_64-linux-gnu",
        "/lib/x86_64-linux-gnu",
    ] {
        push_existing_dir(&mut search_dirs, PathBuf::from(dir));
    }
    if let Ok(cmake_prefix_path) = env::var("CMAKE_PREFIX_PATH") {
        push_cmake_prefix_paths(&mut search_dirs, &cmake_prefix_path);
    }
    push_env_paths(&mut search_dirs, "LD_LIBRARY_PATH");
    push_env_paths(&mut search_dirs, "LIBRARY_PATH");

    // Same policy as mooncake-store/rust/build.rs: link the ASan runtime only
    // when MOONCAKE_LINK_ASAN asks for it, never just because gcc ships it.
    let want_asan = env::var("MOONCAKE_LINK_ASAN")
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "" | "0" | "false" | "off" | "no"
            )
        })
        .unwrap_or(false);
    let has_asan_runtime = want_asan
        && (add_compiler_runtime_search_dir(&mut search_dirs, "libasan.so")
            || add_compiler_runtime_search_dir(&mut search_dirs, "libasan.a"));
    let has_gcov_runtime = add_compiler_runtime_search_dir(&mut search_dirs, "libgcov.a")
        || add_compiler_runtime_search_dir(&mut search_dirs, "libgcov.so");

    for dir in &search_dirs {
        println!("cargo:rustc-link-search=native={}", dir.display());
        // rustc-link-arg (not -tests) so the lib-test binary gets it as well.
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dir.display());
    }

    if want_asan && (has_asan_runtime || has_library(&search_dirs, &["asan"])) {
        println!("cargo:rustc-link-lib=asan");
    }

    // The static archive first, then what it references; mirrors
    // conductor_cpp_core's target_link_libraries in ../CMakeLists.txt
    // (msgpack-cxx and yalantinglibs are header-only).
    for library in [
        "conductor_cpp_core",
        "zmq",
        "crypto",
        "glog",
        "jsoncpp",
        "stdc++",
        "pthread",
        "m",
    ] {
        println!("cargo:rustc-link-lib={library}");
    }

    // gflags when glog was built against it; asio/ibverbs when the top-level
    // build compiles yalantinglibs against libasio.so (the asio_shared branch
    // in ../CMakeLists.txt). Unused shared libraries are dropped by --as-needed.
    for (link_name, candidates) in [
        ("gflags", &["gflags"] as &[&str]),
        ("asio", &["asio"]),
        ("ibverbs", &["ibverbs"]),
    ] {
        if has_library(&search_dirs, candidates) {
            println!("cargo:rustc-link-lib={link_name}");
        }
    }

    if has_gcov_runtime || has_library(&search_dirs, &["gcov"]) {
        println!("cargo:rustc-link-lib=gcov");
    }

    // -----------------------------------------------------------------------
    // Bindings
    // -----------------------------------------------------------------------
    let include_dir = env::var_os("MOONCAKE_CONDUCTOR_INCLUDE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest_dir.join("../include"));
    let header = include_dir.join("conductor/client/conductor_c.h");

    println!("cargo:rerun-if-changed={}", header.display());
    for var in [
        "MOONCAKE_BUILD_DIR",
        "MOONCAKE_CONDUCTOR_LIB_DIR",
        "MOONCAKE_CONDUCTOR_INCLUDE_DIR",
        "CMAKE_PREFIX_PATH",
        "LD_LIBRARY_PATH",
        "LIBRARY_PATH",
        "CC",
        "CXX",
        "MOONCAKE_LINK_ASAN",
    ] {
        println!("cargo:rerun-if-env-changed={var}");
    }

    let bindings = bindgen::Builder::default()
        .header(header.to_string_lossy())
        .allowlist_function("mooncake_conductor_.*")
        .allowlist_type("mooncake_conductor_.*")
        // The enums already carry a MOONCAKE_CONDUCTOR_ prefix.
        .prepend_enum_name(false)
        // Zeroed structs: the C ABI treats NULL/0 fields as "unset".
        .derive_default(true)
        .generate()
        .expect("Unable to generate Mooncake Conductor bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write Mooncake Conductor bindings");
}
