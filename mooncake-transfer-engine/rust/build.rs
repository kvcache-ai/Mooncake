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
use std::path::{Path, PathBuf};
use std::process::Command;

fn flag_on(name: &str) -> bool {
    env::var(name)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("on") || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn push_dir(dirs: &mut Vec<PathBuf>, dir: PathBuf) {
    if dir.is_dir() && !dirs.iter().any(|existing| existing == &dir) {
        dirs.push(dir);
    }
}

fn emit_link_searches(dirs: &[PathBuf]) {
    for dir in dirs {
        println!("cargo:rustc-link-search=native={}", dir.display());
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dir.display());
    }
}

fn has_library(search_dirs: &[PathBuf], name: &str) -> bool {
    let file_names = [
        format!("lib{name}.so"),
        format!("lib{name}.a"),
        format!("lib{name}.dylib"),
    ];
    search_dirs
        .iter()
        .any(|dir| file_names.iter().any(|file| dir.join(file).exists()))
        || ["/usr/lib/x86_64-linux-gnu", "/usr/lib64", "/usr/local/lib"]
            .into_iter()
            .any(|dir| {
                file_names
                    .iter()
                    .any(|file| Path::new(dir).join(file).exists())
            })
}

fn compiler_print_file_name(file_name: &str) -> Option<PathBuf> {
    for tool in ["cc", "gcc", "clang", "c++"] {
        let output = match Command::new(tool)
            .arg(format!("-print-file-name={file_name}"))
            .output()
        {
            Ok(output) if output.status.success() => output,
            _ => continue,
        };
        let Ok(path) = String::from_utf8(output.stdout) else {
            continue;
        };
        let path = PathBuf::from(path.trim());
        if path.as_os_str().is_empty() || path == PathBuf::from(file_name) || !path.exists() {
            continue;
        }
        return Some(path);
    }
    None
}

fn main() {
    println!("cargo:rerun-if-env-changed=MOONCAKE_BUILD_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_TE_LIB_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_TE_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_WITH_ETCD");
    println!("cargo:rerun-if-env-changed=MOONCAKE_WITH_CUDA");
    println!("cargo:rerun-if-env-changed=MOONCAKE_WITHOUT_LIBFABRIC");
    println!("cargo:rerun-if-env-changed=MOONCAKE_LINK_ASAN");
    println!("cargo:rerun-if-env-changed=CUDA_HOME");
    println!("cargo:rerun-if-env-changed=CUDA_PATH");
    println!("cargo:rerun-if-env-changed=CUDART_LIB_DIR");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let mut search_dirs = Vec::new();

    if let Ok(dir) = env::var("MOONCAKE_TE_LIB_DIR") {
        push_dir(&mut search_dirs, PathBuf::from(dir));
    }

    if let Ok(build_dir) = env::var("MOONCAKE_BUILD_DIR") {
        let build_dir = PathBuf::from(build_dir);
        for dir in [
            build_dir.join("mooncake-transfer-engine/src"),
            build_dir.join("mooncake-transfer-engine/src/common/base"),
            build_dir.join("mooncake-asio"),
            build_dir.join("mooncake-common"),
            build_dir.join("mooncake-common/src"),
            build_dir.join("mooncake-common/etcd"),
            build_dir.join("src"),
            build_dir.join("src/common/base"),
        ] {
            push_dir(&mut search_dirs, dir);
        }
    }

    // Standalone cargo from mooncake-transfer-engine/rust, after a top-level
    // or in-tree CMake build.
    for dir in [
        manifest_dir.join("../build/src"),
        manifest_dir.join("../build/src/common/base"),
        manifest_dir.join("../build/mooncake-asio"),
        manifest_dir.join("../../build/mooncake-transfer-engine/src"),
        manifest_dir.join("../../build/mooncake-transfer-engine/src/common/base"),
        manifest_dir.join("../../build/mooncake-asio"),
        manifest_dir.join("../../build/mooncake-common"),
        manifest_dir.join("../../build/mooncake-common/etcd"),
    ] {
        push_dir(&mut search_dirs, dir);
    }

    if Path::new("/opt/amazon/efa/lib").exists() {
        push_dir(&mut search_dirs, PathBuf::from("/opt/amazon/efa/lib"));
    }

    if let Ok(dir) = env::var("CUDART_LIB_DIR") {
        push_dir(&mut search_dirs, PathBuf::from(dir));
    }
    let cuda_home = env::var("CUDA_HOME")
        .or_else(|_| env::var("CUDA_PATH"))
        .unwrap_or_else(|_| "/usr/local/cuda".to_string());
    for dir in [
        PathBuf::from(&cuda_home).join("lib64/stubs"),
        PathBuf::from(&cuda_home).join("lib64"),
        PathBuf::from(&cuda_home).join("lib/stubs"),
        PathBuf::from(&cuda_home).join("lib"),
    ] {
        push_dir(&mut search_dirs, dir);
    }

    emit_link_searches(&search_dirs);

    // Only link the AddressSanitizer runtime when explicitly requested.
    // Sanitized CI libraries need libasan first; Release builds must not
    // pull it in (see mooncake-store/rust/build.rs).
    if flag_on("MOONCAKE_LINK_ASAN") {
        if let Some(path) = compiler_print_file_name("libasan.so") {
            if let Some(parent) = path.parent() {
                println!("cargo:rustc-link-search=native={}", parent.display());
            }
        }
        println!("cargo:rustc-link-lib=asan");
    }

    println!("cargo:rustc-link-lib=static=transfer_engine");
    println!("cargo:rustc-link-lib=static=base");
    println!("cargo:rustc-link-lib=asio");
    println!("cargo:rustc-link-lib=stdc++");
    println!("cargo:rustc-link-lib=ibverbs");
    println!("cargo:rustc-link-lib=glog");
    println!("cargo:rustc-link-lib=gflags");
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=jsoncpp");
    println!("cargo:rustc-link-lib=numa");
    println!("cargo:rustc-link-lib=curl");

    if flag_on("MOONCAKE_WITHOUT_LIBFABRIC") {
        // skip
    } else if flag_on("MOONCAKE_WITH_LIBFABRIC")
        || search_dirs.iter().any(|dir| {
            ["so", "a", "dylib"]
                .into_iter()
                .any(|ext| dir.join(format!("libfabric.{ext}")).exists())
        })
        || [
            "/usr/lib/x86_64-linux-gnu/libfabric.so",
            "/usr/lib64/libfabric.so",
            "/usr/local/lib/libfabric.so",
            "/opt/amazon/efa/lib/libfabric.so",
        ]
        .into_iter()
        .any(|path| Path::new(path).exists())
    {
        println!("cargo:rustc-link-lib=fabric");
    }

    println!("cargo:rerun-if-env-changed=MOONCAKE_WITH_LIBFABRIC");

    if has_library(&search_dirs, "mooncake_common") {
        println!("cargo:rustc-link-lib=static=mooncake_common");
    }
    if has_library(&search_dirs, "yaml-cpp") {
        println!("cargo:rustc-link-lib=yaml-cpp");
    }

    // Default CMake USE_ETCD=ON links etcd_wrapper, not etcd-cpp-api.
    if flag_on("MOONCAKE_WITH_ETCD")
        || has_library(&search_dirs, "etcd_wrapper")
        || has_library(&search_dirs, "etcd-cpp-api")
    {
        if has_library(&search_dirs, "etcd_wrapper") {
            println!("cargo:rustc-link-lib=etcd_wrapper");
        } else if has_library(&search_dirs, "etcd-cpp-api") {
            println!("cargo:rustc-link-lib=etcd-cpp-api");
            println!("cargo:rustc-link-lib=cpprest");
            println!("cargo:rustc-link-lib=ssl");
            println!("cargo:rustc-link-lib=crypto");
        }
    }

    if flag_on("MOONCAKE_WITH_CUDA")
        || has_library(&search_dirs, "cudart")
        || has_library(&search_dirs, "cuda")
    {
        println!("cargo:rustc-link-lib=cudart");
        if has_library(&search_dirs, "cuda") {
            println!("cargo:rustc-link-lib=cuda");
        }
        println!("cargo:rustc-link-lib=rt");
    }

    for name in ["hiredis", "mlx5"] {
        if has_library(&search_dirs, name) {
            println!("cargo:rustc-link-lib={name}");
        }
    }

    let include_dir = env::var("MOONCAKE_TE_INCLUDE_DIR").unwrap_or_else(|_| {
        manifest_dir
            .join("../include")
            .to_string_lossy()
            .into_owned()
    });
    let header = format!("{include_dir}/transfer_engine_c.h");
    println!("cargo:rerun-if-changed={header}");

    let bindings = bindgen::Builder::default()
        .header(&header)
        .generate()
        .expect("Unable to generate Transfer Engine bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings");
}
