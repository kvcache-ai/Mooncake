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

use std::cmp::Ordering;
use std::env;
use std::fs;
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

fn has_static_library(search_dirs: &[PathBuf], name: &str) -> bool {
    let file_name = format!("lib{name}.a");
    search_dirs.iter().any(|dir| dir.join(&file_name).exists())
        || ["/usr/lib/x86_64-linux-gnu", "/usr/lib64", "/usr/local/lib"]
            .into_iter()
            .any(|dir| Path::new(dir).join(&file_name).exists())
}

fn is_tent_archive(lib: &str) -> bool {
    lib == "tent"
        || lib.starts_with("tent_")
        || lib.starts_with("metastore_")
        || lib.starts_with("platform_")
}

/// Collect TENT static archives (`libtent.a`, `libtent_xport_*.a`, …).
/// `USE_TENT=ON` compiles those into `libtransfer_engine.a` as undefined
/// `mooncake::tent::*` symbols; CMake wraps them in `--start-group`.
fn collect_tent_archives(root: &Path, libs: &mut Vec<(PathBuf, String)>) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if path.is_dir() {
                if matches!(name, "CMakeFiles" | "target" | ".git") {
                    continue;
                }
                stack.push(path);
                continue;
            }
            let Some(lib) = name.strip_prefix("lib").and_then(|n| n.strip_suffix(".a")) else {
                continue;
            };
            if !is_tent_archive(lib) || libs.iter().any(|(_, existing)| existing == lib) {
                continue;
            }
            if let Some(parent) = path.parent() {
                libs.push((parent.to_path_buf(), lib.to_string()));
            }
        }
    }
}

fn tent_archive_sort(a: &(PathBuf, String), b: &(PathBuf, String)) -> Ordering {
    match (a.1.as_str() == "tent", b.1.as_str() == "tent") {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => a.1.cmp(&b.1),
    }
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

fn emit_compiler_runtime_search(file_name: &str) -> bool {
    let Some(path) = compiler_print_file_name(file_name) else {
        return false;
    };
    let Some(parent) = path.parent() else {
        return false;
    };

    println!("cargo:rustc-link-search=native={}", parent.display());
    true
}

fn main() {
    println!("cargo:rerun-if-env-changed=MOONCAKE_BUILD_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_TE_LIB_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_TE_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_WITH_ETCD");
    println!("cargo:rerun-if-env-changed=MOONCAKE_WITH_CUDA");
    println!("cargo:rerun-if-env-changed=MOONCAKE_WITHOUT_LIBFABRIC");
    println!("cargo:rerun-if-env-changed=MOONCAKE_WITH_TENT");
    println!("cargo:rerun-if-env-changed=MOONCAKE_LINK_ASAN");
    println!("cargo:rerun-if-env-changed=CUDA_HOME");
    println!("cargo:rerun-if-env-changed=CUDA_PATH");
    println!("cargo:rerun-if-env-changed=CUDART_LIB_DIR");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let mut search_dirs = Vec::new();
    let mut tent_libs: Vec<(PathBuf, String)> = Vec::new();

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
            build_dir.join("mooncake-transfer-engine/tent/src"),
            // libtent_metrics.so is a SHARED lib built under tent/src/metrics.
            // collect_tent_archives skips it (only .a), and has_library's
            // /usr/local/lib fallback only gates the -l emit, not the -L search
            // path — so the linker still needs this directory explicitly.
            build_dir.join("mooncake-transfer-engine/tent/src/metrics"),
            build_dir.join("src"),
            build_dir.join("src/common/base"),
        ] {
            push_dir(&mut search_dirs, dir);
        }
        collect_tent_archives(
            &build_dir.join("mooncake-transfer-engine/tent"),
            &mut tent_libs,
        );
        collect_tent_archives(&build_dir.join("tent"), &mut tent_libs);
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
        manifest_dir.join("../../build/mooncake-transfer-engine/tent/src"),
        manifest_dir.join("../../build/mooncake-transfer-engine/tent/src/metrics"),
        manifest_dir.join("../tent/build/src"),
    ] {
        push_dir(&mut search_dirs, dir);
    }

    collect_tent_archives(
        &manifest_dir.join("../../build/mooncake-transfer-engine/tent"),
        &mut tent_libs,
    );
    collect_tent_archives(&manifest_dir.join("../build"), &mut tent_libs);

    tent_libs.sort_by(tent_archive_sort);
    for (dir, _) in &tent_libs {
        push_dir(&mut search_dirs, dir.clone());
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

    let has_mooncake_common = has_library(&search_dirs, "mooncake_common");
    let link_tent =
        flag_on("MOONCAKE_WITH_TENT") || tent_libs.iter().any(|(_, name)| name == "tent");
    if link_tent {
        // USE_TENT compiles mooncake::tent::* refs into libtransfer_engine.a.
        // Keep transfer_engine inside the same GNU ld rescan group as CMake's
        // tent_link_group so archive order cannot drop those symbols.
        println!("cargo:rustc-link-arg=-Wl,--start-group");
        println!("cargo:rustc-link-arg=-ltransfer_engine");
        println!("cargo:rustc-link-arg=-lbase");
        if tent_libs.is_empty() {
            println!("cargo:rustc-link-arg=-ltent");
        } else {
            for (_, name) in &tent_libs {
                println!("cargo:rustc-link-arg=-l{name}");
            }
        }
        if has_mooncake_common {
            println!("cargo:rustc-link-arg=-lmooncake_common");
        }
        if has_library(&search_dirs, "tent_metrics") {
            println!("cargo:rustc-link-arg=-ltent_metrics");
        }
        println!("cargo:rustc-link-arg=-Wl,--end-group");
        println!("cargo:rustc-link-lib=dl");
        if has_library(&search_dirs, "uring") {
            println!("cargo:rustc-link-lib=uring");
        }
    } else {
        println!("cargo:rustc-link-lib=static=transfer_engine");
        println!("cargo:rustc-link-lib=static=base");
        if has_mooncake_common {
            println!("cargo:rustc-link-lib=static=mooncake_common");
        }
    }

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

    // Coverage-instrumented C++ archives reference __gcov_* symbols. Cargo
    // links the Rust test binary directly, so add GCC's static gcov runtime
    // when it is available. Keep this last: libgcov.a must follow the
    // instrumented archives on the link line. Non-coverage builds have no
    // __gcov_* references, so the static archive contributes no objects.
    if emit_compiler_runtime_search("libgcov.a") || has_static_library(&search_dirs, "gcov") {
        if link_tent {
            println!("cargo:rustc-link-arg=-lgcov");
        } else {
            println!("cargo:rustc-link-lib=gcov");
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
