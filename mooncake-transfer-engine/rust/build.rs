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

fn main() {
    println!("cargo:rustc-link-search=native=../build/src");
    println!("cargo:rustc-link-search=native=../../build/mooncake-transfer-engine/src");
    println!("cargo:rustc-link-lib=static=transfer_engine");

    // libbase.a holds mooncake::Status, which libtransfer_engine.a references.
    println!("cargo:rustc-link-search=native=../build/src/common/base");
    println!("cargo:rustc-link-search=native=../../build/mooncake-transfer-engine/src/common/base");
    println!("cargo:rustc-link-lib=static=base");

    // The transfer_engine build uses ASIO_SEPARATE_COMPILATION + ASIO_DYN_LINK,
    // so the asio symbols live in mooncake-asio/libasio.so.  Link it whenever
    // we can find it (standalone cmake build places it alongside src/).
    println!("cargo:rustc-link-search=native=../build/mooncake-asio");
    println!("cargo:rustc-link-search=native=../../build/mooncake-asio");
    println!("cargo:rustc-link-lib=asio");

    // EFA on AWS installs libfabric under /opt/amazon/efa/lib.
    if std::path::Path::new("/opt/amazon/efa/lib").exists() {
        println!("cargo:rustc-link-search=native=/opt/amazon/efa/lib");
    }

    println!("cargo:rustc-link-lib=stdc++");
    println!("cargo:rustc-link-lib=ibverbs");
    // libfabric (fi_*): only needed for EFA transport, but harmless when
    // the system has it installed; required on AWS EFA hosts.  Opt-out by
    // setting MOONCAKE_WITHOUT_LIBFABRIC=1 if building on a box without it.
    if env::var("MOONCAKE_WITHOUT_LIBFABRIC")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("on") || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        // skip
    } else {
        println!("cargo:rustc-link-lib=fabric");
    }
    println!("cargo:rustc-link-lib=glog");
    println!("cargo:rustc-link-lib=gflags");
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=jsoncpp");
    println!("cargo:rustc-link-lib=numa");
    println!("cargo:rustc-link-lib=curl");

    let flag_on = |name: &str| {
        env::var(name)
            .map(|v| v == "1" || v.eq_ignore_ascii_case("on") || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    };

    // etcd-cpp-api: only needed when transfer_engine was built with
    // USE_ETCD=ON.  Opt-in via MOONCAKE_WITH_ETCD=1 to keep non-etcd builds
    // (e.g. EFA-only on AWS) linkable.
    if flag_on("MOONCAKE_WITH_ETCD") {
        println!("cargo:rustc-link-lib=etcd-cpp-api");
    }

    // CUDA runtime: libtransfer_engine.a built with USE_CUDA=ON pulls in
    // cudaMemcpy/cudaStream* symbols.  The Rust demos themselves don't call
    // CUDA — this is purely a transitive archive dep.  Enable with
    // MOONCAKE_WITH_CUDA=1 and optional CUDA_HOME override for lib path.
    if flag_on("MOONCAKE_WITH_CUDA") {
        // Accept either a CUDA_HOME (append lib64/lib) or an explicit
        // CUDART_LIB_DIR that already points at the directory containing
        // libcudart.so.  This covers both /usr/local/cuda installs and
        // pip-wheel layouts like .../nvidia/cu13/lib.
        if let Ok(dir) = env::var("CUDART_LIB_DIR") {
            println!("cargo:rustc-link-search=native={}", dir);
        } else if let Ok(cuda_home) = env::var("CUDA_HOME") {
            let lib64 = PathBuf::from(&cuda_home).join("lib64");
            let lib = PathBuf::from(&cuda_home).join("lib");
            if lib64.exists() {
                println!("cargo:rustc-link-search=native={}", lib64.display());
            }
            if lib.exists() {
                println!("cargo:rustc-link-search=native={}", lib.display());
            }
        } else {
            println!("cargo:rustc-link-search=native=/usr/local/cuda/lib64");
        }
        println!("cargo:rustc-link-lib=cudart");
    }

    let bindings = bindgen::builder()
        .header("../include/transfer_engine_c.h")
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
