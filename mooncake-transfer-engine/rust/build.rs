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

fn resolve_existing_path(candidates: &[PathBuf], kind: &str) -> PathBuf {
    for candidate in candidates {
        if candidate.exists() {
            return candidate.clone();
        }
    }

    panic!("Unable to locate {}. Checked: {:?}", kind, candidates);
}

fn main() {
    println!("cargo:rerun-if-env-changed=MOONCAKE_TE_LIB_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_TE_HEADER");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let transfer_engine_lib_dir = if let Ok(path) = env::var("MOONCAKE_TE_LIB_DIR") {
        PathBuf::from(path)
    } else {
        resolve_existing_path(
            &[
                manifest_dir.join("../build/src"),
                manifest_dir.join("../../build/mooncake-transfer-engine/src"),
            ],
            "Transfer Engine library directory",
        )
    };

    let transfer_engine_header = if let Ok(path) = env::var("MOONCAKE_TE_HEADER") {
        PathBuf::from(path)
    } else {
        resolve_existing_path(
            &[manifest_dir.join("../include/transfer_engine_c.h")],
            "Transfer Engine C header",
        )
    };

    println!(
        "cargo:rustc-link-search=native={}",
        transfer_engine_lib_dir.display()
    );
    println!("cargo:rustc-link-lib=static=transfer_engine");

    println!("cargo:rustc-link-lib=stdc++");
    println!("cargo:rustc-link-lib=ibverbs");
    println!("cargo:rustc-link-lib=glog");
    println!("cargo:rustc-link-lib=gflags");
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=jsoncpp");
    println!("cargo:rustc-link-lib=numa");
    println!("cargo:rustc-link-lib=etcd-cpp-api");

    println!("cargo:rerun-if-changed={}", transfer_engine_header.display());

    let bindings = bindgen::builder()
        .header(transfer_engine_header.to_string_lossy())
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
