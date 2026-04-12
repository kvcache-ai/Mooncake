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
    println!("cargo:rustc-link-lib=mooncake_store");

    // Dependencies of mooncake_store that must be satisfied at link time.
    // The list mirrors what mooncake-store/src/CMakeLists.txt links against.
    println!("cargo:rustc-link-lib=transfer_engine");
    println!("cargo:rustc-link-lib=stdc++");
    println!("cargo:rustc-link-lib=glog");
    println!("cargo:rustc-link-lib=gflags");
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=xxhash");

    // -----------------------------------------------------------------------
    // Header path for bindgen
    // -----------------------------------------------------------------------
    let include_dir = env::var("MOONCAKE_STORE_INCLUDE_DIR")
        .unwrap_or_else(|_| "../include".to_string());

    let header = format!("{include_dir}/store_c.h");

    // Re-run this build script if the C header changes.
    println!("cargo:rerun-if-changed={header}");
    println!("cargo:rerun-if-env-changed=MOONCAKE_STORE_LIB_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_STORE_INCLUDE_DIR");

    let bindings = bindgen::Builder::default()
        .header(&header)
        // Only pull in declarations from store_c.h (no transitive system headers).
        .allowlist_function("mooncake_store_.*")
        .allowlist_type("mooncake_.*")
        .generate()
        .expect("Unable to generate Mooncake Store bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write Mooncake Store bindings");
}
