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

#[cfg(feature = "link")]
use std::env;
#[cfg(feature = "link")]
use std::path::PathBuf;

// A pure `dlopen` build uses committed, pre-generated bindings
// (src/generated/ffi_dlopen_bindings.rs), so build.rs has nothing to do. This
// also covers the no-feature case (lib.rs emits a compile_error! there).
#[cfg(not(feature = "link"))]
fn main() {}

#[cfg(feature = "link")]
fn main() {
    // CMake sets this to the directory containing libmooncake_store.so. A
    // standalone Cargo build can override it or use the conventional sibling
    // top-level build directory.
    let lib_dir = env::var("MOONCAKE_STORE_LIB_DIR")
        .unwrap_or_else(|_| "../../build/mooncake-store/src".to_string());
    println!("cargo:rustc-link-search=native={lib_dir}");
    println!("cargo:rustc-link-lib=dylib=mooncake_store");
    println!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}");

    // Keep the existing explicit sanitizer opt-in. Toolchain/runtime libraries
    // are intentionally the only link items permitted alongside the Store DSO.
    let link_asan = env::var("MOONCAKE_LINK_ASAN")
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "" | "0" | "false" | "off" | "no"
            )
        })
        .unwrap_or(false);
    if link_asan {
        println!("cargo:rustc-link-lib=asan");
    }

    let include_dir =
        env::var("MOONCAKE_STORE_INCLUDE_DIR").unwrap_or_else(|_| "../include".to_string());
    let header = format!("{include_dir}/store_c.h");

    println!("cargo:rerun-if-changed={header}");
    println!("cargo:rerun-if-env-changed=MOONCAKE_STORE_LIB_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_STORE_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_LINK_ASAN");

    let bindings = bindgen::Builder::default()
        .header(&header)
        .allowlist_function("mooncake_store_.*")
        .allowlist_type("mooncake_.*")
        .generate()
        .expect("Unable to generate Mooncake Store bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").expect("missing OUT_DIR"));
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write Mooncake Store bindings");
}
