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

//! Runtime `dlopen` bindings for the Mooncake Store C API (`store_c.h`).
//!
//! Enabled by the `dlopen` feature. Loads `libmooncake_store.so` at run time via
//! [`libloading`] instead of static linking, so the crate builds with no
//! Mooncake C++ toolchain, headers, or generated bindings present. Exposes the
//! same items as the bindgen backend, so [`crate::store`] is backend-agnostic.
//!
//! The library is resolved lazily on first [`crate::MooncakeStore`] creation
//! from `MOONCAKE_STORE_LIBRARY` (default `libmooncake_store.so`, via the OS
//! loader search path), or eagerly via [`load_library`].

#![allow(non_camel_case_types)]

use std::ffi::{c_void, OsStr, OsString};
use std::os::raw::c_int;
use std::path::Path;
use std::sync::OnceLock;

use libloading::Library;

use crate::error::StoreError;

/// Opaque handle to a Mooncake store, matching `typedef void *mooncake_store_t`.
pub type mooncake_store_t = *mut c_void;

/// Rust mirror of `mooncake_replicate_config_t`; must stay in lock-step with
/// `store_c.h`.
#[repr(C)]
pub struct mooncake_replicate_config_t {
    pub replica_num: usize,
    pub with_soft_pin: c_int,
    pub with_hard_pin: c_int,
    pub preferred_segments: *mut *const libc::c_char,
    pub preferred_segments_count: usize,
}

/// Environment variable naming the shared library to load.
const LIBRARY_ENV: &str = "MOONCAKE_STORE_LIBRARY";
/// Default library name, resolved via the OS loader search path.
const DEFAULT_LIBRARY: &str = "libmooncake_store.so";

/// Generates the symbol vtable, its loader, and the free-function shims from one
/// list. Each Rust name equals the C symbol name (used via `stringify!`).
macro_rules! mooncake_api {
    (
        $(
            fn $name:ident ( $( $arg:ident : $argty:ty ),* $(,)? ) $( -> $ret:ty )? ;
        )*
    ) => {
        /// Resolved function pointers. `_lib` owns the library and must outlive
        /// them, so it is listed first (dropped last).
        struct Api {
            _lib: Library,
            $( $name: unsafe extern "C" fn( $( $argty ),* ) $( -> $ret )?, )*
        }

        impl Api {
            /// # Safety
            /// The named library must export the `mooncake_store_*` C ABI with the
            /// signatures declared here (i.e. be a real `libmooncake_store`).
            unsafe fn load(path: &OsStr) -> Result<Self, StoreError> {
                let lib = Library::new(path)
                    .map_err(|e| StoreError::LibraryLoad(e.to_string()))?;
                $(
                    let $name: unsafe extern "C" fn( $( $argty ),* ) $( -> $ret )? = *lib
                        .get(concat!(stringify!($name), "\0").as_bytes())
                        .map_err(|e| StoreError::SymbolLoad {
                            symbol: stringify!($name),
                            detail: e.to_string(),
                        })?;
                )*
                Ok(Api { _lib: lib, $( $name, )* })
            }
        }

        $(
            /// # Safety
            /// Same contract as the C function in `store_c.h`.
            #[inline]
            #[allow(clippy::too_many_arguments)]
            pub unsafe fn $name( $( $arg: $argty ),* ) $( -> $ret )? {
                (api().$name)( $( $arg ),* )
            }
        )*
    };
}

mooncake_api! {
    fn mooncake_store_create() -> mooncake_store_t;
    fn mooncake_store_destroy(store: mooncake_store_t);
    fn mooncake_store_setup(
        store: mooncake_store_t,
        local_hostname: *const libc::c_char,
        metadata_server: *const libc::c_char,
        global_segment_size: u64,
        local_buffer_size: u64,
        protocol: *const libc::c_char,
        device_name: *const libc::c_char,
        master_server_addr: *const libc::c_char,
    ) -> c_int;
    fn mooncake_store_health_check(store: mooncake_store_t) -> c_int;
    fn mooncake_store_put(
        store: mooncake_store_t,
        key: *const libc::c_char,
        value: *const c_void,
        size: usize,
        config: *const mooncake_replicate_config_t,
    ) -> c_int;
    fn mooncake_store_put_from(
        store: mooncake_store_t,
        key: *const libc::c_char,
        buffer: *mut c_void,
        size: usize,
        config: *const mooncake_replicate_config_t,
    ) -> c_int;
    fn mooncake_store_batch_put_from(
        store: mooncake_store_t,
        keys: *mut *const libc::c_char,
        buffers: *mut *mut c_void,
        sizes: *const usize,
        count: usize,
        config: *const mooncake_replicate_config_t,
        results_out: *mut c_int,
    ) -> c_int;
    fn mooncake_store_get_into(
        store: mooncake_store_t,
        key: *const libc::c_char,
        buffer: *mut c_void,
        size: usize,
    ) -> i64;
    fn mooncake_store_batch_get_into(
        store: mooncake_store_t,
        keys: *mut *const libc::c_char,
        buffers: *mut *mut c_void,
        sizes: *const usize,
        count: usize,
        results_out: *mut i64,
    ) -> c_int;
    fn mooncake_store_is_exist(store: mooncake_store_t, key: *const libc::c_char) -> c_int;
    fn mooncake_store_batch_is_exist(
        store: mooncake_store_t,
        keys: *mut *const libc::c_char,
        count: usize,
        results_out: *mut c_int,
    ) -> c_int;
    fn mooncake_store_get_size(store: mooncake_store_t, key: *const libc::c_char) -> i64;
    fn mooncake_store_get_hostname(
        store: mooncake_store_t,
        buf_out: *mut libc::c_char,
        buf_len: usize,
    ) -> c_int;
    fn mooncake_store_remove(
        store: mooncake_store_t,
        key: *const libc::c_char,
        force: c_int,
    ) -> c_int;
    fn mooncake_store_remove_by_regex(
        store: mooncake_store_t,
        pattern: *const libc::c_char,
        force: c_int,
    ) -> i64;
    fn mooncake_store_remove_all(store: mooncake_store_t, force: c_int) -> i64;
    fn mooncake_store_register_buffer(
        store: mooncake_store_t,
        buffer: *mut c_void,
        size: usize,
    ) -> c_int;
    fn mooncake_store_unregister_buffer(store: mooncake_store_t, buffer: *mut c_void) -> c_int;
}

// Raw function pointers plus a `Library` (Send + Sync on supported platforms).
unsafe impl Send for Api {}
unsafe impl Sync for Api {}

/// Process-wide, load-once handle.
static API: OnceLock<Api> = OnceLock::new();

fn library_path() -> OsString {
    std::env::var_os(LIBRARY_ENV).unwrap_or_else(|| OsString::from(DEFAULT_LIBRARY))
}

/// Load `libmooncake_store.so` from an explicit `path`. Optional; otherwise the
/// library loads lazily on first store creation. Must be called before any
/// store exists, and fails if a library is already loaded.
pub fn load_library(path: impl AsRef<Path>) -> Result<(), StoreError> {
    let api = unsafe { Api::load(path.as_ref().as_os_str()) }?;
    API.set(api)
        .map_err(|_| StoreError::LibraryLoad("Mooncake library is already loaded".to_string()))
}

/// Load the library from the default path on first use. Called by
/// `MooncakeStore::new()` before any other C call.
pub fn ensure_loaded() -> Result<(), StoreError> {
    if API.get().is_some() {
        return Ok(());
    }
    let api = unsafe { Api::load(&library_path()) }?;
    // A concurrent thread may win the race; keeping either load is fine.
    let _ = API.set(api);
    Ok(())
}

/// Loaded vtable. `MooncakeStore::new()` calls [`ensure_loaded`] first, so this
/// never fires in practice.
#[inline]
fn api() -> &'static Api {
    API.get()
        .expect("Mooncake library not loaded; create a MooncakeStore first")
}
