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
//! The raw loader (types, layout checks, all `mooncake_store_*` symbols) is
//! bindgen-generated from `store_c.h` (`dynamic_library_name`, see build.rs), so
//! the ABI is never hand-maintained; this module wraps it in a process-global
//! plus free-function shims so `crate::store` is backend-agnostic. The library
//! loads lazily on first store creation from `MOONCAKE_STORE_LIBRARY` (default
//! `libmooncake_store.so`), or eagerly via [`load_library`].

use std::ffi::{OsStr, OsString};
use std::os::raw::{c_char, c_int, c_void};
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use crate::error::StoreError;

/// Bindgen-generated dynamic bindings (`MooncakeStoreLib` + the C types),
/// committed and regenerated via the `generate_dlopen_bindings` example.
mod sys {
    #![allow(non_camel_case_types, non_snake_case, non_upper_case_globals)]
    #![allow(dead_code)]
    #![allow(clippy::all)] // generated code
    include!("generated/ffi_dlopen_bindings.rs");
}

// Re-export the C types so `crate::store` can name them backend-agnostically.
pub use sys::{mooncake_replicate_config_t, mooncake_store_t};

/// Environment variable naming the shared library to load.
const LIBRARY_ENV: &str = "MOONCAKE_STORE_LIBRARY";
/// Default library name, resolved via the OS loader search path. Platform-aware
/// so a non-Linux consumer that built its own library still finds it by default
/// (the `WITH_STORE_C_SHARED` producer is Linux-only, but the loader is not).
#[cfg(target_os = "linux")]
const DEFAULT_LIBRARY: &str = "libmooncake_store.so";
#[cfg(target_os = "macos")]
const DEFAULT_LIBRARY: &str = "libmooncake_store.dylib";
#[cfg(target_os = "windows")]
const DEFAULT_LIBRARY: &str = "mooncake_store.dll";
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
const DEFAULT_LIBRARY: &str = "libmooncake_store.so";

/// Process-wide, load-once handle to the generated loader.
static API: OnceLock<sys::MooncakeStoreLib> = OnceLock::new();
/// Serializes initialization so a concurrent race dlopen()s at most once.
static INIT_LOCK: Mutex<()> = Mutex::new(());

fn library_path() -> OsString {
    std::env::var_os(LIBRARY_ENV).unwrap_or_else(|| OsString::from(DEFAULT_LIBRARY))
}

/// Open the library and resolve every symbol up front (`dynamic_link_require_all`
/// makes this fail if any `mooncake_store_*` symbol is missing).
fn load(path: &OsStr) -> Result<sys::MooncakeStoreLib, StoreError> {
    unsafe { sys::MooncakeStoreLib::new(path) }.map_err(|e| StoreError::LibraryLoad(e.to_string()))
}

/// Load `libmooncake_store.so` from an explicit `path`. Optional; otherwise the
/// library loads lazily on first store creation. Must be called before any store
/// exists, and returns [`StoreError::LibraryAlreadyLoaded`] if one is loaded.
pub fn load_library(path: impl AsRef<Path>) -> Result<(), StoreError> {
    // Recover on poison rather than panic a library call (the section only loads).
    let _guard = INIT_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if API.get().is_some() {
        return Err(StoreError::LibraryAlreadyLoaded);
    }
    let lib = load(path.as_ref().as_os_str())?;
    // Cannot fail: set under INIT_LOCK after the is_some() check above.
    if API.set(lib).is_err() {
        unreachable!("Mooncake API set while holding INIT_LOCK");
    }
    Ok(())
}

/// Load the library from the default path on first use. Called by
/// `MooncakeStore::new()` before any other C call.
pub fn ensure_loaded() -> Result<(), StoreError> {
    if API.get().is_some() {
        return Ok(());
    }
    // Recover on poison rather than panic a library call (the section only loads).
    let _guard = INIT_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if API.get().is_some() {
        return Ok(());
    }
    let lib = load(&library_path())?;
    // Cannot fail: set under INIT_LOCK after the is_some() check above.
    if API.set(lib).is_err() {
        unreachable!("Mooncake API set while holding INIT_LOCK");
    }
    Ok(())
}

/// The loaded loader. `MooncakeStore::new()` calls [`ensure_loaded`] first, so
/// this never fires in practice.
#[inline]
fn api() -> &'static sys::MooncakeStoreLib {
    API.get()
        .expect("Mooncake library not loaded; create a MooncakeStore first")
}

/// Emits a free-function shim per entry that forwards to the generated loader
/// method of the same name. The shim signatures are compile-checked against the
/// bindgen-generated methods, so they cannot silently drift from `store_c.h`.
macro_rules! shims {
    ( $( fn $name:ident ( $( $arg:ident : $argty:ty ),* $(,)? ) $( -> $ret:ty )? ; )* ) => {
        $(
            /// # Safety
            /// Same contract as the C function in `store_c.h`.
            #[inline]
            #[allow(clippy::too_many_arguments)]
            pub unsafe fn $name( $( $arg: $argty ),* ) $( -> $ret )? {
                api().$name( $( $arg ),* )
            }
        )*
    };
}

shims! {
    fn mooncake_store_create() -> mooncake_store_t;
    fn mooncake_store_destroy(store: mooncake_store_t);
    fn mooncake_store_setup(
        store: mooncake_store_t,
        local_hostname: *const c_char,
        metadata_server: *const c_char,
        global_segment_size: u64,
        local_buffer_size: u64,
        protocol: *const c_char,
        device_name: *const c_char,
        master_server_addr: *const c_char,
    ) -> c_int;
    fn mooncake_store_health_check(store: mooncake_store_t) -> c_int;
    fn mooncake_store_put(
        store: mooncake_store_t,
        key: *const c_char,
        value: *const c_void,
        size: usize,
        config: *const mooncake_replicate_config_t,
    ) -> c_int;
    fn mooncake_store_put_from(
        store: mooncake_store_t,
        key: *const c_char,
        buffer: *mut c_void,
        size: usize,
        config: *const mooncake_replicate_config_t,
    ) -> c_int;
    fn mooncake_store_batch_put_from(
        store: mooncake_store_t,
        keys: *mut *const c_char,
        buffers: *mut *mut c_void,
        sizes: *const usize,
        count: usize,
        config: *const mooncake_replicate_config_t,
        results_out: *mut c_int,
    ) -> c_int;
    fn mooncake_store_get_into(
        store: mooncake_store_t,
        key: *const c_char,
        buffer: *mut c_void,
        size: usize,
    ) -> i64;
    fn mooncake_store_batch_get_into(
        store: mooncake_store_t,
        keys: *mut *const c_char,
        buffers: *mut *mut c_void,
        sizes: *const usize,
        count: usize,
        results_out: *mut i64,
    ) -> c_int;
    fn mooncake_store_is_exist(store: mooncake_store_t, key: *const c_char) -> c_int;
    fn mooncake_store_batch_is_exist(
        store: mooncake_store_t,
        keys: *mut *const c_char,
        count: usize,
        results_out: *mut c_int,
    ) -> c_int;
    fn mooncake_store_get_size(store: mooncake_store_t, key: *const c_char) -> i64;
    fn mooncake_store_get_hostname(
        store: mooncake_store_t,
        buf_out: *mut c_char,
        buf_len: usize,
    ) -> c_int;
    fn mooncake_store_remove(store: mooncake_store_t, key: *const c_char, force: c_int) -> c_int;
    fn mooncake_store_remove_by_regex(
        store: mooncake_store_t,
        pattern: *const c_char,
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

#[cfg(test)]
mod tests {
    use super::*;

    // A missing library must surface StoreError::LibraryLoad, not panic. `load`
    // fails before touching the global, so this needs no real .so.
    #[test]
    fn load_library_missing_reports_library_load_error() {
        let err = load_library("/nonexistent/does-not-exist/libmooncake_store.so")
            .expect_err("loading a missing library must fail");
        assert!(matches!(err, StoreError::LibraryLoad(_)), "got {err:?}");
    }

    // new() must surface the load failure, not panic. Safe despite the shared
    // env/global: no unit test loads a real library, so API stays unset.
    #[test]
    fn new_with_missing_library_reports_library_load_error() {
        let prev = std::env::var_os(LIBRARY_ENV);
        std::env::set_var(
            LIBRARY_ENV,
            "/nonexistent/does-not-exist/libmooncake_store.so",
        );
        let result = crate::MooncakeStore::new();
        match prev {
            Some(v) => std::env::set_var(LIBRARY_ENV, v),
            None => std::env::remove_var(LIBRARY_ENV),
        }
        // MooncakeStore isn't Debug, so match rather than expect_err.
        assert!(
            matches!(result, Err(StoreError::LibraryLoad(_))),
            "new() should report LibraryLoad when the library is missing"
        );
    }
}
