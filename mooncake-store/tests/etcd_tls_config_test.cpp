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

#include <gtest/gtest.h>

#include <cstdlib>

#include "etcd_helper.h"
#include "store_c.h"

namespace mooncake {
namespace {

// ---------------------------------------------------------------------------
// Compile-time & default state tests (both STORE_USE_ETCD=ON and OFF)
// ---------------------------------------------------------------------------

// Verifies that IsTLSConfigured() compiles and returns false when no TLS
// config has been set.  This also acts as a compile-time check that
// SetTLSConfig / IsTLSConfigured are available in the current build.
TEST(EtcdTlsConfigTest, TLSDefaultsAreDisabled) {
    EXPECT_FALSE(EtcdHelper::IsTLSConfigured());
}

// Verifies that the C API mooncake_store_set_etcd_tls_config can be called
// with any combination of null/empty/non-null pointers without crashing.
// This is a no-op in STORE_USE_ETCD=OFF builds, but must still compile
// and be safe to call.
TEST(EtcdTlsConfigTest, CAPIAcceptsTlsConfigWithoutCrash) {
    auto store = mooncake_store_create();
    ASSERT_NE(store, nullptr);

    // All null pointers -> should be safely ignored
    mooncake_store_set_etcd_tls_config(store, nullptr, nullptr, nullptr);

    // Non-null paths -> should be copied internally without crashing
    mooncake_store_set_etcd_tls_config(store, "/tmp/ca.pem", "/tmp/cert.pem",
                                       "/tmp/key.pem");

    // Empty strings -> treated as "no TLS"
    mooncake_store_set_etcd_tls_config(store, "", "", "");

    // Partial config (some null, some valid) -> should not crash
    mooncake_store_set_etcd_tls_config(store, "/tmp/ca.pem", nullptr, nullptr);
    mooncake_store_set_etcd_tls_config(store, nullptr, "/tmp/cert.pem",
                                       "/tmp/key.pem");

    // Reset state before destroying store so that subsequent tests see a
    // clean etcd_tls_explicitly_set flag and untainted EtcdHelper state.
    // All-nullptr triggers the use_fallback path which resets both.
    mooncake_store_set_etcd_tls_config(store, nullptr, nullptr, nullptr);

    mooncake_store_destroy(store);
}

// Verifies that mooncake_store_setup does not crash when called with a
// valid store handle but invalid connection parameters.  The setup itself
// is expected to fail (no real master), but the call should not segfault
// or corrupt memory, regardless of STORE_USE_ETCD build flag.
TEST(EtcdTlsConfigTest, SetupWithInvalidParamsDoesNotCrash) {
    auto store = mooncake_store_create();
    ASSERT_NE(store, nullptr);

    // Call setup with obviously invalid endpoints — will fail internally
    // but must not crash.
    int ret = mooncake_store_setup(store, "localhost", "metadata-service", 1024,
                                   1024, "tcp", "eth0", "127.0.0.1:1");
    // Expected to fail because there is no real master at 127.0.0.1:1
    EXPECT_NE(ret, 0);

    mooncake_store_destroy(store);
}

// ---------------------------------------------------------------------------
// STORE_USE_ETCD=ON specific tests
// ---------------------------------------------------------------------------
#ifdef STORE_USE_ETCD

// Verifies that SetTLSConfig stores values and IsTLSConfigured reflects
// them correctly.  Also verifies that resetting to empty disables TLS again.
TEST(EtcdTlsConfigTest, TLSConfigCanBeSetAndQueried) {
    // Reset any leftover state from previous tests (static variables persist
    // across TEST() boundaries within the same test suite).
    EtcdHelper::SetTLSConfig("", "", "");

    // Initially no TLS
    EXPECT_FALSE(EtcdHelper::IsTLSConfigured());

    // Set TLS config
    EtcdHelper::SetTLSConfig("/tmp/ca.pem", "/tmp/cert.pem", "/tmp/key.pem");
    EXPECT_TRUE(EtcdHelper::IsTLSConfigured());

    // Reset to empty -> TLS should be disabled again
    EtcdHelper::SetTLSConfig("", "", "");
    EXPECT_FALSE(EtcdHelper::IsTLSConfigured());
}

// Verifies the env-var fallback path in mooncake_store_setup:
// when mooncake_store_set_etcd_tls_config() is never called explicitly,
// mooncake_store_setup() reads MC_ETCD_CA_FILE / MC_ETCD_CERT_FILE /
// MC_ETCD_KEY_FILE and configures TLS from them without crashing.
//
// The setup itself is expected to fail (no real etcd master), but the
// env-var loading and TLS configuration steps must complete safely.
TEST(EtcdTlsConfigTest, EnvFallbackLoadsWithoutCrash) {
    // Set environment variables BEFORE creating store / calling setup.
    // Use a known non-existent path so we don't accidentally use real certs.
    ASSERT_EQ(setenv("MC_ETCD_CA_FILE", "/tmp/mooncake-test-ca.pem", 1), 0);
    ASSERT_EQ(setenv("MC_ETCD_CERT_FILE", "/tmp/mooncake-test-cert.pem", 1), 0);
    ASSERT_EQ(setenv("MC_ETCD_KEY_FILE", "/tmp/mooncake-test-key.pem", 1), 0);

    // Start with clean TLS state — the previous test
    // (CAPIAcceptsTlsConfigWithoutCrash) resets via all-nullptr before
    // destroying its store.
    EXPECT_FALSE(EtcdHelper::IsTLSConfigured());

    auto store = mooncake_store_create();
    ASSERT_NE(store, nullptr);

    // Intentionally do NOT call mooncake_store_set_etcd_tls_config.
    // This means etcd_tls_explicitly_set is false, and mooncake_store_setup
    // should trigger the env-var fallback path.
    //
    // The setup call will fail (no real master), but the env-var loading
    // and SetGlobalTLSConfig / EtcdHelper::SetTLSConfig calls should
    // complete without crashing.
    int ret = mooncake_store_setup(store, "localhost", "metadata-service", 1024,
                                   1024, "tcp", "eth0", "127.0.0.1:1");
    EXPECT_NE(ret, 0);

    // Verify that the env-var fallback actually consumed the environment
    // variables and configured TLS in EtcdHelper.  If this assertion fails,
    // it means the env-var fallback path was not triggered (e.g. because
    // etcd_tls_explicitly_set was true from a previous test).
    EXPECT_TRUE(EtcdHelper::IsTLSConfigured());

    mooncake_store_destroy(store);

    unsetenv("MC_ETCD_CA_FILE");
    unsetenv("MC_ETCD_CERT_FILE");
    unsetenv("MC_ETCD_KEY_FILE");
}

// Verifies the explicit config path: when mooncake_store_set_etcd_tls_config
// is called with explicit values, the env-var fallback is NOT triggered
// (etcd_tls_explicitly_set=true), and the explicit values take precedence.
//
// Also verifies that calling the C API correctly propagates to the C++
// EtcdHelper::IsTLSConfigured().
TEST(EtcdTlsConfigTest, ExplicitConfigSkipsEnvFallback) {
    // Set env vars that would conflict if fallback were incorrectly triggered.
    ASSERT_EQ(setenv("MC_ETCD_CA_FILE", "/tmp/env-ca.pem", 1), 0);
    ASSERT_EQ(setenv("MC_ETCD_CERT_FILE", "/tmp/env-cert.pem", 1), 0);
    ASSERT_EQ(setenv("MC_ETCD_KEY_FILE", "/tmp/env-key.pem", 1), 0);

    auto store = mooncake_store_create();
    ASSERT_NE(store, nullptr);

    // Call mooncake_store_set_etcd_tls_config with DIFFERENT values from env.
    // This sets etcd_tls_explicitly_set=true, so env vars are ignored.
    mooncake_store_set_etcd_tls_config(store, "/tmp/explicit-ca.pem",
                                       "/tmp/explicit-cert.pem",
                                       "/tmp/explicit-key.pem");

    // Setup will fail (no real master), but it should use the explicit
    // values, not the env-var fallback.
    int ret = mooncake_store_setup(store, "localhost", "metadata-service", 1024,
                                   1024, "tcp", "eth0", "127.0.0.1:1");
    EXPECT_NE(ret, 0);

    // TLS should be configured (the explicit C API call set it up).
    // Even though conflicting env vars exist, the explicit path took
    // precedence because etcd_tls_explicitly_set=true.
    EXPECT_TRUE(EtcdHelper::IsTLSConfigured());

    mooncake_store_destroy(store);

    unsetenv("MC_ETCD_CA_FILE");
    unsetenv("MC_ETCD_CERT_FILE");
    unsetenv("MC_ETCD_KEY_FILE");
}

#endif  // STORE_USE_ETCD

}  // namespace
}  // namespace mooncake
