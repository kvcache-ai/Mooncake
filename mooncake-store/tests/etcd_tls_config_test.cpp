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
// with any combination of enabled/null/empty/non-null pointers without
// crashing.  This is a no-op in STORE_USE_ETCD=OFF builds, but must still
// compile and be safe to call.
TEST(EtcdTlsConfigTest, CAPIAcceptsTlsConfigWithoutCrash) {
    auto store = mooncake_store_create();
    ASSERT_NE(store, nullptr);

    // enabled=0, all null pointers -> should be safely ignored
    mooncake_store_set_etcd_tls_config(store, 0, nullptr, nullptr, nullptr);

    // enabled=1, non-null paths -> should be copied internally without crashing
    mooncake_store_set_etcd_tls_config(store, 1, "/tmp/ca.pem", "/tmp/cert.pem",
                                       "/tmp/key.pem");

    // enabled=0 -> disables TLS regardless of cert paths
    mooncake_store_set_etcd_tls_config(store, 0, "", "", "");

    // enabled=1 but partial config -> should not crash (will log error)
    mooncake_store_set_etcd_tls_config(store, 1, "/tmp/ca.pem", nullptr,
                                       nullptr);
    mooncake_store_set_etcd_tls_config(store, 1, nullptr, "/tmp/cert.pem",
                                       "/tmp/key.pem");

    // Reset state before destroying store so that subsequent tests see a
    // clean etcd_tls_explicitly_set flag and untainted EtcdHelper state.
    mooncake_store_set_etcd_tls_config(store, 0, nullptr, nullptr, nullptr);

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
// them correctly.  Also verifies that the enabled flag controls TLS state.
TEST(EtcdTlsConfigTest, TLSConfigCanBeSetAndQueried) {
    // Reset any leftover state from previous tests (static variables persist
    // across TEST() boundaries within the same test suite).
    EtcdHelper::SetTLSConfig(false, "", "", "");

    // Initially no TLS
    EXPECT_FALSE(EtcdHelper::IsTLSConfigured());

    // Set TLS config with enabled=true
    EtcdHelper::SetTLSConfig(true, "/tmp/ca.pem", "/tmp/cert.pem",
                             "/tmp/key.pem");
    EXPECT_TRUE(EtcdHelper::IsTLSConfigured());

    // Disable TLS -> should be disabled even if cert paths are non-empty
    EtcdHelper::SetTLSConfig(false, "/tmp/ca.pem", "/tmp/cert.pem",
                             "/tmp/key.pem");
    EXPECT_FALSE(EtcdHelper::IsTLSConfigured());

    // Enable TLS again
    EtcdHelper::SetTLSConfig(true, "/tmp/ca.pem", "/tmp/cert.pem",
                             "/tmp/key.pem");
    EXPECT_TRUE(EtcdHelper::IsTLSConfigured());

    // Reset to disabled
    EtcdHelper::SetTLSConfig(false, "", "", "");
    EXPECT_FALSE(EtcdHelper::IsTLSConfigured());
}

// Verifies that enabling TLS with missing cert paths does not activate TLS.
TEST(EtcdTlsConfigTest, TLSRequiresAllCertsWhenEnabled) {
    EtcdHelper::SetTLSConfig(false, "", "", "");

    // enabled=true but missing cert/key -> TLS should NOT be activated
    EtcdHelper::SetTLSConfig(true, "/tmp/ca.pem", "", "");
    EXPECT_FALSE(EtcdHelper::IsTLSConfigured());

    // enabled=true but missing key -> TLS should NOT be activated
    EtcdHelper::SetTLSConfig(true, "/tmp/ca.pem", "/tmp/cert.pem", "");
    EXPECT_FALSE(EtcdHelper::IsTLSConfigured());

    // enabled=true with all three -> TLS should be activated
    EtcdHelper::SetTLSConfig(true, "/tmp/ca.pem", "/tmp/cert.pem",
                             "/tmp/key.pem");
    EXPECT_TRUE(EtcdHelper::IsTLSConfigured());

    // Cleanup
    EtcdHelper::SetTLSConfig(false, "", "", "");
}

// Verifies the env-var fallback path in mooncake_store_setup:
// when mooncake_store_set_etcd_tls_config() is never called explicitly,
// mooncake_store_setup() reads MC_ETCD_* env vars and configures TLS
// from them without crashing.
TEST(EtcdTlsConfigTest, EnvFallbackLoadsWithoutCrash) {
    // Set environment variables BEFORE creating store / calling setup.
    ASSERT_EQ(setenv("MC_ETCD_CA_FILE", "/tmp/mooncake-test-ca.pem", 1), 0);
    ASSERT_EQ(setenv("MC_ETCD_CERT_FILE", "/tmp/mooncake-test-cert.pem", 1), 0);
    ASSERT_EQ(setenv("MC_ETCD_KEY_FILE", "/tmp/mooncake-test-key.pem", 1), 0);

    // Start with clean TLS state
    EXPECT_FALSE(EtcdHelper::IsTLSConfigured());

    auto store = mooncake_store_create();
    ASSERT_NE(store, nullptr);

    // Intentionally do NOT call mooncake_store_set_etcd_tls_config.
    // The setup call will fail (no real master), but the env-var loading
    // should complete without crashing.
    int ret = mooncake_store_setup(store, "localhost", "metadata-service", 1024,
                                   1024, "tcp", "eth0", "127.0.0.1:1");
    EXPECT_NE(ret, 0);

    // The env-var fallback should have consumed the environment variables
    // and configured TLS in EtcdHelper (enabled is auto-set when env vars
    // provide all three cert paths).
    EXPECT_TRUE(EtcdHelper::IsTLSConfigured());

    mooncake_store_destroy(store);

    unsetenv("MC_ETCD_CA_FILE");
    unsetenv("MC_ETCD_CERT_FILE");
    unsetenv("MC_ETCD_KEY_FILE");
}

// Verifies the explicit config path: when mooncake_store_set_etcd_tls_config
// is called with explicit values, the env-var fallback is NOT triggered.
TEST(EtcdTlsConfigTest, ExplicitConfigSkipsEnvFallback) {
    // Set env vars that would conflict if fallback were incorrectly triggered.
    ASSERT_EQ(setenv("MC_ETCD_CA_FILE", "/tmp/env-ca.pem", 1), 0);
    ASSERT_EQ(setenv("MC_ETCD_CERT_FILE", "/tmp/env-cert.pem", 1), 0);
    ASSERT_EQ(setenv("MC_ETCD_KEY_FILE", "/tmp/env-key.pem", 1), 0);

    auto store = mooncake_store_create();
    ASSERT_NE(store, nullptr);

    // Call mooncake_store_set_etcd_tls_config with DIFFERENT values from env.
    // This sets etcd_tls_explicitly_set=true, so env vars are ignored.
    mooncake_store_set_etcd_tls_config(store, 1, "/tmp/explicit-ca.pem",
                                       "/tmp/explicit-cert.pem",
                                       "/tmp/explicit-key.pem");

    // Setup will fail (no real master), but it should use the explicit
    // values, not the env-var fallback.
    int ret = mooncake_store_setup(store, "localhost", "metadata-service", 1024,
                                   1024, "tcp", "eth0", "127.0.0.1:1");
    EXPECT_NE(ret, 0);

    // TLS should be configured (the explicit C API call set it up).
    EXPECT_TRUE(EtcdHelper::IsTLSConfigured());

    mooncake_store_destroy(store);

    unsetenv("MC_ETCD_CA_FILE");
    unsetenv("MC_ETCD_CERT_FILE");
    unsetenv("MC_ETCD_KEY_FILE");
}

// Verifies that enabled=0 disables TLS even if cert paths are provided.
TEST(EtcdTlsConfigTest, DisabledFlagOverridesCertPaths) {
    auto store = mooncake_store_create();
    ASSERT_NE(store, nullptr);

    // First enable TLS
    mooncake_store_set_etcd_tls_config(store, 1, "/tmp/ca.pem", "/tmp/cert.pem",
                                       "/tmp/key.pem");
    EXPECT_TRUE(EtcdHelper::IsTLSConfigured());

    // Now disable with enabled=0 but keep cert paths -> TLS should be off
    mooncake_store_set_etcd_tls_config(store, 0, "/tmp/ca.pem", "/tmp/cert.pem",
                                       "/tmp/key.pem");
    EXPECT_FALSE(EtcdHelper::IsTLSConfigured());

    mooncake_store_destroy(store);
}

#endif  // STORE_USE_ETCD

}  // namespace
}  // namespace mooncake
