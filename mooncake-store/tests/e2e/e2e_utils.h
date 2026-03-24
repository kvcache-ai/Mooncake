#pragma once

#include <gflags/gflags.h>
#include <string>

DECLARE_string(etcd_endpoints);
DECLARE_string(ha_backend_type);
DECLARE_string(ha_backend_connstring);

namespace mooncake {
namespace testing {

// Common flags used in the tests

// Flags for transfer engine
#define FLAG_protocol \
    DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
#define FLAG_device_name                 \
    DEFINE_string(device_name, "ibp6s0", \
                  "Device name to use, valid if protocol=rdma");
#define FLAG_engine_meta_url                                         \
    DEFINE_string(engine_meta_url, "http://127.0.0.1:8080/metadata", \
                  "Metadata connection string for transfer engine");
#define USE_engine_flags FLAG_protocol FLAG_device_name FLAG_engine_meta_url

// Flags for master and client
#define FLAG_etcd_endpoints \
    DEFINE_string(etcd_endpoints, "localhost:2379", "Etcd endpoints");
#define FLAG_ha_backend_type               \
    DEFINE_string(ha_backend_type, "etcd", \
                  "HA backend type for tests: etcd | redis | k8s");
#define FLAG_ha_backend_connstring                                          \
    DEFINE_string(ha_backend_connstring, "",                                \
                  "HA backend connection string for tests; if unset, fall " \
                  "back to etcd_endpoints");
#define FLAG_master_path                                               \
    DEFINE_string(master_path, "./mooncake-store/src/mooncake_master", \
                  "Path to the master executable");
#define FLAG_client_path                                                   \
    DEFINE_string(client_path, "./mooncake-store/tests/e2e/client_runner", \
                  "Path to the client executable");
#define FLAG_out_dir \
    DEFINE_string(out_dir, "./output", "Directory for log files");

// Flags for random seed
#define FLAG_rand_seed                  \
    DEFINE_int32(rand_seed, time(NULL), \
                 "Random seed, 0 means use current time as seed");

inline std::string ResolveTestHABackendConnstring() {
    if (!::FLAGS_ha_backend_connstring.empty()) {
        return ::FLAGS_ha_backend_connstring;
    }
    return ::FLAGS_etcd_endpoints;
}

inline std::string NormalizeConnstringScheme(const std::string& connstring) {
    const auto scheme_pos = connstring.find("://");
    if (scheme_pos == std::string::npos || scheme_pos == 0) {
        return connstring;
    }
    return connstring.substr(scheme_pos + 3);
}

inline std::string BuildTestMasterServerEntry() {
    return ::FLAGS_ha_backend_type + "://" +
           NormalizeConnstringScheme(ResolveTestHABackendConnstring());
}

}  // namespace testing
}  // namespace mooncake
