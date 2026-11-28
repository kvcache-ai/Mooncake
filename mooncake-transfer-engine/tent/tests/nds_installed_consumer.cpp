// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include "tent/transfer_engine.h"

static_assert(mooncake::tent::NDS == TRANSPORT_NDS);
static_assert(TRANSPORT_MEMFABRIC == 18 && TRANSPORT_NDS == 19);

namespace {
struct EngineDeleter {
    void operator()(tent_engine_t engine) const { tent_destroy_engine(engine); }
};
}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) return 1;
    const bool nds_enabled = std::strcmp(argv[1], "on") == 0;
    if (!nds_enabled && std::strcmp(argv[1], "off") != 0) return 1;
    tent_set_config("metadata_type", "p2p");
    tent_set_config("rpc_server_hostname", "127.0.0.1");
    tent_set_config("rpc_server_port", "0");
    tent_set_config("transports/nds/enable", nds_enabled ? "true" : "false");
    std::unique_ptr<void, EngineDeleter> engine(tent_create_engine());
    if (!engine || !tent_available(engine.get())) return 2;
    char name[1024]{};
    int rc = tent_segment_name(engine.get(), name, sizeof(name));
    if (rc || !std::strlen(name)) return 3;

    constexpr size_t kSize = 4096;
    std::unique_ptr<void, decltype(&std::free)> buffer(
        std::aligned_alloc(kSize, kSize), &std::free);
    if (!buffer) return 4;
    tent_memory_options_t options{};
    options.transport_type = TRANSPORT_NDS;
    rc = tent_register_memory_ex(engine.get(), buffer.get(), kSize, &options);
    if (nds_enabled) {
        // Explicit NDS registration fails if the mock driver did not
        // initialize.
        if (rc) return 5;
        if (tent_unregister_memory(engine.get(), buffer.get(), kSize)) return 6;
    } else if (!rc) {
        // Keep cleanup best-effort before reporting that disabled NDS accepted
        // memory.
        (void)tent_unregister_memory(engine.get(), buffer.get(), kSize);
        return 7;
    }
    std::cout << "installed C++/C API consumer (NDS " << argv[1] << "): PASS\n";
    return 0;
}
