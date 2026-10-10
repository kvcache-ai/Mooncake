// Copyright 2025 KVCache.AI
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

// Two TENT cores in one process: libtent_shared.so (linked by this program)
// and a module with its own static copy. Usage:
//   plugin_two_cores_app <plugin-dir> <second-core.so>
//   shared_first|second_first
// The core that the global scope resolves to loads the plugin; the other
// one must report host_symbols_not_exported, in either order.

#include <dlfcn.h>

#include <cstring>
#include <iostream>
#include <string>

#include "tent/common/config.h"
#include "tent/transfer_engine.h"

using namespace mooncake::tent;

#define REQUIRE(cond)                                                  \
    do {                                                               \
        if (!(cond)) {                                                 \
            std::cerr << "FAILED: " #cond " (line " << __LINE__ << ")" \
                      << std::endl;                                    \
            return 1;                                                  \
        }                                                              \
    } while (0)

static bool sharedCoreLoadsPlugin(const std::string& dir) {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("transports/tcp/enable", true);
    config->set("transports/rdma/enable", false);
    config->set("plugins/dir", dir);
    config->set("transports/loopback/enable", true);
    config->set("plugins/required", std::string("loopback"));
    TransferEngine engine(config);
    return engine.available();
}

int main(int argc, char** argv) {
    REQUIRE(argc == 4);
    const std::string dir = argv[1];
    const std::string order = argv[3];
    REQUIRE(order == "shared_first" || order == "second_first");

    if (order == "shared_first") REQUIRE(sharedCoreLoadsPlugin(dir));

    void* second = dlopen(argv[2], RTLD_NOW | RTLD_LOCAL);
    if (!second) std::cerr << dlerror() << std::endl;
    REQUIRE(second != nullptr);
    using Check = int (*)(const char*, char*, size_t);
    auto check =
        reinterpret_cast<Check>(dlsym(second, "tent_test_second_core_check"));
    REQUIRE(check != nullptr);
    char result[4096] = {};
    REQUIRE(check(dir.c_str(), result, sizeof(result)) == 0);
    std::cout << "second core: " << result << std::endl;
    const std::string text = result;
    REQUIRE(text.rfind("available;host_symbols_not_exported|", 0) == 0);
    REQUIRE(text.find("another TENT copy") != std::string::npos);

    if (order == "second_first") REQUIRE(sharedCoreLoadsPlugin(dir));
    std::cout << "plugin_two_cores_app " << order << ": OK" << std::endl;
    return 0;
}
