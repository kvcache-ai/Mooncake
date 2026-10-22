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

// A module with its own, statically linked TENT core (built with
// -Bsymbolic so it really uses that copy). Used to check that a second TENT
// core in one process refuses plugins instead of mixing with the first.

#include <cstdio>
#include <string>

#include "tent/common/config.h"
#include "tent/transfer_engine.h"
#include "transport_plugin_registry.h"

// Creates an engine of this core with `plugin_dir` and writes
// "<available|unavailable>;<outcome>|<detail>;..." into `out`.
extern "C" __attribute__((visibility("default"))) int
tent_test_second_core_check(const char* plugin_dir, char* out, size_t len) {
    using namespace mooncake::tent;
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("transports/tcp/enable", true);
    config->set("transports/rdma/enable", false);
    config->set("plugins/dir", std::string(plugin_dir));
    config->set("transports/loopback/enable", true);
    std::string result;
    {
        TransferEngine engine(config);
        result = engine.available() ? "available" : "unavailable";
    }
    for (const auto& report : TransportPluginRegistry::Get().reports()) {
        result += std::string(";") + pluginOutcomeName(report.outcome) + "|" +
                  report.detail;
    }
    std::snprintf(out, len, "%s", result.c_str());
    return 0;
}
