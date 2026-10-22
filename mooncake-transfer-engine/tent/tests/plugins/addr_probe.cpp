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

// A valid transport plugin that also reports where it sees the core's
// process-wide objects, so a test can check that a plugin loaded with
// RTLD_LOCAL shares them with the host instead of holding private copies.

#include "tent/common/utils/random.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"
#include "tent/transport/shm/shm_transport.h"
#include "tent/transport_plugin.hpp"

namespace mooncake {
namespace tent {
namespace addr_probe {

class ProbeTransport : public Transport {
   public:
    Status install(std::string&, std::shared_ptr<ControlService>,
                   std::shared_ptr<Topology>,
                   std::shared_ptr<Config>) override {
        caps.dram_to_dram = true;
        return Status::OK();
    }
    const char* getName() const override { return "addr_probe"; }
};

}  // namespace addr_probe
}  // namespace tent
}  // namespace mooncake

// out[0]: Platform::getLoader() (out-of-line function)
// out[1]: SimpleRandom::Get() (inline function-local static)
// out[2]: Slab<ShmSubBatch>::Get() (template function-local static)
extern "C" __attribute__((visibility("default"))) void
tent_test_addr_probe_addresses(const void** out) {
    using namespace mooncake::tent;
    out[0] = &Platform::getLoader();
    out[1] = &SimpleRandom::Get();
    out[2] = &Slab<ShmSubBatch>::Get();
}

TENT_DEFINE_TRANSPORT_PLUGIN(addr_probe,
                             mooncake::tent::addr_probe::ProbeTransport,
                             mooncake::tent::UNSPEC, 0,
                             TENT_XPORT_CAP_DRAM_TO_DRAM)
