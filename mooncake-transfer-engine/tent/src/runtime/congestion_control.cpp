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

#include "tent/runtime/congestion_control.h"

namespace mooncake {
namespace tent {

class NoOpCongestionControlPlugin : public CongestionControlPlugin {
   public:
    AdmitDecision onAdmit(const AdmitContext& /*ctx*/) override {
        return {true, 0.0f, 0};
    }

    void onCompletion(const CompletionEvent& /*event*/) override {}

    void onCongestionSignal(const CongestionSignal& /*signal*/) override {}

    uint64_t getDeviceRateLimit(int /*device_id*/) const override { return 0; }

    const char* getName() const override { return "noop"; }
};

std::shared_ptr<CongestionControlPlugin> createDefaultCongestionControlPlugin() {
    return std::make_shared<NoOpCongestionControlPlugin>();
}

}  // namespace tent
}  // namespace mooncake
