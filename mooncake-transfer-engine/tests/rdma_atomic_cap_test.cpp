// Copyright 2026 KVCache.AI
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

#include <infiniband/verbs.h>

#include "config.h"

namespace mooncake {
namespace {

// Populate every device attribute updateGlobalConfig() inspects with a value
// large enough that only the RD-atomic fields under test can tighten the
// configuration; the remaining fields stay at their defaults.
ibv_device_attr makePermissiveDeviceAttr() {
    ibv_device_attr attr{};
    attr.max_qp = 1 << 20;
    attr.max_cq = 1 << 20;
    attr.max_qp_wr = 1 << 20;
    attr.max_sge = 1 << 20;
    attr.max_cqe = 1 << 20;
    attr.max_mr_size = 1ull << 60;
    attr.max_qp_init_rd_atom = 16;
    attr.max_qp_rd_atom = 16;
    return attr;
}

// globalConfig() is a process-wide singleton; restore it so the assertions in
// these cases cannot leak into later tests in the same binary.
class ScopedGlobalConfig {
   public:
    ScopedGlobalConfig() : saved_(globalConfig()) {}
    ~ScopedGlobalConfig() { globalConfig() = saved_; }

   private:
    GlobalConfig saved_;
};

}  // namespace

TEST(RdmaAtomicCapTest, ClampsBothDirectionsToDeviceLimits) {
    ScopedGlobalConfig restore;
    globalConfig().max_qp_init_rd_atom = 16;
    globalConfig().max_qp_rd_atom = 16;

    auto attr = makePermissiveDeviceAttr();
    attr.max_qp_init_rd_atom = 4;
    attr.max_qp_rd_atom = 8;
    updateGlobalConfig(attr);

    EXPECT_EQ(globalConfig().max_qp_init_rd_atom, 4);
    EXPECT_EQ(globalConfig().max_qp_rd_atom, 8);
}

TEST(RdmaAtomicCapTest, KeepsConfiguredValueWhenDeviceIsMoreCapable) {
    ScopedGlobalConfig restore;
    globalConfig().max_qp_init_rd_atom = 8;
    globalConfig().max_qp_rd_atom = 12;

    auto attr = makePermissiveDeviceAttr();
    updateGlobalConfig(attr);

    EXPECT_EQ(globalConfig().max_qp_init_rd_atom, 8);
    EXPECT_EQ(globalConfig().max_qp_rd_atom, 12);
}

}  // namespace mooncake
