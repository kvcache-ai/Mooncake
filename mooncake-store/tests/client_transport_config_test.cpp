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

#include "../src/client_transport_config.h"

namespace mooncake::internal {

TEST(ClientTransportConfigTest, ExplicitlyDiscoversEfa) {
    const auto plan = MakeTransportDiscoveryPlan("efa", /*auto_discover=*/true,
                                                 /*force_tcp=*/false);

    EXPECT_FALSE(plan.transfer_engine_auto_discover);
    EXPECT_TRUE(plan.explicitly_discover_efa);
}

TEST(ClientTransportConfigTest, PreservesGenericRdmaDiscovery) {
    const auto plan = MakeTransportDiscoveryPlan("rdma", /*auto_discover=*/true,
                                                 /*force_tcp=*/false);

    EXPECT_TRUE(plan.transfer_engine_auto_discover);
    EXPECT_FALSE(plan.explicitly_discover_efa);
}

TEST(ClientTransportConfigTest, LeavesManualEfaSetupUnchanged) {
    const auto plan = MakeTransportDiscoveryPlan("efa", /*auto_discover=*/false,
                                                 /*force_tcp=*/false);

    EXPECT_FALSE(plan.transfer_engine_auto_discover);
    EXPECT_FALSE(plan.explicitly_discover_efa);
}

TEST(ClientTransportConfigTest, ForceTcpDisablesDiscovery) {
    const auto plan = MakeTransportDiscoveryPlan("efa", /*auto_discover=*/true,
                                                 /*force_tcp=*/true);

    EXPECT_FALSE(plan.transfer_engine_auto_discover);
    EXPECT_FALSE(plan.explicitly_discover_efa);
}

}  // namespace mooncake::internal
