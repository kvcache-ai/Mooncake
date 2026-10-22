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

// With no plugin directory configured, an engine must behave exactly as it
// did before transport plugins existed: no file is opened, the transport
// order, the banner and the published descriptor are unchanged.

#include <gtest/gtest.h>

#include <cstdlib>
#include <vector>

#include "transport_plugin_test_util.h"

namespace mooncake {
namespace tent {
namespace {

using plugin_test::LogCapture;
using plugin_test::makeConfig;
using Peer = ::mooncake::TransferEngineImplTestPeer;

// getSupportedTransports(UNSPEC) before plugin slots existed.
std::vector<TransportType> builtinOrder(const TransportList& list) {
    std::vector<TransportType> result;
    for (auto type : {MNNVL, NVLINK, XPU, RDMA, FABRIC, SUNRISE_LINK,
                      AscendDirect, SHM, TCP, HP_TCP, GDS, MPCOMM, TPU, HYLINK})
        if (list[type]) result.push_back(type);
    return result;
}

// The "Loaded Transports" string before plugin slots existed.
std::string builtinBanner(const TransportList& list) {
    std::string out;
    for (int type = 0; type < kNumBuiltinTransportTypes; ++type) {
        if (!list[type]) continue;
        out += list[type]->getName();
        out += " ";
    }
    return out;
}

class TransportPluginNoopTest : public ::testing::Test {
   protected:
    void SetUp() override {
        unsetenv("MC_TENT_PLUGIN_DIR");
        TransportPluginRegistry::Get().resetForTest();
    }
    void TearDown() override { TransportPluginRegistry::Get().resetForTest(); }
};

TEST_F(TransportPluginNoopTest, NoPluginDirectoryChangesNothing) {
    auto& registry = TransportPluginRegistry::Get();
    const size_t dlopen_before = registry.dlopenCount();
    LogCapture log;
    auto conf = makeConfig("");
    conf->set("verbose", true);
#if TENT_TEST_TCP_PLUGIN_ONLY
    // This core has no TCP without plugins; SHM gives registerLocalMemory a
    // transport (it does not take this heap buffer).
    conf->set("transports/shm/enable", true);
#endif
    TransferEngine engine(conf);
    ASSERT_TRUE(engine.available());

    EXPECT_EQ(registry.dlopenCount(), dlopen_before);
    EXPECT_TRUE(registry.reports().empty());
    EXPECT_FALSE(log.contains("[TransportPlugin]"));
    EXPECT_FALSE(log.contains("Transport plugins:"));
    for (int slot = kFirstPluginSlot; slot < kNumTransportTypes; ++slot)
        EXPECT_STREQ(transportTypeName(static_cast<TransportType>(slot)),
                     "unknown");

    const auto& transports = Peer::transports(engine);
    for (int slot = kFirstPluginSlot; slot < kNumTransportTypes; ++slot)
        EXPECT_EQ(transports[slot], nullptr);
    EXPECT_EQ(Peer::supported(engine), builtinOrder(transports));
    EXPECT_TRUE(
        log.contains(" - Loaded Transports:  " + builtinBanner(transports)));

    std::vector<char> buffer(4096);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    // The published descriptor is what the plain serializer produces.
    EXPECT_EQ(Peer::localDumpedJson(engine), Peer::localJson(engine));
    EXPECT_EQ(Peer::localDumpedJson(engine).find("plugin_"), std::string::npos);
    engine.unregisterLocalMemory(buffer.data(), buffer.size());
}

// Counterpart with a plugin loaded: built-in transports keep their order and
// plugin slots follow them.
TEST_F(TransportPluginNoopTest, PluginSlotsComeAfterBuiltins) {
    TransferEngine engine(makeConfig(plugin_test::pluginDir()));
    ASSERT_TRUE(engine.available());
    auto expected = builtinOrder(Peer::transports(engine));
    expected.push_back(parseTransportType("loopback"));
    EXPECT_EQ(Peer::supported(engine), expected);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
