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

// Built twice from this file: once as an executable that exports its TENT
// symbols (TENT_TEST_HOST_EXPORTS=1, i.e. -rdynamic) and once without. The
// first must load plugins that share the host's objects; the second must
// refuse every plugin with host_symbols_not_exported instead of crashing.

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

#include "tent/common/utils/random.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"
#include "tent/transport/tcp/tcp_transport.h"
#include "transport_plugin_test_util.h"

namespace mooncake {
namespace tent {
namespace {

using plugin_test::LogCapture;
using plugin_test::makeConfig;
using plugin_test::pluginDir;
using Peer = ::mooncake::TransferEngineImplTestPeer;

class TransportPluginHostTest : public ::testing::Test {
   protected:
    void SetUp() override { TransportPluginRegistry::Get().resetForTest(); }
    void TearDown() override { TransportPluginRegistry::Get().resetForTest(); }
};

#if TENT_TEST_HOST_EXPORTS

TEST_F(TransportPluginHostTest, ExportingHostLoadsOneSharedCopy) {
    TransferEngine target(makeConfig(pluginDir()));
    TransferEngine initiator(makeConfig(pluginDir()));
    ASSERT_TRUE(target.available() && initiator.available());
    EXPECT_EQ(
        plugin_test::loadedCopies(pluginDir() + "/libtent_xport_loopback.so"),
        1);

    std::vector<char> src(65536, 'h'), dst(65536, 0);
    ASSERT_TRUE(initiator.registerLocalMemory(src.data(), src.size()).ok());
    ASSERT_TRUE(target.registerLocalMemory(dst.data(), dst.size()).ok());
    const auto before = plugin_test::loopbackBytesCopied();
    ASSERT_TRUE(plugin_test::writeOnce(initiator, target.getSegmentName(),
                                       src.data(), dst.data(), src.size(),
                                       parseTransportType("loopback"))
                    .ok());
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), src.size()), 0);
    EXPECT_EQ(plugin_test::loopbackBytesCopied() - before,
              (long long)src.size());
    initiator.unregisterLocalMemory(src.data(), src.size());
    target.unregisterLocalMemory(dst.data(), dst.size());
}

// The engine comes first: Platform::getLoader() is initialised once per
// process by its first caller, and only an engine passes it a real config.
TEST_F(TransportPluginHostTest, PluginSeesHostSingletons) {
    auto conf = makeConfig(plugin_test::probePluginDir());
    conf->set("transports/addr_probe/enable", true);
    TransferEngine engine(conf);
    ASSERT_TRUE(engine.available());
    auto reports = plugin_test::reportsByName();
    ASSERT_EQ(reports["addr_probe"].outcome, PluginOutcome::kLoaded)
        << reports["addr_probe"].detail;
    void* handle =
        dlopen((plugin_test::probePluginDir() + "/libtent_xport_addr_probe.so")
                   .c_str(),
               RTLD_NOW | RTLD_NOLOAD);
    ASSERT_NE(handle, nullptr);
    using Fn = void (*)(const void**);
    auto fn =
        reinterpret_cast<Fn>(dlsym(handle, "tent_test_addr_probe_addresses"));
    ASSERT_NE(fn, nullptr);
    const void* seen[3] = {};
    fn(seen);
    EXPECT_EQ(seen[0], &Platform::getLoader());
    EXPECT_EQ(seen[1], &SimpleRandom::Get());
    EXPECT_EQ(seen[2], &Slab<TcpSubBatch>::Get());
    dlclose(handle);
}

#else

TEST_F(TransportPluginHostTest, NonExportingHostRefusesPlugins) {
    LogCapture log;
    TransferEngine engine(makeConfig(pluginDir()));
    ASSERT_TRUE(engine.available());
    auto reports = TransportPluginRegistry::Get().reports();
    ASSERT_EQ(reports.size(), 1u);
    EXPECT_EQ(reports[0].outcome, PluginOutcome::kHostSymbolsNotExported);
    EXPECT_EQ(reports[0].path, pluginDir());
    EXPECT_NE(reports[0].detail.find("-rdynamic"), std::string::npos);
    EXPECT_TRUE(log.contains("[TransportPlugin] host_symbols_not_exported"));
    // Nothing was mapped, nothing was created.
    EXPECT_EQ(
        plugin_test::loadedCopies(pluginDir() + "/libtent_xport_loopback.so"),
        0);
    EXPECT_EQ(TransportPluginRegistry::Get().dlopenCount(), 0u);
    for (int t = kFirstPluginSlot; t < kNumTransportTypes; ++t)
        EXPECT_EQ(Peer::transports(engine)[t], nullptr);
    EXPECT_EQ(parseTransportType("loopback"), UNSPEC);
}

// With a shadowed duplicate in a second directory the reason is still the
// host problem, not the duplicate.
TEST_F(TransportPluginHostTest, RequiredReasonPrefersTheHostProblem) {
    LogCapture log;
    auto conf = makeConfig(pluginDir() + ":" + pluginDir());
    conf->set("plugins/required", std::vector<std::string>{"loopback"});
    TransferEngine engine(conf);
    EXPECT_FALSE(engine.available());
    EXPECT_TRUE(
        log.contains("required transport plugin 'loopback' not "
                     "loaded: host_symbols_not_exported"));
}

TEST_F(TransportPluginHostTest, RequiredPluginReportsTheHostProblem) {
    LogCapture log;
    auto conf = makeConfig(pluginDir());
    conf->set("plugins/required", std::vector<std::string>{"loopback"});
    TransferEngine engine(conf);
    EXPECT_FALSE(engine.available());
    EXPECT_TRUE(
        log.contains("required transport plugin 'loopback' not "
                     "loaded: host_symbols_not_exported"));
}

#endif

}  // namespace
}  // namespace tent
}  // namespace mooncake
