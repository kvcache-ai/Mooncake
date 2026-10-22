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

// Hostile inputs for the transport plugin loader: junk in the plugin
// directory, malformed configuration and peer metadata, and engines created
// and destroyed concurrently.

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>
#include <vector>

#include "tent/common/transport_slots.h"
#include "transport_plugin_test_util.h"
#include "transport_wire_codec.h"

namespace mooncake {
namespace tent {
namespace {

namespace fs = std::filesystem;
using plugin_test::LogCapture;
using plugin_test::makeConfig;
using plugin_test::pluginDir;
using plugin_test::reportsByName;
using Peer = ::mooncake::TransferEngineImplTestPeer;

class TempDir {
   public:
    // Next to the built plugins, which is known to allow exec mappings
    // (/tmp may be mounted noexec).
    TempDir() {
        std::string pattern =
            (fs::path(pluginDir()).parent_path() / "tent_plugins_XXXXXX")
                .string();
        const char* made = mkdtemp(pattern.data());
        EXPECT_NE(made, nullptr) << pattern;
        path_ = made ? made : pattern;
    }
    ~TempDir() {
        std::error_code ec;
        fs::permissions(path_, fs::perms::owner_all, ec);
        for (auto& entry : fs::directory_iterator(path_, ec))
            fs::permissions(entry.path(), fs::perms::owner_all, ec);
        fs::remove_all(path_, ec);
    }
    const std::string& path() const { return path_; }
    std::string file(const std::string& name) const {
        return path_ + "/" + name;
    }

   private:
    std::string path_;
};

class TransportPluginAdversarialTest : public ::testing::Test {
   protected:
    void SetUp() override { TransportPluginRegistry::Get().resetForTest(); }
    void TearDown() override { TransportPluginRegistry::Get().resetForTest(); }
};

TEST_F(TransportPluginAdversarialTest, JunkFilesInPluginDirectory) {
    TempDir dir;
    fs::copy_file(pluginDir() + "/libtent_xport_loopback.so",
                  dir.file("libtent_xport_loopback.so"));
    std::ofstream(dir.file("libtent_xport_garbage.so")) << "not an ELF file";
    std::ofstream(dir.file("libtent_xport_empty.so"));
    fs::create_directory(dir.file("libtent_xport_adir.so"));
    fs::create_symlink(dir.file("missing.so"),
                       dir.file("libtent_xport_dangling.so"));
    std::ofstream(dir.file("libtent_xport_locked.so")) << "x";
    fs::permissions(dir.file("libtent_xport_locked.so"), fs::perms::none);
    std::ofstream(dir.file("libtent_xport_" + std::string(80, 'a') + ".so"));
    std::ofstream(dir.file("libtent_xport_Upper.so"));
    std::ofstream(dir.file("libtent_xport_.so"));
    std::ofstream(dir.file("libtent_xport_loopback.so.1"));
    std::ofstream(dir.file("README"));

    TransferEngine engine(makeConfig(dir.path()));
    ASSERT_TRUE(engine.available());
    auto reports = reportsByName();
    EXPECT_EQ(reports["loopback"].outcome, PluginOutcome::kLoaded);
    EXPECT_EQ(reports["garbage"].outcome, PluginOutcome::kRejectedBadEntry);
    EXPECT_EQ(reports["empty"].outcome, PluginOutcome::kRejectedBadEntry);
    // Unreadable, and not an ELF file even for a reader who may open it.
    EXPECT_EQ(reports["locked"].outcome, PluginOutcome::kRejectedBadEntry);
    EXPECT_EQ(reports[std::string(80, 'a')].outcome,
              PluginOutcome::kRejectedNameMismatch);
    EXPECT_EQ(reports["Upper"].outcome, PluginOutcome::kRejectedNameMismatch);
    // Not regular files or not matching the pattern: never looked at.
    EXPECT_FALSE(reports.count("adir"));
    EXPECT_FALSE(reports.count("dangling"));
    EXPECT_FALSE(reports.count(""));
    EXPECT_EQ(reports.size(), 6u);
    EXPECT_NE(Peer::transports(engine)[parseTransportType("loopback")],
              nullptr);
}

TEST_F(TransportPluginAdversarialTest, DirectoryListEdgeCases) {
    auto conf = makeConfig("::" + pluginDir() + "::/nonexistent::");
    TransferEngine engine(conf);
    ASSERT_TRUE(engine.available());
    auto reports = TransportPluginRegistry::Get().reports();
    ASSERT_EQ(reports.size(), 1u);
    EXPECT_EQ(reports[0].outcome, PluginOutcome::kLoaded);
}

TEST_F(TransportPluginAdversarialTest, SameDirectoryListedTwice) {
    auto& registry = TransportPluginRegistry::Get();
    const size_t before = registry.dlopenCount();
    TransferEngine engine(makeConfig(pluginDir() + ":" + pluginDir()));
    ASSERT_TRUE(engine.available());
    auto reports = registry.reports();
    ASSERT_EQ(reports.size(), 2u);
    EXPECT_EQ(reports[0].outcome, PluginOutcome::kLoaded);
    EXPECT_EQ(reports[1].outcome, PluginOutcome::kRejectedDuplicateName);
    EXPECT_EQ(registry.dlopenCount(), before + 1);
}

TEST_F(TransportPluginAdversarialTest, NonStringPluginDirIsReported) {
    LogCapture log;
    auto conf = makeConfig("");
    conf->set("plugins/dir", std::vector<std::string>{pluginDir()});
    TransferEngine engine(conf);
    ASSERT_TRUE(engine.available());
    EXPECT_TRUE(TransportPluginRegistry::Get().reports().empty());
    EXPECT_TRUE(log.contains("plugins/dir must be a string"));
}

TEST_F(TransportPluginAdversarialTest, ConcurrentEnginesShareOneLoad) {
    auto& registry = TransportPluginRegistry::Get();
    const size_t before = registry.dlopenCount();
    constexpr int kEngines = 8;
    std::vector<std::unique_ptr<TransferEngine>> engines(kEngines);
    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    for (int i = 0; i < kEngines; ++i) {
        threads.emplace_back([&, i] {
            while (!go.load()) std::this_thread::yield();
            engines[i] =
                std::make_unique<TransferEngine>(makeConfig(pluginDir()));
        });
    }
    go.store(true);
    for (auto& t : threads) t.join();
    EXPECT_EQ(registry.dlopenCount(), before + 1);
    EXPECT_EQ(
        plugin_test::loadedCopies(pluginDir() + "/libtent_xport_loopback.so"),
        1);
    const auto slot = parseTransportType("loopback");
    ASSERT_EQ(slot, kFirstPluginSlot);
    for (auto& engine : engines) {
        ASSERT_TRUE(engine->available());
        EXPECT_NE(Peer::transports(*engine)[slot], nullptr);
    }

    // Every engine writes to its neighbour over the plugin.
    std::vector<std::vector<char>> buffers(kEngines,
                                           std::vector<char>(4096, 0));
    for (int i = 0; i < kEngines; ++i) {
        std::fill(buffers[i].begin(), buffers[i].end(), char('a' + i));
        ASSERT_TRUE(
            engines[i]->registerLocalMemory(buffers[i].data(), 4096).ok());
    }
    std::vector<char> scratch(kEngines * 4096);
    for (int i = 0; i < kEngines; ++i) {
        char* src = scratch.data() + i * 4096;
        std::fill(src, src + 4096, char('A' + i));
        ASSERT_TRUE(engines[i]->registerLocalMemory(src, 4096).ok());
    }
    threads.clear();
    std::atomic<int> failures{0};
    for (int i = 0; i < kEngines; ++i) {
        threads.emplace_back([&, i] {
            auto& target = *engines[(i + 1) % kEngines];
            char* src = scratch.data() + i * 4096;
            if (!plugin_test::writeOnce(*engines[i], target.getSegmentName(),
                                        src, buffers[(i + 1) % kEngines].data(),
                                        4096, slot)
                     .ok())
                failures.fetch_add(1);
        });
    }
    for (auto& t : threads) t.join();
    EXPECT_EQ(failures.load(), 0);
    for (int i = 0; i < kEngines; ++i) {
        const char expected = char('A' + (i + kEngines - 1) % kEngines);
        EXPECT_EQ(buffers[i][0], expected) << i;
        EXPECT_EQ(buffers[i][4095], expected) << i;
    }
    for (int i = 0; i < kEngines; ++i) {
        engines[i]->unregisterLocalMemory(buffers[i].data(), 4096);
        engines[i]->unregisterLocalMemory(scratch.data() + i * 4096, 4096);
    }

    // Tear them down concurrently as well.
    threads.clear();
    for (int i = 0; i < kEngines; ++i)
        threads.emplace_back([&, i] { engines[i].reset(); });
    for (auto& t : threads) t.join();
}

// Randomized peer metadata: decoding never throws, never yields an index
// outside the transport array, and never duplicates a plugin slot.
TEST_F(TransportPluginAdversarialTest, RandomPeerTransportsDecodeSafely) {
    TransportType loopback = UNSPEC;
    ASSERT_EQ(assignPluginTransportSlot("loopback", 0, loopback),
              SlotAssignResult::kAssigned);
    std::mt19937 rng(20261011);
    std::uniform_int_distribution<int> value(-50, 60);
    std::uniform_int_distribution<int> count(0, 12);
    const std::vector<std::string> names = {"loopback", "vendorx", "",
                                            "loopback", "UNSPEC",  "tcp"};
    for (int round = 0; round < 2000; ++round) {
        json buffer = {{"addr", 4096},          {"length", 64},
                       {"location", "cpu:0"},   {"regions", json::array()},
                       {"rkey", json::array()}, {"shm_path", ""},
                       {"mnnvl_handle", ""},    {"internal", false}};
        json transports = json::array();
        json attrs = json::array();
        for (int i = count(rng); i > 0; --i) {
            transports.push_back(value(rng));
            attrs.push_back(json::array({value(rng), "a"}));
        }
        buffer["transports"] = transports;
        buffer["transport_attrs"] = attrs;
        switch (round % 4) {
            case 0:
                buffer["plugin_transports"] =
                    json::array({names[round % names.size()],
                                 names[(round / 3) % names.size()]});
                break;
            case 1:
                buffer["plugin_transports"] = value(rng);  // wrong type
                break;
            case 2:
                buffer["plugin_transports"] = json::array({1, nullptr});
                buffer["plugin_transport_attrs"] = json::array({1});
                break;
            default:
                buffer["plugin_transport_attrs"] = {{"loopback", 7},
                                                    {"vendorx", "v"}};
        }
        json segment = {{"name", "peer"},
                        {"type", "memory"},
                        {"machine_id", "m"},
                        {"rpc_server_addr", "127.0.0.1:1"},
                        {"detail",
                         {{"device_attrs", json::object()},
                          {"buffers", json::array({buffer})},
                          {"devices", json::array()},
                          {"transport_attrs",
                           json::array({json::array({value(rng), "s"})})},
                          {"plugin_transport_attrs",
                           round % 2 ? json(5) : json::object()}}}};
        SegmentDesc desc;
        ASSERT_NO_THROW(desc = segment.get<SegmentDesc>()) << segment.dump();
        decodeSegmentDescFromWire(desc);
        const auto& decoded = desc.getMemory().buffers[0];
        int loopbacks = 0;
        for (auto type : decoded.transports) {
            const bool valid =
                (type >= 0 && type < kNumBuiltinTransportTypes) ||
                type == loopback;
            EXPECT_TRUE(valid) << type << " from " << segment.dump();
            if (type == loopback) ++loopbacks;
        }
        EXPECT_LE(loopbacks, 1);
        for (const auto& [type, attr] : decoded.transport_attrs) {
            EXPECT_TRUE((type >= 0 && type < kNumBuiltinTransportTypes) ||
                        type == loopback);
        }
        for (const auto& [type, attr] : desc.getMemory().transport_attrs)
            EXPECT_TRUE(type >= 0 && type < kNumBuiltinTransportTypes);
        EXPECT_TRUE(decoded.plugin_transports.empty());
    }
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
