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

// loadTransportPlugins() (src/runtime/transport_plugin_loader.cpp), through
// TransferEngine. TENT_TEST_PLUGIN_ROOT/<name>/ holds libtent_xport_<name>.so,
// built from tests/plugins/.

#include <dlfcn.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/transfer_engine.h"

namespace mooncake {
namespace tent {
namespace {

namespace fs = std::filesystem;

std::string pluginDir(const std::string& name) {
    return std::string(TENT_TEST_PLUGIN_ROOT) + "/" + name;
}

struct LogLine {
    google::LogSeverity severity;
    std::string text;
};

class LogLines : public google::LogSink {
   public:
    LogLines() { google::AddLogSink(this); }
    ~LogLines() override { google::RemoveLogSink(this); }

    void send(google::LogSeverity severity, const char*, const char*, int,
              const std::tm*, const char* message, size_t length) override {
        std::lock_guard<std::mutex> lock(mutex_);
        lines_.push_back({severity, std::string(message, length)});
    }

    std::vector<LogLine> containing(const std::string& text) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<LogLine> found;
        for (const auto& line : lines_)
            if (line.text.find(text) != std::string::npos)
                found.push_back(line);
        return found;
    }

    // The single line about `file` has `severity` and contains `text`.
    void expectOnly(const std::string& file, google::LogSeverity severity,
                    const std::string& text) {
        const auto lines = containing(file);
        ASSERT_EQ(lines.size(), 1u) << file;
        EXPECT_EQ(lines[0].severity, severity) << lines[0].text;
        EXPECT_NE(lines[0].text.find(text), std::string::npos) << lines[0].text;
    }

   private:
    std::mutex mutex_;
    std::vector<LogLine> lines_;
};

// An engine with no transport of its own, so that the SHM slot the test
// plugins take is the only way to move data.
std::shared_ptr<Config> makeConfig(const std::string& plugin_dir) {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "");
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("rpc_server_port", "0");
    for (const char* transport : {"tcp", "shm", "rdma", "io_uring", "nvlink",
                                  "mnnvl", "gds", "ascend_direct"})
        config->set(std::string("transports/") + transport + "/enable", false);
    if (!plugin_dir.empty()) config->set("plugins/dir", plugin_dir);
    return config;
}

// Memory registers with SHM only when a transport fills that slot.
bool hasShmSlot(TransferEngine& engine) {
    std::vector<char> buffer(4096);
    MemoryOptions options;
    options.type = SHM;
    if (!engine.registerLocalMemory(buffer.data(), buffer.size(), options).ok())
        return false;
    EXPECT_TRUE(engine.unregisterLocalMemory(buffer.data()).ok());
    return true;
}

// Writes one registered buffer to another through the engine.
void expectWrite(TransferEngine& engine) {
    std::vector<char> source(4096, 'x'), target(4096);
    for (auto* buffer : {&source, &target})
        ASSERT_TRUE(engine.registerLocalMemory(buffer->data(), 4096).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = 4096;
    request.transport_hint = SHM;
    BatchID batch = engine.allocateBatch(1);
    ASSERT_TRUE(engine.submitTransfer(batch, {request}).ok());
    TransferStatus status;
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, COMPLETED);
    EXPECT_EQ(target, source);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
    for (auto* buffer : {&source, &target})
        EXPECT_TRUE(engine.unregisterLocalMemory(buffer->data()).ok());
}

class TransportPluginLoaderTest : public ::testing::Test {
   protected:
    void SetUp() override { unsetenv("MC_TENT_PLUGIN_DIR"); }
    void TearDown() override { unsetenv("MC_TENT_PLUGIN_DIR"); }

    LogLines log_;
};

TEST_F(TransportPluginLoaderTest, NoDirectoryLoadsNothing) {
    TransferEngine engine(makeConfig(""));
    ASSERT_TRUE(engine.available());
    EXPECT_FALSE(hasShmSlot(engine));
    EXPECT_TRUE(log_.containing("Transport plugin").empty());
}

TEST_F(TransportPluginLoaderTest, MissingDirectoryOnlyWarns) {
    TransferEngine engine(makeConfig(pluginDir("missing")));
    ASSERT_TRUE(engine.available());
    log_.expectOnly(pluginDir("missing"), google::WARNING, "directory");
}

TEST_F(TransportPluginLoaderTest, EnvironmentOverridesConfiguration) {
    setenv("MC_TENT_PLUGIN_DIR", pluginDir("memcpy").c_str(), 1);
    TransferEngine engine(makeConfig(pluginDir("missing")));
    ASSERT_TRUE(engine.available());
    EXPECT_TRUE(hasShmSlot(engine));
    EXPECT_TRUE(log_.containing(pluginDir("missing")).empty());
}

TEST_F(TransportPluginLoaderTest, LoadedPluginMovesData) {
    TransferEngine engine(makeConfig(pluginDir("memcpy")));
    ASSERT_TRUE(engine.available());
    log_.expectOnly("libtent_xport_memcpy.so", google::INFO, "loaded as shm");
    expectWrite(engine);
}

// Two builds of the memcpy plugin, libtent_xport_a.so and libtent_xport_b.so.
TEST_F(TransportPluginLoaderTest, FirstPluginForASlotWins) {
    TransferEngine engine(makeConfig(pluginDir("two")));
    ASSERT_TRUE(engine.available());
    EXPECT_EQ(log_.containing("libtent_xport_a.so loaded as shm").size(), 1u);
    log_.expectOnly("libtent_xport_b.so", google::INFO,
                    "shm is already provided");
}

TEST_F(TransportPluginLoaderTest, CoreTransportIsNotReplaced) {
    auto config = makeConfig(pluginDir("memcpy"));
    config->set("transports/shm/enable", true);
    TransferEngine engine(config);
    ASSERT_TRUE(engine.available());
    log_.expectOnly("libtent_xport_memcpy.so", google::INFO,
                    "shm is already provided");
}

// Each engine installs a transport of its own (the plugin refuses a second
// install), and the plugin stays loaded after both engines are gone.
TEST_F(TransportPluginLoaderTest, EachEngineHasItsOwnTransport) {
    {
        auto first =
            std::make_unique<TransferEngine>(makeConfig(pluginDir("memcpy")));
        TransferEngine second(makeConfig(pluginDir("memcpy")));
        ASSERT_TRUE(first->available() && second.available());
        EXPECT_EQ(log_.containing("loaded as shm").size(), 2u);
        first.reset();
        expectWrite(second);
    }
    const auto path = pluginDir("memcpy") + "/libtent_xport_memcpy.so";
    void* handle = dlopen(path.c_str(), RTLD_NOW | RTLD_NOLOAD);
    EXPECT_NE(handle, nullptr);
    if (handle) dlclose(handle);
}

// A text file under a plugin name, next to files without one.
TEST_F(TransportPluginLoaderTest, NonLibraryFileIsSkipped) {
    const auto dir = pluginDir("text");
    fs::create_directories(dir);
    for (const char* name :
         {"/libtent_xport_text.so", "/libtent_xport_text.so.1", "/notes"})
        std::ofstream(dir + name) << "not a library\n";
    TransferEngine engine(makeConfig(dir));
    ASSERT_TRUE(engine.available());
    log_.expectOnly(dir + "/libtent_xport_text.so", google::WARNING,
                    "skipped: ");
    EXPECT_TRUE(log_.containing("notes").empty());
}

struct BadPlugin {
    const char* name;
    google::LogSeverity severity;
    const char* reason;
};

class BadPluginTest : public TransportPluginLoaderTest,
                      public ::testing::WithParamInterface<BadPlugin> {};

// Each bad plugin gets one log line and leaves the SHM slot empty.
TEST_P(BadPluginTest, IsSkipped) {
    const BadPlugin& plugin = GetParam();
    const auto dir = pluginDir(plugin.name);
    TransferEngine engine(makeConfig(dir));
    ASSERT_TRUE(engine.available());
    EXPECT_FALSE(hasShmSlot(engine));
    log_.expectOnly(dir + "/libtent_xport_" + plugin.name + ".so",
                    plugin.severity, plugin.reason);
}

INSTANTIATE_TEST_SUITE_P(
    TransportPluginLoader, BadPluginTest,
    ::testing::Values(
        BadPlugin{"bad_type", google::WARNING, "does not exist in this TENT"},
        BadPlugin{"declines", google::INFO, "declined this engine"},
        BadPlugin{"throws", google::WARNING,
                  "skipped: exception: no device for this plugin"},
        BadPlugin{"no_entry", google::WARNING,
                  "no descriptor from tent_register_transport_plugin()"}),
    [](const auto& info) { return std::string(info.param.name); });

class TransportPluginLoaderDeathTest : public TransportPluginLoaderTest {};

// Both plugins' C++ entry points abort: had the loader called one before the
// build ID check, the program would die without this message.
TEST_F(TransportPluginLoaderDeathTest, ForeignBuildStopsTheProgram) {
    testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_DEATH(TransferEngine engine(makeConfig(pluginDir("wrong_build"))),
                 "libtent_xport_wrong_build.so was built for another build, "
                 "but this TENT is");
    EXPECT_DEATH(TransferEngine engine(makeConfig(pluginDir("no_build_id"))),
                 "libtent_xport_no_build_id.so has no build ID from "
                 "tent_transport_plugin_build_id\\(\\)");
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
