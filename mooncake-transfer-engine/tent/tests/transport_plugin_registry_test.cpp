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

// Discovery, ABI checks and diagnostics of the transport plugin loader.

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <vector>

#include "tent/common/transport_slots.h"
#include "tent/transport_plugin.hpp"
#include "transport_plugin_test_util.h"

namespace mooncake {
namespace tent {
namespace {

using plugin_test::badPluginDir;
using plugin_test::dupPluginDir;
using plugin_test::LogCapture;
using plugin_test::makeConfig;
using plugin_test::pluginDir;
using plugin_test::reportsByName;
using Peer = ::mooncake::TransferEngineImplTestPeer;

class TransportPluginRegistryTest : public ::testing::Test {
   protected:
    void SetUp() override { TransportPluginRegistry::Get().resetForTest(); }
    void TearDown() override { TransportPluginRegistry::Get().resetForTest(); }
};

TEST_F(TransportPluginRegistryTest, LoadsLoopbackOnce) {
    auto& registry = TransportPluginRegistry::Get();
    const size_t before = registry.dlopenCount();
    auto conf = makeConfig(pluginDir());
    ASSERT_TRUE(registry.ensureLoaded(*conf, nullptr).ok());

    auto reports = reportsByName();
    ASSERT_EQ(reports.size(), 1u);
    ASSERT_TRUE(reports.count("loopback"));
    EXPECT_EQ(reports["loopback"].outcome, PluginOutcome::kLoaded);
    EXPECT_EQ(registry.dlopenCount(), before + 1);
    EXPECT_EQ(parseTransportType("loopback"), kFirstPluginSlot);
    EXPECT_STREQ(transportTypeName(kFirstPluginSlot), "loopback");

    // Same settings again, and different settings: no second load.
    LogCapture log;
    ASSERT_TRUE(registry.ensureLoaded(*conf, nullptr).ok());
    auto other = makeConfig(badPluginDir());
    ASSERT_TRUE(registry.ensureLoaded(*other, nullptr).ok());
    EXPECT_EQ(registry.dlopenCount(), before + 1);
    EXPECT_EQ(registry.reports().size(), 1u);
    EXPECT_TRUE(log.contains("keeping the first"));
    EXPECT_EQ(
        plugin_test::loadedCopies(pluginDir() + "/libtent_xport_loopback.so"),
        1);
}

TEST_F(TransportPluginRegistryTest, NothingConfiguredLoadsNothing) {
    auto& registry = TransportPluginRegistry::Get();
    const size_t before = registry.dlopenCount();
    auto conf = makeConfig("");
    ASSERT_TRUE(registry.ensureLoaded(*conf, nullptr).ok());
    EXPECT_TRUE(registry.reports().empty());
    EXPECT_EQ(registry.dlopenCount(), before);
}

TEST_F(TransportPluginRegistryTest, MissingDirectoryIsDiagnosed) {
    LogCapture log;
    auto conf = makeConfig("/nonexistent/tent_plugins");
    ASSERT_TRUE(
        TransportPluginRegistry::Get().ensureLoaded(*conf, nullptr).ok());
    EXPECT_TRUE(TransportPluginRegistry::Get().reports().empty());
    EXPECT_TRUE(
        log.contains("plugin directory /nonexistent/tent_plugins does not "
                     "exist"));
    // A file instead of a directory.
    TransportPluginRegistry::Get().resetForTest();
    auto file = makeConfig(pluginDir() + "/libtent_xport_loopback.so");
    ASSERT_TRUE(
        TransportPluginRegistry::Get().ensureLoaded(*file, nullptr).ok());
    EXPECT_TRUE(log.contains("libtent_xport_loopback.so is not a directory"));
}

// This test links the core statically, so $ORIGIN is its own directory. It
// runs from "/" so that a relative or working-directory result cannot pass.
TEST_F(TransportPluginRegistryTest, OriginIsTheCoreDirectory) {
    namespace fs = std::filesystem;
    const fs::path exe_dir = fs::read_symlink("/proc/self/exe").parent_path();
    struct RestoreCwd {
        fs::path cwd = fs::current_path();
        ~RestoreCwd() { fs::current_path(cwd); }
    } restore_cwd;
    fs::current_path("/");
    const std::string rel =
        fs::path(pluginDir()).lexically_relative(exe_dir).string();
    ASSERT_FALSE(rel.empty());
    auto conf = makeConfig("$ORIGIN/" + rel);
    ASSERT_TRUE(
        TransportPluginRegistry::Get().ensureLoaded(*conf, nullptr).ok());
    auto reports = reportsByName();
    ASSERT_TRUE(reports.count("loopback"));
    EXPECT_EQ(reports["loopback"].outcome, PluginOutcome::kLoaded);
    EXPECT_EQ(reports["loopback"].path,
              exe_dir.string() + "/" + rel + "/libtent_xport_loopback.so");
    // A missing directory is reported with the entry it came from; the
    // plugins/load warning names the expanded directory.
    TransportPluginRegistry::Get().resetForTest();
    LogCapture log;
    auto missing = makeConfig("$ORIGIN/no_such_dir");
    missing->set("plugins/load", std::vector<std::string>{"vendorx"});
    ASSERT_TRUE(
        TransportPluginRegistry::Get().ensureLoaded(*missing, nullptr).ok());
    EXPECT_TRUE(log.contains("plugin directory " + exe_dir.string() +
                             "/no_such_dir (from $ORIGIN/no_such_dir) does "
                             "not exist"));
    EXPECT_TRUE(log.contains("no libtent_xport_vendorx.so exists in " +
                             exe_dir.string() + "/no_such_dir"));
}

TEST_F(TransportPluginRegistryTest, OriginOnlyAsTheFirstComponent) {
    LogCapture log;
    auto conf = makeConfig("$ORIGINAL/x:/nonexistent/$ORIGIN");
    ASSERT_TRUE(
        TransportPluginRegistry::Get().ensureLoaded(*conf, nullptr).ok());
    EXPECT_TRUE(TransportPluginRegistry::Get().reports().empty());
    EXPECT_TRUE(log.contains("plugin directory $ORIGINAL/x does not exist"));
    EXPECT_TRUE(
        log.contains("plugin directory /nonexistent/$ORIGIN does not exist"));
}

// Every broken plugin in the bad directory gets exactly the outcome it was
// built for, and the engine still comes up and moves data over TCP.
TEST_F(TransportPluginRegistryTest, EveryBadPluginIsDiagnosed) {
    TENT_TEST_REQUIRE_BUILTIN_TCP();
    LogCapture log;
    TransferEngine target(makeConfig(badPluginDir()));
    ASSERT_TRUE(target.available());
    TransferEngine initiator(makeConfig(badPluginDir()));
    ASSERT_TRUE(initiator.available());

    const std::map<std::string, PluginOutcome> expected = {
        {"abi_version", PluginOutcome::kRejectedAbiVersion},
        {"newer_struct", PluginOutcome::kRejectedAbiVersion},
        {"fingerprint", PluginOutcome::kRejectedAbiFingerprint},
        {"missing_entry", PluginOutcome::kRejectedBadEntry},
        {"null_create", PluginOutcome::kRejectedCreateFailed},
        {"probe_fails", PluginOutcome::kNotApplicable},
        {"caps_overclaim", PluginOutcome::kRejectedCapsOverclaim},
        {"install_fails", PluginOutcome::kInstallFailed},
        {"wrongname", PluginOutcome::kRejectedNameMismatch},
        {"tcp", PluginOutcome::kRejectedConflictWithBuiltin},
        {"shm", PluginOutcome::kRejectedDuplicateName},
        {"create_throws", PluginOutcome::kRejectedCreateFailed},
        {"probe_throws", PluginOutcome::kNotApplicable},
        {"builtin_range", PluginOutcome::kRejectedBadEntry},
        {"notify_overclaim", PluginOutcome::kRejectedCapsOverclaim},
        {"cancel_overclaim", PluginOutcome::kRejectedCapsOverclaim},
    };
    auto reports = reportsByName();
    EXPECT_EQ(reports.size(), expected.size());
    for (const auto& [name, outcome] : expected) {
        ASSERT_TRUE(reports.count(name)) << name;
        EXPECT_EQ(reports[name].outcome, outcome)
            << name << ": " << pluginOutcomeName(reports[name].outcome) << " ("
            << reports[name].detail << ")";
        EXPECT_FALSE(reports[name].detail.empty()) << name;
        // One line in the fixed format for the final outcome.
        EXPECT_TRUE(log.contains(std::string("[TransportPlugin] ") +
                                 pluginOutcomeName(outcome) + " " + name +
                                 ": "))
            << name;
    }
    EXPECT_FALSE(reports.count("notaplugin"));
    for (const auto& line : log.lines())
        EXPECT_EQ(line.find("libnotaplugin"), std::string::npos) << line;

    // Specific diagnostics.
    EXPECT_NE(reports["probe_fails"].detail.find("stub device not present"),
              std::string::npos);
    EXPECT_NE(reports["null_create"].detail.find("stub refuses"),
              std::string::npos);
    EXPECT_NE(reports["caps_overclaim"].detail.find("gpu_to_gpu"),
              std::string::npos);
    EXPECT_NE(reports["fingerprint"].detail.find("0x0000000000000001"),
              std::string::npos);
    EXPECT_NE(reports["missing_entry"].detail.find("tent_transport_plugin"),
              std::string::npos);
    EXPECT_NE(reports["create_throws"].detail.find("stub create exploded"),
              std::string::npos);
    EXPECT_NE(reports["probe_throws"].detail.find("stub probe exploded"),
              std::string::npos);
    EXPECT_NE(reports["builtin_range"].detail.find("builtin_type 1000"),
              std::string::npos);
    EXPECT_NE(
        reports["notify_overclaim"].detail.find("TENT_XPORT_FLAG_NOTIFICATION"),
        std::string::npos);
    EXPECT_NE(
        reports["cancel_overclaim"].detail.find("TENT_XPORT_FLAG_CANCELLATION"),
        std::string::npos);
    // The detail points at the engine's own warning for the failed install.
    EXPECT_NE(reports["install_fails"].detail.find("'Transport stub skipped'"),
              std::string::npos);
    EXPECT_TRUE(log.contains("Transport stub skipped"));

    // Rejected plugins left no transport behind.
    for (auto* engine : {&target, &initiator}) {
        auto& transports = Peer::transports(*engine);
        for (int t = kFirstPluginSlot; t < kNumTransportTypes; ++t)
            EXPECT_EQ(transports[t], nullptr) << t;
        ASSERT_NE(transports[TCP], nullptr);
        EXPECT_STREQ(transports[TCP]->getName(), "tcp");
        EXPECT_EQ(Peer::supported(*engine), std::vector<TransportType>{TCP});
    }

    std::vector<char> src(4096, 'q'), dst(4096, 0);
    ASSERT_TRUE(target.registerLocalMemory(dst.data(), dst.size()).ok());
    ASSERT_TRUE(initiator.registerLocalMemory(src.data(), src.size()).ok());
    ASSERT_TRUE(plugin_test::writeOnce(initiator, target.getSegmentName(),
                                       src.data(), dst.data(), src.size(),
                                       UNSPEC)
                    .ok());
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), src.size()), 0);
    initiator.unregisterLocalMemory(src.data(), src.size());
    target.unregisterLocalMemory(dst.data(), dst.size());
}

TEST_F(TransportPluginRegistryTest, DuplicateAcrossDirectories) {
    auto& registry = TransportPluginRegistry::Get();
    const size_t before = registry.dlopenCount();
    auto conf = makeConfig(pluginDir() + ":" + dupPluginDir());
    ASSERT_TRUE(registry.ensureLoaded(*conf, nullptr).ok());
    auto reports = registry.reports();
    ASSERT_EQ(reports.size(), 2u);
    EXPECT_EQ(reports[0].outcome, PluginOutcome::kLoaded);
    EXPECT_EQ(reports[0].path, pluginDir() + "/libtent_xport_loopback.so");
    EXPECT_EQ(reports[1].outcome, PluginOutcome::kRejectedDuplicateName);
    EXPECT_EQ(reports[1].path, dupPluginDir() + "/libtent_xport_loopback.so");
    EXPECT_NE(reports[1].detail.find(reports[0].path), std::string::npos);
    // The duplicate is rejected by name, before it is opened.
    EXPECT_EQ(registry.dlopenCount(), before + 1);
}

TEST_F(TransportPluginRegistryTest, NoFreeSlot) {
    auto& registry = TransportPluginRegistry::Get();
    registry.fillSlotsForTest(kNumPluginSlots);
    auto conf = makeConfig(pluginDir());
    ASSERT_TRUE(registry.ensureLoaded(*conf, nullptr).ok());
    auto reports = reportsByName();
    ASSERT_TRUE(reports.count("loopback"));
    EXPECT_EQ(reports["loopback"].outcome, PluginOutcome::kRejectedNoFreeSlot);
    EXPECT_EQ(parseTransportType("loopback"), UNSPEC);
}

TEST_F(TransportPluginRegistryTest, LoadListFiltersBeforeOpening) {
    LogCapture log;
    auto& registry = TransportPluginRegistry::Get();
    const size_t before = registry.dlopenCount();
    auto conf = makeConfig(pluginDir());
    conf->set("plugins/load", std::vector<std::string>{"vendorx"});
    ASSERT_TRUE(registry.ensureLoaded(*conf, nullptr).ok());
    auto reports = reportsByName();
    ASSERT_TRUE(reports.count("loopback"));
    EXPECT_EQ(reports["loopback"].outcome, PluginOutcome::kDisabledByConfig);
    EXPECT_EQ(registry.dlopenCount(), before);
    EXPECT_TRUE(log.contains("plugins/load lists 'vendorx'"));
}

TEST_F(TransportPluginRegistryTest, EnableFalseKeepsNameButNoTransport) {
    auto conf = makeConfig(pluginDir());
    conf->set("transports/loopback/enable", false);
    TransferEngine engine(conf);
    ASSERT_TRUE(engine.available());
    auto reports = reportsByName();
    EXPECT_EQ(reports["loopback"].outcome, PluginOutcome::kDisabledByConfig);
    const auto slot = parseTransportType("loopback");
    ASSERT_EQ(slot, kFirstPluginSlot);
    EXPECT_EQ(Peer::transports(engine)[slot], nullptr);
}

// The plugin binds to the host's glog: its install() log line reaches a sink
// registered in this executable.
TEST_F(TransportPluginRegistryTest, PluginSharesHostLogging) {
    LogCapture log;
    TransferEngine engine(makeConfig(pluginDir()));
    ASSERT_TRUE(engine.available());
    EXPECT_TRUE(log.contains("Loopback transport installed: slot 19"));
    EXPECT_TRUE(log.contains("[TransportPlugin] loaded loopback: slot 19"));
}

TEST_F(TransportPluginRegistryTest, EnvironmentVariableSetsPluginDir) {
    setenv("MC_TENT_PLUGIN_DIR", pluginDir().c_str(), 1);
    auto conf = makeConfig("");
    conf->set("transports/loopback/enable", true);
    TransferEngine engine(conf);
    unsetenv("MC_TENT_PLUGIN_DIR");
    ASSERT_TRUE(engine.available());
    auto reports = reportsByName();
    ASSERT_TRUE(reports.count("loopback"));
    EXPECT_EQ(reports["loopback"].outcome, PluginOutcome::kLoaded);
    EXPECT_NE(Peer::transports(engine)[kFirstPluginSlot], nullptr);
}

TEST_F(TransportPluginRegistryTest, RequiredFailureIsPluginError) {
    auto& registry = TransportPluginRegistry::Get();
    auto conf = makeConfig(pluginDir());
    conf->set("plugins/required", std::vector<std::string>{"nonexist"});
    ASSERT_TRUE(registry.ensureLoaded(*conf, nullptr).ok());
    TransportList transports;
    auto status = registry.verifyInstalled(*conf, transports);
    EXPECT_TRUE(status.IsPluginError());
    EXPECT_EQ(status.ToString().rfind(
                  "PluginError: required transport plugin 'nonexist' not "
                  "loaded: no libtent_xport_nonexist.so in plugins/dir",
                  0),
              0u)
        << status.ToString();
}

TEST_F(TransportPluginRegistryTest, SamplePluginIsOffByDefault) {
    auto conf = makeConfig(pluginDir());
    conf->set("transports/loopback/enable", nullptr);
    TransferEngine engine(conf);
    ASSERT_TRUE(engine.available());
    auto reports = reportsByName();
    EXPECT_EQ(reports["loopback"].outcome, PluginOutcome::kDisabledByConfig);
    EXPECT_NE(reports["loopback"].detail.find("off by default"),
              std::string::npos);
    EXPECT_EQ(Peer::transports(engine)[kFirstPluginSlot], nullptr);
}

TEST_F(TransportPluginRegistryTest, MalformedRequiredListFails) {
    LogCapture log;
    auto conf = makeConfig(pluginDir());
    conf->set("plugins/required", 5);
    TransferEngine engine(conf);
    EXPECT_FALSE(engine.available());
    EXPECT_TRUE(log.contains("plugins/required must be a list"));
}

TEST_F(TransportPluginRegistryTest, NameListsMayBeSingleStrings) {
    auto conf = makeConfig(pluginDir());
    conf->set("plugins/required", std::string("loopback"));
    conf->set("plugins/load", std::string("loopback"));
    TransferEngine engine(conf);
    ASSERT_TRUE(engine.available());
    EXPECT_EQ(reportsByName()["loopback"].outcome, PluginOutcome::kLoaded);
}

TEST_F(TransportPluginRegistryTest, MalformedLoadListLoadsNothing) {
    LogCapture log;
    auto& registry = TransportPluginRegistry::Get();
    const size_t before = registry.dlopenCount();
    auto conf = makeConfig(pluginDir());
    conf->set("plugins/load", 7);
    TransferEngine engine(conf);
    ASSERT_TRUE(engine.available());
    EXPECT_EQ(registry.dlopenCount(), before);
    EXPECT_TRUE(log.contains("plugins/load must be a list"));
}

// Each engine applies its own plugins/load, and the banner of each engine
// describes that engine even though reports() keeps the latest event.
TEST_F(TransportPluginRegistryTest, LoadListAndSummaryArePerEngine) {
    LogCapture log;
    auto with = makeConfig(pluginDir());
    with->set("verbose", true);
    TransferEngine first(with);
    auto without = makeConfig(pluginDir());
    without->set("verbose", true);
    without->set("plugins/load", std::vector<std::string>{"vendorx"});
    TransferEngine second(without);
    ASSERT_TRUE(first.available() && second.available());
    EXPECT_NE(Peer::transports(first)[kFirstPluginSlot], nullptr);
    EXPECT_EQ(Peer::transports(second)[kFirstPluginSlot], nullptr);
    EXPECT_TRUE(
        log.contains(" - Transport plugins:  loopback(loaded,slot 19)"));
    EXPECT_TRUE(
        log.contains(" - Transport plugins:  loopback(disabled_by_config)"));
}

// construct() can stop between instantiate() and verifyInstalled(); the
// leftover must not be charged to the next engine using the same list.
TEST_F(TransportPluginRegistryTest, AbandonedInstantiateIsNotReportedLater) {
    auto& registry = TransportPluginRegistry::Get();
    auto conf = makeConfig(pluginDir());
    ASSERT_TRUE(registry.ensureLoaded(*conf, nullptr).ok());
    TransportList transports;
    ASSERT_TRUE(registry.instantiate(*conf, nullptr, transports).ok());
    ASSERT_NE(transports[kFirstPluginSlot], nullptr);
    // The engine unwound. Keep the object alive so that the next one cannot
    // reuse its address and mask a stale entry.
    auto abandoned = std::move(transports[kFirstPluginSlot]);

    auto later = makeConfig("");
    later->set("plugins/required", std::vector<std::string>{"loopback"});
    ASSERT_TRUE(registry.instantiate(*later, nullptr, transports).ok());
    std::string summary;
    auto status = registry.verifyInstalled(*later, transports, &summary);
    EXPECT_TRUE(status.IsPluginError());
    EXPECT_NE(status.ToString().find("plugins/dir is not set"),
              std::string::npos);
    EXPECT_EQ(summary, "");
    EXPECT_NE(reportsByName()["loopback"].outcome,
              PluginOutcome::kInstallFailed);

    // A plugin engine at the same address starts clean as well.
    ASSERT_TRUE(registry.instantiate(*conf, nullptr, transports).ok());
    ASSERT_NE(transports[kFirstPluginSlot], abandoned);
    ASSERT_TRUE(registry.verifyInstalled(*conf, transports, &summary).ok());
    EXPECT_EQ(summary, "loopback(loaded,slot 19)");
    transports[kFirstPluginSlot].reset();
    abandoned.reset();
}

// The banner summary describes the engine it is printed for, even when
// another engine changed the process-wide report in between.
TEST_F(TransportPluginRegistryTest, SummaryIsPerEngineWhenInterleaved) {
    auto& registry = TransportPluginRegistry::Get();
    auto off = makeConfig(pluginDir());
    off->set("transports/loopback/enable", false);
    auto on = makeConfig(pluginDir());
    ASSERT_TRUE(registry.ensureLoaded(*on, nullptr).ok());
    TransportList a, b;
    ASSERT_TRUE(registry.instantiate(*off, nullptr, a).ok());
    ASSERT_TRUE(registry.instantiate(*on, nullptr, b).ok());
    std::string summary_b, summary_a;
    ASSERT_TRUE(registry.verifyInstalled(*on, b, &summary_b).ok());
    ASSERT_TRUE(registry.verifyInstalled(*off, a, &summary_a).ok());
    EXPECT_EQ(summary_b, "loopback(loaded,slot 19)");
    EXPECT_EQ(summary_a, "loopback(disabled_by_config)");
    EXPECT_EQ(reportsByName()["loopback"].outcome, PluginOutcome::kLoaded);
    b[kFirstPluginSlot].reset();
}

// Reports follow the order of the files on disk, whether or not a file was
// opened: here the skipped files come before the loaded one.
TEST_F(TransportPluginRegistryTest, ReportsFollowFileOrder) {
    auto conf = makeConfig(badPluginDir() + ":" + pluginDir());
    conf->set("plugins/load", std::vector<std::string>{"loopback"});
    ASSERT_TRUE(
        TransportPluginRegistry::Get().ensureLoaded(*conf, nullptr).ok());
    auto reports = TransportPluginRegistry::Get().reports();
    ASSERT_GE(reports.size(), 2u);
    for (size_t i = 0; i + 1 < reports.size(); ++i) {
        EXPECT_EQ(reports[i].outcome, PluginOutcome::kDisabledByConfig)
            << reports[i].name;
        EXPECT_EQ(reports[i].path.rfind(badPluginDir(), 0), 0u);
        if (i > 0) EXPECT_LT(reports[i - 1].path, reports[i].path);
    }
    EXPECT_EQ(reports.back().name, "loopback");
    EXPECT_EQ(reports.back().outcome, PluginOutcome::kLoaded);
}

TEST_F(TransportPluginRegistryTest, UnreadableDirectoryIsDiagnosed) {
    if (geteuid() == 0) GTEST_SKIP() << "root can list any directory";
    // In the build tree, next to the plugin directories.
    std::string pattern = (std::filesystem::path(pluginDir()).parent_path() /
                           "tent_plugins_noread_XXXXXX")
                              .string();
    ASSERT_NE(mkdtemp(pattern.data()), nullptr);
    ASSERT_EQ(chmod(pattern.c_str(), 0311), 0);
    LogCapture log;
    auto conf = makeConfig(pattern);
    ASSERT_TRUE(
        TransportPluginRegistry::Get().ensureLoaded(*conf, nullptr).ok());
    chmod(pattern.c_str(), 0700);
    rmdir(pattern.c_str());
    EXPECT_TRUE(log.contains("cannot list plugin directory " + pattern));
}

TEST_F(TransportPluginRegistryTest, HostAbiSymbolDescribesThisCore) {
    const auto* host = tent_transport_plugin_host_abi();
    ASSERT_NE(host, nullptr);
    EXPECT_EQ(host->struct_size, sizeof(tent_transport_plugin_host_t));
    EXPECT_EQ(host->abi_version, TENT_TRANSPORT_PLUGIN_ABI_VERSION);
    EXPECT_EQ(host->abi_fingerprint, tent_transport_abi_fingerprint());
    EXPECT_EQ(dlsym(RTLD_DEFAULT, TENT_TRANSPORT_PLUGIN_HOST_ABI_SYMBOL),
              reinterpret_cast<void*>(&tent_transport_plugin_host_abi));
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
