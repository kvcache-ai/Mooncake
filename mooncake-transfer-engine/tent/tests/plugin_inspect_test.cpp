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

// tent_plugin_inspect on the broken plugins of the registry tests, the
// loopback sample and the built-in plugins. With --install-check the verdicts
// must be the ones the loader gives (transport_plugin_registry_test.cpp,
// EveryBadPluginIsDiagnosed); by default the tool stops before create().

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <filesystem>
#include <map>
#include <sstream>
#include <string>

#include "tent/common/config.h"
#include "tent/transport_plugin.hpp"

namespace mooncake {
namespace tent {
namespace {

namespace fs = std::filesystem;

struct Run {
    int exit_code = -1;
    std::string out;
};

std::string quoted(const std::string& path) { return "'" + path + "'"; }

// Runs the tool with `prefix` (environment assignments or a cd) in front; its
// stderr is dropped unless `with_stderr`.
Run inspect(const std::string& args, bool with_stderr = false,
            const std::string& prefix = "") {
    Run run;
    const std::string command = prefix + " " + quoted(TENT_TEST_INSPECT_TOOL) +
                                " " + args +
                                (with_stderr ? " 2>&1" : " 2>/dev/null");
    FILE* pipe = popen(command.c_str(), "r");
    if (!pipe) return run;
    char buf[4096];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), pipe)) > 0) run.out.append(buf, n);
    const int status = pclose(pipe);
    if (WIFEXITED(status)) run.exit_code = WEXITSTATUS(status);
    return run;
}

json inspectJson(const std::string& args, int* exit_code = nullptr) {
    auto run = inspect("--json " + args);
    if (exit_code) *exit_code = run.exit_code;
    return json::parse(run.out);
}

std::string pluginFile(const std::string& dir, const std::string& name) {
    return dir + "/libtent_xport_" + name + ".so";
}

// The same, quoted for the shell.
std::string pluginArg(const std::string& dir, const std::string& name) {
    return quoted(pluginFile(dir, name));
}

const std::string kBadDirArg = quoted(TENT_TEST_BAD_PLUGIN_DIR);

// The loader's verdicts (EveryBadPluginIsDiagnosed).
std::map<std::string, std::string> expectedBadVerdicts() {
    std::map<std::string, std::string> expected = {
        {"abi_version", "rejected_abi_version"},
        {"newer_struct", "rejected_abi_version"},
        {"fingerprint", "rejected_abi_fingerprint"},
        {"missing_entry", "rejected_bad_entry"},
        {"null_create", "rejected_create_failed"},
        {"probe_fails", "not_applicable"},
        {"caps_overclaim", "rejected_caps_overclaim"},
        {"install_fails", "install_failed"},
        {"wrongname", "rejected_name_mismatch"},
        {"tcp", "rejected_conflict_with_builtin"},
        {"shm", "rejected_duplicate_name"},
        {"create_throws", "rejected_create_failed"},
        {"probe_throws", "not_applicable"},
        {"builtin_range", "rejected_bad_entry"},
        {"notify_overclaim", "rejected_caps_overclaim"},
        {"cancel_overclaim", "rejected_caps_overclaim"},
    };
#if TENT_TEST_TCP_PLUGIN_ONLY
    // A core without TCP has no built-in for the stub to conflict with.
    expected["tcp"] = "ok";
#endif
    return expected;
}

TEST(PluginInspect, EachBadPluginGetsTheLoaderVerdict) {
    for (const auto& [name, outcome] : expectedBadVerdicts()) {
        int exit_code = -1;
        auto result = inspectJson(
            "--install-check " + pluginArg(TENT_TEST_BAD_PLUGIN_DIR, name),
            &exit_code);
        ASSERT_EQ(result.size(), 1u) << name;
        EXPECT_EQ(result[0]["outcome"], outcome)
            << name << ": " << result[0]["detail"];
        EXPECT_EQ(exit_code, outcome == "ok" ? 0 : 1) << name;
        if (outcome != "ok")
            EXPECT_FALSE(result[0]["detail"].get<std::string>().empty())
                << name;
    }
}

TEST(PluginInspect, BadDirectoryAtOnce) {
    int exit_code = -1;
    auto result =
        inspectJson("--install-check --dir " + kBadDirArg, &exit_code);
    EXPECT_EQ(exit_code, 1);
    auto expected = expectedBadVerdicts();
    size_t seen = 0;
    for (const auto& entry : result) {
        if (entry["outcome"] == "ignored") continue;
        const std::string name = entry["name"];
        ASSERT_TRUE(expected.count(name)) << name;
        EXPECT_EQ(entry["outcome"], expected[name])
            << name << ": " << entry["detail"];
        ++seen;
    }
    EXPECT_EQ(seen, expected.size());
}

TEST(PluginInspect, ReportsBothSidesOfTheAbi) {
    const uint64_t core = tent_transport_abi_fingerprint();
    auto version =
        inspectJson(pluginArg(TENT_TEST_BAD_PLUGIN_DIR, "abi_version"))[0];
    EXPECT_EQ(version["abi_version"], TENT_TRANSPORT_PLUGIN_ABI_VERSION + 1);
    EXPECT_EQ(version["core_abi_version"], TENT_TRANSPORT_PLUGIN_ABI_VERSION);

    auto fingerprint =
        inspectJson(pluginArg(TENT_TEST_BAD_PLUGIN_DIR, "fingerprint"))[0];
    char core_hex[19];
    std::snprintf(core_hex, sizeof(core_hex), "0x%016llx",
                  static_cast<unsigned long long>(core));
    EXPECT_EQ(fingerprint["core_fingerprint"], core_hex);
    EXPECT_NE(fingerprint["abi_fingerprint"], core_hex);

    auto probe =
        inspectJson(pluginArg(TENT_TEST_BAD_PLUGIN_DIR, "probe_fails"))[0];
    EXPECT_EQ(probe["probe_rc"], 3);
    EXPECT_EQ(probe["probe_error"], "stub device not present");
}

TEST(PluginInspect, LoopbackIsOk) {
    int exit_code = -1;
    auto result =
        inspectJson(pluginArg(TENT_TEST_PLUGIN_DIR, "loopback"), &exit_code);
    EXPECT_EQ(exit_code, 0);
    ASSERT_EQ(result.size(), 1u);
    const auto& loopback = result[0];
    EXPECT_EQ(loopback["outcome"], "ok") << loopback["detail"];
    EXPECT_EQ(loopback["name"], "loopback");
    EXPECT_EQ(loopback["version"], "1");
    EXPECT_EQ(loopback["builtin_type"], 0);
    EXPECT_EQ(loopback["flags"], TENT_XPORT_FLAG_SAME_MACHINE_ONLY |
                                     TENT_XPORT_FLAG_NOTIFICATION |
                                     TENT_XPORT_FLAG_CANCELLATION);
    EXPECT_EQ(loopback["declared_caps"], TENT_XPORT_CAP_DRAM_TO_DRAM);
    EXPECT_EQ(loopback["abi_version"], loopback["core_abi_version"]);
    EXPECT_EQ(loopback["abi_fingerprint"], loopback["core_fingerprint"]);
}

TEST(PluginInspect, BuiltinPlugins) {
    const std::string dir = TENT_TEST_BUILTIN_PLUGIN_DIR;
    int checked = 0;
    for (const char* name : {"tcp", "rdma"}) {
        if (!fs::exists(pluginFile(dir, name))) continue;
        ++checked;
        int exit_code = -1;
        auto entry = inspectJson("--install-check " + pluginArg(dir, name),
                                 &exit_code)[0];
        const std::string outcome = entry["outcome"];
        if (std::string(name) == "rdma" && outcome == "not_applicable") {
            // A host without an active RDMA port.
            EXPECT_NE(entry["probe_rc"], 0);
            EXPECT_FALSE(entry["probe_error"].get<std::string>().empty());
            EXPECT_EQ(exit_code, 1);
            continue;
        }
        // In a "both" build the core has the transport itself.
        const std::string mode = std::string(name) == "tcp"
                                     ? TENT_TEST_XPORT_TCP
                                     : TENT_TEST_XPORT_RDMA;
        EXPECT_EQ(outcome,
                  mode == "both" ? "rejected_conflict_with_builtin" : "ok")
            << name << ": " << entry["detail"];
        EXPECT_NE(entry["builtin_type"], 0) << name;
    }
    if (!checked) GTEST_SKIP() << "TCP and RDMA are built into the core";
}

// Without --install-check no engine is built, so the verdicts that need
// create() or install() are not reached.
TEST(PluginInspect, DefaultStopsBeforeCreate) {
    auto expected = expectedBadVerdicts();
    for (const char* name :
         {"null_create", "create_throws", "caps_overclaim", "install_fails",
          "notify_overclaim", "cancel_overclaim", "tcp"})
        expected[name] = "ok";
    int exit_code = -1;
    auto result = inspectJson("--dir " + kBadDirArg, &exit_code);
    EXPECT_EQ(exit_code, 1);
    size_t seen = 0;
    for (const auto& entry : result) {
        if (entry["outcome"] == "ignored") continue;
        const std::string name = entry["name"];
        ASSERT_TRUE(expected.count(name)) << name;
        EXPECT_EQ(entry["outcome"], expected[name])
            << name << ": " << entry["detail"];
        ++seen;
    }
    EXPECT_EQ(seen, expected.size());

    auto run =
        inspect("--verbose " + pluginArg(TENT_TEST_PLUGIN_DIR, "loopback"),
                /*with_stderr=*/true);
    EXPECT_EQ(run.exit_code, 0) << run.out;
    EXPECT_EQ(run.out.find("Transfer Engine"), std::string::npos) << run.out;
}

TEST(PluginInspect, TableListsFilesThatAreNotPlugins) {
    // In the build tree, next to the plugin directories.
    const fs::path dir = fs::path(TENT_TEST_PLUGIN_DIR).parent_path() /
                         ("tent_plugin_inspect_" + std::to_string(::getpid()));
    fs::create_directories(dir);
    fs::copy_file(pluginFile(TENT_TEST_PLUGIN_DIR, "loopback"),
                  pluginFile(dir.string(), "loopback"));
    std::FILE* readme = std::fopen((dir / "README").c_str(), "w");
    ASSERT_NE(readme, nullptr);
    std::fclose(readme);

    auto run = inspect("--dir " + quoted(dir.string()));
    fs::remove_all(dir);
    EXPECT_EQ(run.exit_code, 0) << run.out;
    EXPECT_NE(run.out.find("FINGERPRINT"), std::string::npos) << run.out;
    std::istringstream lines(run.out);
    std::string line;
    int ok = 0, ignored = 0;
    while (std::getline(lines, line)) {
        if (line.rfind("README", 0) == 0 &&
            line.find("ignored") != std::string::npos)
            ++ignored;
        if (line.rfind("libtent_xport_loopback.so", 0) == 0 &&
            line.size() >= 2 && line.compare(line.size() - 2, 2, "ok") == 0)
            ++ok;
    }
    EXPECT_EQ(ok, 1) << run.out;
    EXPECT_EQ(ignored, 1) << run.out;
}

TEST(PluginInspect, FileNotNamedAsAPlugin) {
    int exit_code = -1;
    auto entry = inspectJson(
        quoted(std::string(TENT_TEST_BAD_PLUGIN_DIR) + "/libnotaplugin.so"),
        &exit_code)[0];
    EXPECT_EQ(entry["outcome"], "rejected_name_mismatch");
    EXPECT_EQ(exit_code, 1);
}

// The install check loads the files it is given whatever the environment
// says, and keeps the engine's log out of the output.
TEST(PluginInspect, InstallCheckIgnoresTheEnvironment) {
    auto run =
        inspect("--json --install-check " +
                    pluginArg(TENT_TEST_BAD_PLUGIN_DIR, "caps_overclaim"),
                false, "MC_TENT_PLUGIN_DIR=" + quoted(TENT_TEST_PLUGIN_DIR));
    auto entry = json::parse(run.out)[0];
    EXPECT_EQ(entry["outcome"], "rejected_caps_overclaim") << entry["detail"];
    EXPECT_EQ(run.exit_code, 1);

    auto quiet = inspect(
        "--install-check " + pluginArg(TENT_TEST_PLUGIN_DIR, "loopback"),
        /*with_stderr=*/true);
    EXPECT_EQ(quiet.exit_code, 0) << quiet.out;
    EXPECT_EQ(quiet.out.find("Transfer Engine"), std::string::npos)
        << quiet.out;
}

// A directory named with a trailing slash is still one directory.
TEST(PluginInspect, InstallCheckOnDirectoryWithTrailingSlash) {
    int exit_code = -1;
    auto result =
        inspectJson("--install-check --dir " +
                        quoted(std::string(TENT_TEST_PLUGIN_DIR) + "/"),
                    &exit_code);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0]["outcome"], "ok") << result[0]["detail"];
    EXPECT_EQ(exit_code, 0);
}

// A plugin file that is a symlink is checked under the name it was listed
// with, not as the file it points to.
TEST(PluginInspect, InstallCheckOnSymlinkedPlugin) {
    const fs::path dir =
        fs::path(TENT_TEST_PLUGIN_DIR).parent_path() /
        ("tent_plugin_inspect_link_" + std::to_string(::getpid()));
    fs::create_directories(dir);
    fs::create_symlink(pluginFile(TENT_TEST_PLUGIN_DIR, "loopback"),
                       pluginFile(dir.string(), "loopback"));
    int exit_code = -1;
    auto result = inspectJson("--install-check --dir " + quoted(dir.string()),
                              &exit_code);
    fs::remove_all(dir);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0]["outcome"], "ok") << result[0]["detail"];
    EXPECT_EQ(exit_code, 0);
}

// A bare file name is the file in the working directory, for the checks and
// for the engine of --install-check, not one the library search path finds
// first: here a broken plugin under the same name in LD_LIBRARY_PATH.
TEST(PluginInspect, BareFileName) {
    const fs::path decoy =
        fs::path(TENT_TEST_PLUGIN_DIR).parent_path() /
        ("tent_plugin_inspect_decoy_" + std::to_string(::getpid()));
    fs::create_directories(decoy);
    fs::copy_file(pluginFile(TENT_TEST_BAD_PLUGIN_DIR, "abi_version"),
                  pluginFile(decoy.string(), "loopback"));
    auto run =
        inspect("--json --install-check libtent_xport_loopback.so", false,
                "cd " + quoted(TENT_TEST_PLUGIN_DIR) +
                    " && LD_LIBRARY_PATH=" + quoted(decoy.string()));
    fs::remove_all(decoy);
    ASSERT_EQ(run.exit_code, 0) << run.out;
    auto entry = json::parse(run.out)[0];
    EXPECT_EQ(entry["outcome"], "ok") << entry["detail"];
}

TEST(PluginInspect, NoPluginFiles) {
    const fs::path dir =
        fs::path(TENT_TEST_PLUGIN_DIR).parent_path() /
        ("tent_plugin_inspect_empty_" + std::to_string(::getpid()));
    fs::create_directories(dir);
    auto run = inspect("--dir " + quoted(dir.string()), /*with_stderr=*/true);
    fs::remove_all(dir);
    EXPECT_EQ(run.exit_code, 1) << run.out;
    EXPECT_NE(run.out.find("no plugin files"), std::string::npos) << run.out;
}

TEST(PluginInspect, UsageErrors) {
    EXPECT_EQ(inspect("").exit_code, 2);
    EXPECT_EQ(inspect("--dir").exit_code, 2);
    EXPECT_EQ(inspect("--json --install-check").exit_code, 2);
    EXPECT_EQ(inspect("--bogus x.so").exit_code, 2);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
