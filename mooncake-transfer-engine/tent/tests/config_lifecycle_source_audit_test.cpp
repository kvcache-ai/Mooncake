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

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <regex>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "tent/common/config_lifecycle.h"

namespace mooncake {
namespace tent {
namespace {

namespace fs = std::filesystem;

struct SourceKey {
    std::string path;
    fs::path source;
    size_t line{0};
};

using ConstantMap = std::unordered_map<std::string, std::vector<SourceKey>>;

bool isProductionSource(const fs::path& path) {
    const auto extension = path.extension().string();
    return extension == ".h" || extension == ".hpp" || extension == ".cpp" ||
           extension == ".cc";
}

std::string readFile(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

// Remove comments while preserving strings, character literals, byte
// positions, and newlines. Keeping positions stable makes diagnostics point to
// the original source line and prevents commented-out Config reads from being
// audited.
std::string stripComments(std::string source) {
    enum class State { kCode, kLineComment, kBlockComment, kString, kChar };
    State state = State::kCode;
    bool escaped = false;

    for (size_t index = 0; index < source.size(); ++index) {
        const char current = source[index];
        const char next = index + 1 < source.size() ? source[index + 1] : '\0';

        if (state == State::kLineComment) {
            if (current == '\n') {
                state = State::kCode;
            } else {
                source[index] = ' ';
            }
            continue;
        }
        if (state == State::kBlockComment) {
            if (current == '*' && next == '/') {
                source[index] = ' ';
                source[index + 1] = ' ';
                ++index;
                state = State::kCode;
            } else if (current != '\n') {
                source[index] = ' ';
            }
            continue;
        }
        if (state == State::kString || state == State::kChar) {
            if (escaped) {
                escaped = false;
            } else if (current == '\\') {
                escaped = true;
            } else if ((state == State::kString && current == '"') ||
                       (state == State::kChar && current == '\'')) {
                state = State::kCode;
            }
            continue;
        }

        if (current == '/' && next == '/') {
            source[index] = ' ';
            source[index + 1] = ' ';
            ++index;
            state = State::kLineComment;
        } else if (current == '/' && next == '*') {
            source[index] = ' ';
            source[index + 1] = ' ';
            ++index;
            state = State::kBlockComment;
        } else if (current == '"') {
            state = State::kString;
        } else if (current == '\'') {
            state = State::kChar;
        }
    }
    return source;
}

size_t lineAt(std::string_view source, size_t offset) {
    return 1 + static_cast<size_t>(
                   std::count(source.begin(), source.begin() + offset, '\n'));
}

void appendLiteralMatches(const std::string& source, const fs::path& path,
                          const std::regex& pattern, size_t capture,
                          std::vector<SourceKey>* keys) {
    for (std::sregex_iterator match(source.begin(), source.end(), pattern), end;
         match != end; ++match) {
        keys->push_back(
            {(*match)[capture].str(), path,
             lineAt(source, static_cast<size_t>(match->position()))});
    }
}

std::string unqualifiedName(std::string name) {
    const auto separator = name.rfind("::");
    if (separator != std::string::npos) name.erase(0, separator + 2);
    return name;
}

std::vector<fs::path> productionSources(const fs::path& root) {
    std::vector<fs::path> sources;
    for (const char* directory : {"include", "src"}) {
        const fs::path source_root = root / directory;
        for (const auto& entry :
             fs::recursive_directory_iterator(source_root)) {
            if (entry.is_regular_file() && isProductionSource(entry.path())) {
                sources.push_back(entry.path());
            }
        }
    }
    std::sort(sources.begin(), sources.end());
    return sources;
}

std::vector<SourceKey> discoverProductionConfigKeys(const fs::path& root) {
    // Config parsing helpers forward their second argument to Config. The env
    // helpers use the second argument for the environment name and the third
    // for the actual configuration path.
    const std::regex forwarded_read(
        R"REGEX((?:captureExplicitConfigValue|restoreExplicitConfigValue|readPositive|readNonNegative|readBool)\s*\(\s*config\s*,\s*"([^"]+)")REGEX");
    const std::regex env_mapping(
        R"REGEX((?:setConfig|setBoolConfig|setArrayConfig)\s*\(\s*config\s*,\s*"[^"]+"\s*,\s*"([^"]+)")REGEX");
    const std::regex constant_assignment(
        R"REGEX(\b([A-Za-z_][A-Za-z0-9_]*)\s*(?:\[\s*\])?\s*=\s*"([^"]+)")REGEX");
    const std::regex config_object(
        R"(\b(?:const\s+)?Config\s*(?:[&*]\s*)*([A-Za-z_][A-Za-z0-9_]*))");
    const std::regex config_smart_pointer(
        R"((?:shared_ptr|unique_ptr)\s*<\s*(?:const\s+)?Config\s*>\s*([A-Za-z_][A-Za-z0-9_]*))");

    const auto sources = productionSources(root);
    std::unordered_map<std::string, std::string> contents;
    ConstantMap constants;
    std::unordered_map<std::string, ConstantMap> constants_by_file;
    std::set<std::string> config_receivers;
    for (const auto& path : sources) {
        auto source = stripComments(readFile(path));
        for (std::sregex_iterator
                 match(source.begin(), source.end(), constant_assignment),
             end;
             match != end; ++match) {
            constants[(*match)[1].str()].push_back(
                {(*match)[2].str(), path,
                 lineAt(source, static_cast<size_t>(match->position()))});
            constants_by_file[path.string()][(*match)[1].str()].push_back(
                {(*match)[2].str(), path,
                 lineAt(source, static_cast<size_t>(match->position()))});
        }
        for (const auto* pattern : {&config_object, &config_smart_pointer}) {
            for (std::sregex_iterator
                     match(source.begin(), source.end(), *pattern),
                 end;
                 match != end; ++match) {
                config_receivers.insert((*match)[1].str());
            }
        }
        contents.emplace(path.string(), std::move(source));
    }

    std::string receiver = "(?:";
    for (auto iterator = config_receivers.begin();
         iterator != config_receivers.end(); ++iterator) {
        if (iterator != config_receivers.begin()) receiver += '|';
        receiver += "\\b" + *iterator + "\\b";
    }
    receiver += ')';

    // Direct Config reads. Building the receiver set from Config declarations
    // avoids treating nlohmann::json::contains/get calls as configuration
    // access without imposing a naming convention on Config variables.
    const std::string method =
        R"((?:get|getArray|contains|dumpSubtree)\s*(?:<[^(){};]*>)?)";
    const std::regex literal_read(receiver + R"(\s*(?:\.|->)\s*)" + method +
                                  R"REGEX(\s*\(\s*"([^"]+)")REGEX");
    const std::regex constant_read(
        receiver + R"(\s*(?:\.|->)\s*)" + method +
        R"(\s*\(\s*((?:[A-Za-z_][A-Za-z0-9_]*::)*[A-Za-z_][A-Za-z0-9_]*))");

    std::vector<SourceKey> keys;
    const std::set<std::string> dynamic_arguments = {"config_key", "key",
                                                     "key_path"};
    for (const auto& path : sources) {
        const auto& source = contents.at(path.string());
        appendLiteralMatches(source, path, literal_read, 1, &keys);
        appendLiteralMatches(source, path, forwarded_read, 1, &keys);
        appendLiteralMatches(source, path, env_mapping, 1, &keys);

        for (std::sregex_iterator
                 match(source.begin(), source.end(), constant_read),
             end;
             match != end; ++match) {
            const std::string argument = (*match)[1].str();
            const std::string name = unqualifiedName(argument);
            if (dynamic_arguments.contains(name)) continue;

            const auto& local_constants = constants_by_file.at(path.string());
            const auto local_values = local_constants.find(name);
            const auto global_values = constants.find(name);
            const std::vector<SourceKey>* values = nullptr;
            if (local_values != local_constants.end()) {
                values = &local_values->second;
            } else if (global_values != constants.end()) {
                values = &global_values->second;
            }
            if (values == nullptr) {
                ADD_FAILURE()
                    << path << ':'
                    << lineAt(source, static_cast<size_t>(match->position()))
                    << ": cannot resolve Config key constant " << argument;
                continue;
            }
            keys.insert(keys.end(), values->begin(), values->end());
        }
    }
    return keys;
}

TEST(ConfigLifecycleSourceAuditTest,
     ProductionConfigReadsAreLifecycleClassified) {
    const fs::path source_root = TENT_SOURCE_ROOT;
    ASSERT_TRUE(fs::is_directory(source_root / "include"));
    ASSERT_TRUE(fs::is_directory(source_root / "src"));

    const auto keys = discoverProductionConfigKeys(source_root);
    ASSERT_GT(keys.size(), 50U);

    std::set<std::string> discovered;
    for (const auto& key : keys) {
        discovered.insert(key.path);
        if (classifyConfigPath(key.path) == ConfigLifecycle::kUnsupported) {
            ADD_FAILURE() << key.source << ':' << key.line
                          << ": Config key is missing from the lifecycle "
                             "inventory: "
                          << key.path;
        }
    }

    // Sentinels cover each supported discovery form so a future edit cannot
    // accidentally turn the audit into an empty or partial scan.
    for (const char* expected : {
             "metadata_type",
             "metrics/http_port",
             "rpc_server_threads",
             "transports/rdma/rail_error_threshold",
             "transports/tcp/max_retry_count",
             "transports/ub/worker_count",
         }) {
        EXPECT_TRUE(discovered.contains(expected)) << expected;
    }
}

TEST(ConfigLifecycleSourceAuditTest, DetectsUnclassifiedDirectRead) {
    const fs::path source_root =
        fs::temp_directory_path() / "mooncake_config_lifecycle_source_audit";
    fs::remove_all(source_root);
    fs::create_directories(source_root / "include");
    fs::create_directories(source_root / "src");

    {
        std::ofstream source(source_root / "src" / "new_config.cpp");
        source << R"(
            void load(const Config& settings) {
                (void)settings.get("new/unregistered", 0);
            }
        )";
    }

    const auto keys = discoverProductionConfigKeys(source_root);
    fs::remove_all(source_root);

    ASSERT_EQ(keys.size(), 1U);
    EXPECT_EQ(keys.front().path, "new/unregistered");
    EXPECT_EQ(classifyConfigPath(keys.front().path),
              ConfigLifecycle::kUnsupported);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
