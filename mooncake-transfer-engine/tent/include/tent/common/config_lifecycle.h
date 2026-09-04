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

#ifndef TENT_CONFIG_LIFECYCLE_H
#define TENT_CONFIG_LIFECYCLE_H

#include <tent/common/config.h>

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace mooncake {
namespace tent {

// This classification describes whether a field may participate in a future
// runtime update. kRuntimeCandidate does not mean that live application is
// implemented yet; it only records that the field is eligible for that work.
enum class ConfigLifecycle : uint8_t {
    kBootstrapOnly,
    kRuntimeCandidate,
    kDerived,
    kUnsupported,
};

enum class ConfigFieldMatch : uint8_t {
    kExact,
    kSubtree,
};

struct ConfigFieldSpec {
    std::string_view path;
    ConfigLifecycle lifecycle;
    ConfigFieldMatch match;
};

// Return the audited TENT configuration field inventory. When more than one
// entry matches a path, the most specific (longest) path wins.
std::span<const ConfigFieldSpec> configFieldInventory();

ConfigLifecycle classifyConfigPath(std::string_view path);

const char* configLifecycleName(ConfigLifecycle lifecycle);

enum class ConfigDiagnosticCode : uint8_t {
    kInvalidRoot,
    kUnsupportedField,
};

struct ConfigDiagnostic {
    ConfigDiagnosticCode code;
    std::string path;
    std::string message;
};

// A lifecycle-restricted view over a frozen Config. Both bootstrap and runtime
// views share the same immutable backing object, so one bundle always
// represents one coherent capture of the legacy configuration.
class LifecycleConfigView {
   public:
    template <typename T>
    T get(const std::string& key_path, const T& default_value) const {
        if (!canRead(key_path)) return default_value;
        return values_->get<T>(key_path, default_value);
    }

    std::string get(const std::string& key_path,
                    const char* default_value) const {
        return get<std::string>(key_path, std::string(default_value));
    }

    template <typename T>
    std::vector<T> getArray(const std::string& key_path) const {
        return get<std::vector<T>>(key_path, {});
    }

    bool contains(const std::string& key_path) const;

    bool dumpSubtree(const std::string& key_path, std::string* out) const;

    ConfigLifecycle lifecycle() const { return lifecycle_; }

   protected:
    LifecycleConfigView(std::shared_ptr<const Config> values,
                        ConfigLifecycle lifecycle)
        : values_(std::move(values)), lifecycle_(lifecycle) {
        if (values_) configured_paths_ = values_->paths();
    }
    ~LifecycleConfigView() = default;

   private:
    bool allows(std::string_view key_path) const;
    bool canRead(std::string_view key_path) const;

    std::shared_ptr<const Config> values_;
    ConfigLifecycle lifecycle_;
    std::vector<std::string> configured_paths_;
};

class BootstrapConfig final : public LifecycleConfigView {
   public:
    explicit BootstrapConfig(std::shared_ptr<const Config> values)
        : LifecycleConfigView(std::move(values),
                              ConfigLifecycle::kBootstrapOnly) {}
};

class RuntimeConfig final : public LifecycleConfigView {
   public:
    explicit RuntimeConfig(std::shared_ptr<const Config> values)
        : LifecycleConfigView(std::move(values),
                              ConfigLifecycle::kRuntimeCandidate) {}
};

struct RuntimeConfigSnapshot {
    uint64_t generation{0};
    std::shared_ptr<const RuntimeConfig> config;
    int max_failover_attempts{3};
    bool enable_auto_failover_on_poll{true};
};

struct TentConfigBundle {
    std::shared_ptr<const BootstrapConfig> bootstrap;
    std::shared_ptr<const RuntimeConfigSnapshot> runtime;
    std::vector<ConfigDiagnostic> diagnostics;
};

// Adapt an already-merged legacy Config into the canonical lifecycle bundle.
// This deliberately does not load files or environment variables, preserving
// the precedence rules of the caller that produced effective_config.
TentConfigBundle buildTentConfigBundle(const Config& effective_config,
                                       uint64_t generation = 0);

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_CONFIG_LIFECYCLE_H
