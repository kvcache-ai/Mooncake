#pragma once

// Internal to Mooncake: this header lives under src/ and is only exported to
// in-tree targets through mooncake_common's build interface.

#include <functional>
#include <initializer_list>
#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "ascii_string.h"
#include "environment_value_parser.h"
#include "environment_variable.h"

namespace mooncake {

namespace test {
class EnvironTestPeer;
}  // namespace test

// Where environment values come from. Production code reads the process
// environment through Environ::Process(); tests inject a MapEnvironSource
// instead of mutating it.
class EnvironSource {
   public:
    virtual ~EnvironSource() = default;
    virtual std::optional<std::string> Get(std::string_view name) const = 0;
};

// Fixed name/value pairs, for tests.
class MapEnvironSource final : public EnvironSource {
   public:
    MapEnvironSource() = default;
    MapEnvironSource(
        std::initializer_list<std::pair<const std::string, std::string>> values)
        : values_(values) {}

    std::optional<std::string> Get(std::string_view name) const override;
    void Set(std::string name, std::string value);
    void Unset(std::string_view name);

   private:
    std::map<std::string, std::string, std::less<>> values_;
};

// Reads and parses environment variables from an EnvironSource. Environ holds
// no settings of its own: each component resolves its configuration once
// (typically at startup or first use) and keeps the result.
class Environ {
   public:
    // `source` must outlive this Environ.
    explicit Environ(const EnvironSource& source) : source_(&source) {}

    // Environ over the process environment. The environment is captured on
    // the first read and every later read is served from that snapshot, so
    // changes made afterwards (setenv, os.environ, ...) are not observed. Set
    // variables before initializing Mooncake components, and do not read from
    // static initializers.
    static const Environ& Process();

    // Raw value, or nullopt when the variable is unset.
    std::optional<std::string> Get(std::string_view name) const {
        return source_->Get(name);
    }

    // Missing or invalid values return nullopt; string variables preserve an
    // explicitly empty value.
    template <typename T>
    std::optional<T> GetTyped(const EnvironmentVariable<T>& variable) const {
        const auto value = Get(variable.name);
        if (!value.has_value()) {
            return std::nullopt;
        }
        return TryParseEnvironmentValue<T>(*value);
    }

    // Missing values return `default_value`; invalid values warn and return
    // `default_value`.
    template <typename T>
    T GetTypedOr(const EnvironmentVariable<T>& variable,
                 T default_value) const {
        const auto value = Get(variable.name);
        if (!value.has_value()) {
            return default_value;
        }

        auto parsed = TryParseEnvironmentValue<T>(*value);
        if (parsed.has_value()) {
            return std::move(*parsed);
        }
        std::cerr << "[Mooncake] Warning: invalid value '" << *value
                  << "' for env " << variable.name << ", using default "
                  << default_value << std::endl;
        return default_value;
    }

    // Splits the value on `delimiter`, trims ASCII whitespace around each
    // item, and parses every item as T. An empty value yields an empty list.
    // Returns nullopt when the variable is unset or any item is invalid.
    template <typename T>
    std::optional<std::vector<T>> GetList(
        const EnvironmentVariable<std::vector<T>>& variable,
        char delimiter = ',') const {
        const auto value = Get(variable.name);
        if (!value.has_value()) {
            return std::nullopt;
        }

        std::vector<T> items;
        if (value->empty()) {
            return items;
        }
        std::string_view rest = *value;
        while (true) {
            const size_t pos = rest.find(delimiter);
            auto parsed = TryParseEnvironmentValue<T>(
                TrimAsciiWhitespace(rest.substr(0, pos)));
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            items.push_back(std::move(*parsed));
            if (pos == std::string_view::npos) {
                return items;
            }
            rest.remove_prefix(pos + 1);
        }
    }

   private:
    friend class test::EnvironTestPeer;

    // Captures the process environment behind Process() again. Not
    // synchronized with concurrent reads.
    static void RefreshProcess();

    const EnvironSource* source_;
};

}  // namespace mooncake
