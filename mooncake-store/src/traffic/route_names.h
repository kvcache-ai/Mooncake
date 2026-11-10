#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace mooncake::traffic {

// coro_rpc renders a registered function as "ns::Service::method(params) ->
// ret". Traffic configuration names the bare method, so drop the signature and
// every qualification. Taking the last "::" segment keeps this independent of
// the concrete service class.
inline std::string_view ShortRouteName(std::string_view name) {
    const auto args = name.find('(');
    if (args != std::string_view::npos) {
        name = name.substr(0, args);
    }
    const auto last = name.rfind("::");
    if (last != std::string_view::npos) {
        name.remove_prefix(last + 2);
    }
    const auto angle = name.find('<');
    return angle == std::string_view::npos ? name : name.substr(0, angle);
}

// Route keys the RPC service registered, mapped to the method name coro_rpc
// dispatches them under. coro_rpc reports the handler it stored, which for a
// pooled handler is the pool's coroutine rather than the service method, so the
// registration site reports the keys and names instead of reading them back.
class RouteNameRegistry {
   public:
    void Add(std::uint32_t route_key, std::string_view registered_name) {
        names_.try_emplace(route_key, ShortRouteName(registered_name));
    }

    std::string_view NameOf(std::uint32_t route_key) const {
        const auto it = names_.find(route_key);
        return it == names_.end() ? std::string_view{} : it->second;
    }

    bool ContainsName(std::string_view short_name) const {
        for (const auto& entry : names_) {
            if (entry.second == short_name) {
                return true;
            }
        }
        return false;
    }

    std::optional<std::uint32_t> KeyOf(std::string_view short_name) const {
        for (const auto& entry : names_) {
            if (entry.second == short_name) {
                return entry.first;
            }
        }
        return std::nullopt;
    }

    bool empty() const { return names_.empty(); }

    // Every registered name, so a session can describe its routes before the
    // first request arrives.
    const std::unordered_map<std::uint32_t, std::string>& Names() const {
        return names_;
    }

    // Sorted, comma separated list for configuration error messages.
    std::string Describe() const {
        std::vector<std::string_view> names;
        names.reserve(names_.size());
        for (const auto& entry : names_) {
            names.push_back(entry.second);
        }
        std::sort(names.begin(), names.end());
        std::string out;
        for (const auto& name : names) {
            if (!out.empty()) {
                out.append(", ");
            }
            out.append(name);
        }
        return out;
    }

   private:
    std::unordered_map<std::uint32_t, std::string> names_;
};

}  // namespace mooncake::traffic
