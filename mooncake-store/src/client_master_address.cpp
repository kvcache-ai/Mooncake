#include "client_master_address.h"

#include <charconv>
#include <cctype>
#include <string_view>

namespace mooncake {
namespace {

std::string_view TrimEndpoint(std::string_view value) {
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.front()))) {
        value.remove_prefix(1);
    }
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.back()))) {
        value.remove_suffix(1);
    }
    return value;
}

bool IsHostCharacter(char character) {
    const auto value = static_cast<unsigned char>(character);
    return std::isalnum(value) || character == '.' || character == '-' ||
           character == '_' || character == ':' || character == '%';
}

bool ParseEtcdEndpoint(std::string_view endpoint) {
    endpoint = TrimEndpoint(endpoint);
    if (endpoint.starts_with("http://")) {
        endpoint.remove_prefix(std::string_view("http://").size());
    } else if (endpoint.starts_with("https://")) {
        endpoint.remove_prefix(std::string_view("https://").size());
    }
    if (endpoint.empty() ||
        endpoint.find_first_of("/?#") != std::string_view::npos) {
        return false;
    }

    std::string_view host;
    std::string_view port_text;
    if (endpoint.front() == '[') {
        const auto close = endpoint.find(']');
        if (close <= 1 || close + 1 >= endpoint.size() ||
            endpoint[close + 1] != ':') {
            return false;
        }
        host = endpoint.substr(1, close - 1);
        port_text = endpoint.substr(close + 2);
    } else {
        const auto colon = endpoint.rfind(':');
        if (colon == std::string_view::npos || colon == 0 ||
            endpoint.find(':') != colon) {
            return false;
        }
        host = endpoint.substr(0, colon);
        port_text = endpoint.substr(colon + 1);
    }
    if (host.empty() || port_text.empty()) return false;
    for (const char character : host) {
        if (!IsHostCharacter(character)) return false;
    }

    unsigned int parsed_port = 0;
    const auto [end, error] = std::from_chars(
        port_text.data(), port_text.data() + port_text.size(), parsed_port);
    if (error != std::errc{} || end != port_text.data() + port_text.size() ||
        parsed_port == 0 || parsed_port > 65535) {
        return false;
    }
    return true;
}

bool ValidateEtcdEndpoints(std::string_view endpoints) {
    size_t count = 0;
    size_t start = 0;
    while (start <= endpoints.size()) {
        const auto end = endpoints.find_first_of(";,", start);
        const auto length = end == std::string_view::npos
                                ? endpoints.size() - start
                                : end - start;
        if (!ParseEtcdEndpoint(endpoints.substr(start, length))) {
            return false;
        }
        ++count;
        if (end == std::string_view::npos) break;
        start = end + 1;
    }
    return count > 0;
}

tl::expected<std::optional<ha::HABackendSpec>, ErrorCode> MakeEtcdSpec(
    const std::string& connstring) {
    if (!ValidateEtcdEndpoints(connstring)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto availability =
        ha::ValidateHABackendAvailability(ha::HABackendType::ETCD);
    if (availability != ErrorCode::OK) {
        return tl::make_unexpected(availability);
    }
    return std::optional<ha::HABackendSpec>{ha::HABackendSpec{
        .type = ha::HABackendType::ETCD,
        .connstring = connstring,
        .cluster_namespace = "",
    }};
}

}  // namespace

tl::expected<std::optional<ha::HABackendSpec>, ErrorCode> ParseHABackendSpec(
    const std::string& master_server_entry) {
    const std::string_view entry = TrimEndpoint(master_server_entry);
    const auto delimiter_pos = entry.find("://");
    if (delimiter_pos != std::string_view::npos) {
        const auto backend_name = entry.substr(0, delimiter_pos);
        auto backend_type = ha::ParseHABackendType(backend_name);
        if (!backend_type.has_value()) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto connstring = std::string(entry.substr(delimiter_pos + 3));
        if (backend_type.value() == ha::HABackendType::ETCD) {
            return MakeEtcdSpec(connstring);
        }

        auto availability =
            ha::ValidateHABackendAvailability(backend_type.value());
        if (availability != ErrorCode::OK) {
            return tl::make_unexpected(availability);
        }
        return std::optional<ha::HABackendSpec>{ha::HABackendSpec{
            .type = backend_type.value(),
            .connstring = std::move(connstring),
            .cluster_namespace = "",
        }};
    }

    if (entry.find_first_of(";,") != std::string_view::npos) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return std::optional<ha::HABackendSpec>{std::nullopt};
}

}  // namespace mooncake
