#include "traffic_config.h"

#include <sys/stat.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string_view>
#include <thread>
#include <utility>

#include <glog/logging.h>
#include <json/json.h>

#include "traffic/route_names.h"
#include "traffic/traffic_admin.h"
#include "traffic/traffic_redaction.h"

namespace mooncake::traffic {

namespace {

constexpr const char* kControlTokenFile = "control.token";

bool ParseSize(const Json::Value& node, const char* name, std::uint64_t* out,
               std::string* error) {
    if (node.isNull()) {
        return true;
    }
    if (!node.isUInt64()) {
        *error = std::string("config ") + name + " must be an integer";
        return false;
    }
    *out = node.asUInt64();
    return true;
}

bool ParseOptions(const Json::Value& root, std::size_t rpc_threads,
                  RecorderConfig* config, std::string* error) {
    static constexpr std::string_view kKnownOptions[] = {
        "output_dir",  "redaction",        "routes",         "control",
        "ring_bytes",  "max_record_bytes", "segment_bytes",  "segment_seconds",
        "block_bytes", "max_total_bytes",  "stop_timeout_ms"};
    for (const auto& key : root.getMemberNames()) {
        const bool known =
            std::find(std::begin(kKnownOptions), std::end(kKnownOptions),
                      std::string_view(key)) != std::end(kKnownOptions);
        if (!known) {
            *error = "unknown configuration key '" + key +
                     "'; a misspelled key would otherwise be ignored";
            return false;
        }
    }
    if (!root["output_dir"].isString() ||
        root["output_dir"].asString().empty()) {
        *error = "output_dir is required";
        return false;
    }
    config->output_dir = root["output_dir"].asString();
    if (root.isMember("redaction")) {
        const Json::Value& redaction = root["redaction"];
        // Falling back to "none" for a non-string value would write request
        // bodies the operator expected to be redacted.
        if (!redaction.isString()) {
            *error = "redaction must be a string";
            return false;
        }
        const auto mode = redaction.asString();
        if (mode == "none") {
            config->redaction_mode = RedactionMode::kNone;
        } else if (mode == "metadata") {
            config->redaction_mode = RedactionMode::kMetadata;
        } else if (mode == "redacted") {
            config->redaction_mode = RedactionMode::kRedacted;
        } else {
            *error = "redaction must be none, metadata, or redacted";
            return false;
        }
    }
    std::string size_error;
    if (!ParseSize(root["ring_bytes"], "ring_bytes", &config->ring_bytes,
                   &size_error) ||
        !ParseSize(root["max_record_bytes"], "max_record_bytes",
                   &config->max_record_bytes, &size_error) ||
        !ParseSize(root["segment_bytes"], "segment_bytes",
                   &config->segment_bytes, &size_error) ||
        !ParseSize(root["segment_seconds"], "segment_seconds",
                   &config->segment_seconds, &size_error) ||
        !ParseSize(root["block_bytes"], "block_bytes", &config->block_bytes,
                   &size_error) ||
        !ParseSize(root["max_total_bytes"], "max_total_bytes",
                   &config->max_total_bytes, &size_error) ||
        !ParseSize(root["stop_timeout_ms"], "stop_timeout_ms",
                   &config->stop_timeout_ms, &size_error)) {
        *error = size_error;
        return false;
    }
    config->ring_shards =
        rpc_threads > 0
            ? static_cast<std::uint32_t>(std::min<std::size_t>(
                  rpc_threads, std::numeric_limits<std::uint32_t>::max()))
            : std::max(1u, std::thread::hardware_concurrency());
    if (config->block_bytes == 0 ||
        config->block_bytes > config->segment_bytes) {
        *error = "block_bytes must be in (0, segment_bytes]";
        return false;
    }
    if (config->max_record_bytes == 0 ||
        config->max_record_bytes > config->block_bytes) {
        *error = "max_record_bytes must be in (0, block_bytes]";
        return false;
    }
    // A block is read back whole by the replay tool, which refuses anything
    // above kMaxBlockBytes; the same bound applies when recording.
    if (config->block_bytes > kMaxBlockBytes) {
        *error =
            "block_bytes must be at most " + std::to_string(kMaxBlockBytes);
        return false;
    }
    if (config->max_total_bytes > 0 &&
        config->max_total_bytes < config->segment_bytes) {
        *error = "max_total_bytes must be >= segment_bytes";
        return false;
    }
    return true;
}

bool ParseControl(const Json::Value& root, RecorderConfig* config,
                  std::string* error) {
    if (root.isMember("control") && !root["control"].isObject()) {
        *error = "control must be an object";
        return false;
    }
    if (root["control"].isObject()) {
        const Json::Value& control = root["control"];
        if (control.isMember("host")) {
            if (!control["host"].isString()) {
                *error = "control.host must be a string";
                return false;
            }
            config->control_host = control["host"].asString();
        }
        if (control.isMember("port")) {
            const Json::Value& port = control["port"];
            const long parsed =
                port.isUInt()
                    ? static_cast<long>(port.asUInt())
                    : (port.isInt() ? static_cast<long>(port.asInt()) : 0);
            if (parsed <= 0 || parsed > 65535) {
                *error = "control.port must be a port number";
                return false;
            }
            config->control_port = static_cast<std::uint16_t>(parsed);
        }
        if (control.isMember("token")) {
            if (!control["token"].isString()) {
                *error = "control.token must be a string";
                return false;
            }
            config->control_token = control["token"].asString();
        }
        if (control.isMember("token_file")) {
            if (!control["token_file"].isString()) {
                *error = "control.token_file must be a string";
                return false;
            }
            config->control_token_file = control["token_file"].asString();
        }
    }
    // The environment override is applied last so that it wins over the
    // configuration file, which is what an operator overriding a deployed
    // configuration expects.
    if (const char* port = std::getenv("MOONCAKE_TRAFFIC_CONTROL_PORT");
        port != nullptr && *port != '\0') {
        char* end = nullptr;
        errno = 0;
        const long parsed = std::strtol(port, &end, 10);
        if (errno != 0 || end == nullptr || *end != '\0' || parsed <= 0 ||
            parsed > 65535) {
            *error = std::string(
                         "MOONCAKE_TRAFFIC_CONTROL_PORT must be a "
                         "port number, got '") +
                     port + "'";
            return false;
        }
        config->control_port = static_cast<std::uint16_t>(parsed);
    }
    if (config->control_host != "127.0.0.1" &&
        config->control_host != "localhost" && config->control_host != "::1") {
        *error = "control.host must be a loopback address";
        return false;
    }
    return true;
}

bool ParseRoutes(const Json::Value& root, const RouteNameRegistry& routes,
                 RecorderConfig* config, std::string* error) {
    if (root.isMember("routes") && !root["routes"].isArray()) {
        *error = "routes must be an array";
        return false;
    }
    if (root["routes"].isArray()) {
        if (root["routes"].empty()) {
            // An empty list would otherwise record every route, which is the
            // opposite of what an explicit allowlist communicates.
            *error =
                "routes must not be empty; omit the key to record every route";
            return false;
        }
        for (const auto& item : root["routes"]) {
            if (!item.isString()) {
                *error = "routes[] must contain strings";
                return false;
            }
            const std::string route = item.asString();
            if (route == "*") {
                if (root["routes"].size() != 1) {
                    *error =
                        "wildcard route cannot be combined with other routes";
                    return false;
                }
                config->allowed_routes.clear();
                config->allowed_route_names.clear();
                continue;
            }
            char* end = nullptr;
            errno = 0;
            const unsigned long key = std::strtoul(route.c_str(), &end, 10);
            if (end != route.c_str() && *end == '\0' && errno != ERANGE &&
                key <= std::numeric_limits<std::uint32_t>::max()) {
                config->allowed_routes.insert(static_cast<std::uint32_t>(key));
            } else {
                const std::string_view short_name = ShortRouteName(route);
                if (routes.empty()) {
                    *error = "route name '" + route +
                             "' cannot be resolved because the caller did not "
                             "register the RPC routes";
                    return false;
                }
                if (!routes.ContainsName(short_name)) {
                    *error = "unknown route name '" + route +
                             "'; registered routes: " + routes.Describe();
                    return false;
                }
                config->allowed_route_names.push_back(std::string(short_name));
            }
        }
    }
    return true;
}

bool ValidateRedacted(const RecorderConfig& config,
                      const RedactionRegistry& redaction,
                      const RouteNameRegistry& routes, std::string* error) {
    if (config.allowed_routes.empty() && config.allowed_route_names.empty()) {
        *error = "redacted mode requires an explicit routes list";
        return false;
    }
    for (const auto route_key : config.allowed_routes) {
        if (!redaction.Contains(route_key)) {
            *error = "no redactor for route " + std::to_string(route_key);
            return false;
        }
    }
    for (const auto& route_name : config.allowed_route_names) {
        const auto route_key = routes.KeyOf(route_name);
        if (!route_key.has_value()) {
            *error = "route name '" + route_name +
                     "' cannot be resolved to a route key";
            return false;
        }
        if (!redaction.Contains(*route_key)) {
            *error = "no redactor for route " + route_name;
            return false;
        }
    }
    return true;
}

// Reads the configured token, or creates the output directory and publishes a
// token there. The configuration is not usable until one of the two exists.
bool ResolveControlToken(RecorderConfig* config, std::string* error) {
    if (config->control_token.empty() && !config->control_token_file.empty()) {
        auto token =
            ReadControlToken(config->control_token_file, TokenFormat::kAny);
        if (!token.has_value()) {
            *error =
                "cannot read control.token_file " + config->control_token_file;
            return false;
        }
        config->control_token = std::move(*token);
    }
    if (!config->control_token.empty()) {
        if (!config->control_token_file.empty()) {
            LOG(INFO) << "traffic: control token file "
                      << config->control_token_file;
        }
        return true;
    }
    std::error_code ec;
    const bool created =
        std::filesystem::create_directories(config->output_dir, ec);
    if (ec ||
        (!created && !std::filesystem::is_directory(config->output_dir))) {
        *error = "cannot create output_dir " + config->output_dir;
        return false;
    }
    if (created) {
        ::chmod(config->output_dir.c_str(), 0700);
    }
    const std::filesystem::path token_path =
        std::filesystem::path(config->output_dir) / kControlTokenFile;
    auto published = ReadControlToken(token_path, TokenFormat::kHex);
    if (!published.has_value()) {
        std::error_code token_ec;
        const bool token_exists = std::filesystem::exists(token_path, token_ec);
        if (token_ec || token_exists ||
            !GenerateControlToken(&config->control_token) ||
            !WriteControlToken(token_path, &config->control_token)) {
            *error = "cannot create a secure control token at " +
                     token_path.string();
            return false;
        }
    } else {
        config->control_token = std::move(*published);
    }
    LOG(INFO) << "traffic: control token file " << token_path;
    return true;
}

}  // namespace

bool LoadRecordingConfig(const std::string& path, std::size_t rpc_threads,
                         const RedactionRegistry& redaction,
                         const RouteNameRegistry& routes,
                         RecorderConfig* config, std::string* error) {
    std::ifstream input(path);
    if (!input) {
        *error = "cannot open config " + path;
        return false;
    }
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errors;
    if (!Json::parseFromStream(builder, input, &root, &errors)) {
        *error = "invalid config: " + errors;
        return false;
    }
    if (!root.isObject()) {
        *error = "config root must be an object";
        return false;
    }
    if (!ParseOptions(root, rpc_threads, config, error) ||
        !ParseControl(root, config, error) ||
        !ParseRoutes(root, routes, config, error)) {
        return false;
    }
    if (config->redaction_mode == RedactionMode::kRedacted &&
        !ValidateRedacted(*config, redaction, routes, error)) {
        return false;
    }
    return ResolveControlToken(config, error);
}

}  // namespace mooncake::traffic
