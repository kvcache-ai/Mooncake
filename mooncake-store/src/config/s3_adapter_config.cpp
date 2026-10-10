#include "s3_adapter_config.h"

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <cctype>
#include <glog/logging.h>
#include <string_view>

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {
namespace {

std::string ReadPrimaryOrAlias(const EnvironmentVariable<std::string>& primary,
                               const EnvironmentVariable<std::string>& alias) {
    const auto alias_value = Environ::Read(alias);
    return Environ::Read(primary).value_or(alias_value.value_or(""));
}

// Host part of "scheme://host[:port]", without the port.
std::string_view EndpointHost(std::string_view endpoint) {
    endpoint.remove_prefix(endpoint.find("://") + 3);
    if (endpoint.starts_with('[')) {
        const size_t close = endpoint.find(']');
        return endpoint.substr(
            0, close == std::string_view::npos ? endpoint.size() : close + 1);
    }
    return endpoint.substr(0, endpoint.find(':'));
}

// Virtual-hosted addressing prepends the bucket to the host name, which only
// resolves for DNS names. IP literals and localhost need path-style requests.
bool NeedsPathStyle(std::string_view host) {
    if (host.starts_with('[')) return true;  // IPv6 literal
    if (boost::algorithm::iequals(host, "localhost")) return true;
    return !host.empty() && std::all_of(host.begin(), host.end(), [](char c) {
        return std::isdigit(static_cast<unsigned char>(c)) || c == '.';
    });
}

}  // namespace

tl::expected<S3AdapterConfig, ErrorCode> S3AdapterConfig::FromEnvironment() {
    using Variables = S3AdapterEnvironmentVariables;
    S3AdapterConfig config;

    config.endpoint = ReadPrimaryOrAlias(Variables::MOONCAKE_S3_ENDPOINT,
                                         Variables::AWS_ENDPOINT_URL);
    config.bucket = Environ::Read(Variables::MOONCAKE_S3_BUCKET).value_or("");

    // Region: MOONCAKE_S3_REGION, then AWS_REGION, then AWS_DEFAULT_REGION
    // (the order the AWS CLI and SDKs use), then us-east-1.
    if (auto region = Environ::Read(Variables::MOONCAKE_S3_REGION)) {
        config.region = *region;
    } else if (auto aws_region = Environ::Read(Variables::AWS_REGION)) {
        config.region = *aws_region;
    } else if (auto aws_default_region =
                   Environ::Read(Variables::AWS_DEFAULT_REGION)) {
        config.region = *aws_default_region;
    } else {
        LOG(WARNING) << "S3 region not set (MOONCAKE_S3_REGION, AWS_REGION or "
                        "AWS_DEFAULT_REGION); signing for "
                     << config.region
                     << ". Requests to a bucket in another AWS region will "
                        "fail.";
    }

    // Credentials come from one source as a set. If either MOONCAKE_S3_ key
    // variable is set, the key, secret and session token are all read from
    // MOONCAKE_S3_*; otherwise all three come from AWS_*. Mixing sources would
    // let an unrelated AWS_SESSION_TOKEN (for example from aws sso) ride along
    // with static keys and fail every request.
    const bool mooncake_credentials =
        Environ::Read(Variables::MOONCAKE_S3_ACCESS_KEY_ID).has_value() ||
        Environ::Read(Variables::MOONCAKE_S3_SECRET_ACCESS_KEY).has_value();
    if (mooncake_credentials) {
        config.access_key_id =
            Environ::Read(Variables::MOONCAKE_S3_ACCESS_KEY_ID).value_or("");
        config.access_key_secret =
            Environ::Read(Variables::MOONCAKE_S3_SECRET_ACCESS_KEY)
                .value_or("");
        config.security_token =
            Environ::Read(Variables::MOONCAKE_S3_SESSION_TOKEN).value_or("");
    } else {
        config.access_key_id =
            Environ::Read(Variables::AWS_ACCESS_KEY_ID).value_or("");
        config.access_key_secret =
            Environ::Read(Variables::AWS_SECRET_ACCESS_KEY).value_or("");
        config.security_token =
            Environ::Read(Variables::AWS_SESSION_TOKEN).value_or("");
    }
    boost::algorithm::trim(config.security_token);

    config.path_style =
        Environ::ReadOr(Variables::MOONCAKE_S3_PATH_STYLE, false);
    config.anonymous = Environ::ReadOr(Variables::MOONCAKE_S3_ANONYMOUS, false);
    config.max_connections =
        std::max(1, Environ::ReadOr(Variables::MOONCAKE_S3_MAX_CONNECTIONS,
                                    config.max_connections));
    config.receive_buffer_size =
        std::clamp(Environ::ReadOr(Variables::MOONCAKE_S3_RECEIVE_BUFFER_SIZE,
                                   config.receive_buffer_size),
                   16 * 1024, 10 * 1024 * 1024);
    config.upload_buffer_size =
        std::clamp(Environ::ReadOr(Variables::MOONCAKE_S3_UPLOAD_BUFFER_SIZE,
                                   config.upload_buffer_size),
                   16 * 1024, 2 * 1024 * 1024);

    while (!config.endpoint.empty() && config.endpoint.back() == '/') {
        config.endpoint.pop_back();
    }

    if (config.endpoint.empty() || config.bucket.empty() ||
        config.region.empty()) {
        LOG(ERROR) << "S3 requires MOONCAKE_S3_ENDPOINT (or AWS_ENDPOINT_URL), "
                      "MOONCAKE_S3_BUCKET and a non-empty region";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // The scheme decides both the transport and the signed Host header, so it
    // must be explicit.
    if (!boost::algorithm::istarts_with(config.endpoint, "http://") &&
        !boost::algorithm::istarts_with(config.endpoint, "https://")) {
        LOG(ERROR) << "S3 endpoint must start with http:// or https://: "
                   << config.endpoint;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // The adapter appends the bucket and key to the endpoint, so a path here
    // would be sent but not signed.
    if (config.endpoint.find('/', config.endpoint.find("://") + 3) !=
        std::string::npos) {
        LOG(ERROR) << "S3 endpoint must not contain a path: "
                   << config.endpoint;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const std::string_view host = EndpointHost(config.endpoint);
    if (host.empty()) {
        LOG(ERROR) << "S3 endpoint has no host: " << config.endpoint;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!config.path_style && NeedsPathStyle(host)) {
        LOG(WARNING) << "S3 endpoint host " << host
                     << " is an IP address or localhost; using path-style "
                        "addressing (virtual-hosted needs a DNS name)";
        config.path_style = true;
    }
    if (!config.anonymous &&
        (config.access_key_id.empty() || config.access_key_secret.empty())) {
        LOG(ERROR) << "S3 credentials are missing; set "
                      "MOONCAKE_S3_ACCESS_KEY_ID and "
                      "MOONCAKE_S3_SECRET_ACCESS_KEY (or AWS_ACCESS_KEY_ID and "
                      "AWS_SECRET_ACCESS_KEY)";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return config;
}

}  // namespace mooncake
