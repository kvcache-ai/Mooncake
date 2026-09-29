#include "oss_adapter_config.h"

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>

#include "environ.h"
#include "client_environment_variables.h"

namespace mooncake {
namespace {

std::string ReadPrimaryOrAlias(const Environ& env,
                               const EnvironmentVariable<std::string>& primary,
                               const EnvironmentVariable<std::string>& alias) {
    const auto alias_value = env.GetTyped(alias);
    return env.GetTyped(primary).value_or(alias_value.value_or(""));
}

}  // namespace

tl::expected<OssAdapterConfig, ErrorCode> OssAdapterConfig::FromEnvironment(
    const Environ& env) {
    using Variables = ClientEnvironmentVariables::Offload::Oss;
    OssAdapterConfig config;

    config.endpoint = ReadPrimaryOrAlias(env, Variables::MOONCAKE_OSS_ENDPOINT,
                                         Variables::OSS_ENDPOINT);
    config.bucket = ReadPrimaryOrAlias(env, Variables::MOONCAKE_OSS_BUCKET,
                                       Variables::OSS_BUCKET);
    config.region = ReadPrimaryOrAlias(env, Variables::MOONCAKE_OSS_REGION,
                                       Variables::OSS_REGION);
    config.access_key_id =
        ReadPrimaryOrAlias(env, Variables::MOONCAKE_OSS_ACCESS_KEY_ID,
                           Variables::OSS_ACCESS_KEY_ID);
    config.access_key_secret =
        ReadPrimaryOrAlias(env, Variables::MOONCAKE_OSS_ACCESS_KEY_SECRET,
                           Variables::OSS_ACCESS_KEY_SECRET);
    config.security_token =
        ReadPrimaryOrAlias(env, Variables::MOONCAKE_OSS_SECURITY_TOKEN,
                           Variables::OSS_SESSION_TOKEN);
    boost::algorithm::trim(config.security_token);
    config.path_style =
        env.GetTypedOr(Variables::MOONCAKE_OSS_PATH_STYLE, false);
    config.anonymous = env.GetTypedOr(Variables::MOONCAKE_OSS_ANONYMOUS, false);
    config.max_connections =
        std::max(1, env.GetTypedOr(Variables::MOONCAKE_OSS_MAX_CONNECTIONS,
                                   config.max_connections));
    config.receive_buffer_size =
        std::clamp(env.GetTypedOr(Variables::MOONCAKE_OSS_RECEIVE_BUFFER_SIZE,
                                  config.receive_buffer_size),
                   16 * 1024, 10 * 1024 * 1024);
    config.upload_buffer_size =
        std::clamp(env.GetTypedOr(Variables::MOONCAKE_OSS_UPLOAD_BUFFER_SIZE,
                                  config.upload_buffer_size),
                   16 * 1024, 2 * 1024 * 1024);

    while (!config.endpoint.empty() && config.endpoint.back() == '/') {
        config.endpoint.pop_back();
    }

    if (config.endpoint.empty() || config.bucket.empty() ||
        config.region.empty()) {
        LOG(ERROR) << "OSS requires MOONCAKE_OSS_ENDPOINT, "
                      "MOONCAKE_OSS_BUCKET and MOONCAKE_OSS_REGION";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!config.anonymous &&
        (config.access_key_id.empty() || config.access_key_secret.empty())) {
        LOG(ERROR)
            << "OSS credentials are missing; set MOONCAKE_OSS_ACCESS_KEY_ID "
               "and MOONCAKE_OSS_ACCESS_KEY_SECRET";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return config;
}

}  // namespace mooncake
