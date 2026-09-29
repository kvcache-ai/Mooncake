#include "hugepage_config.h"

#include <string>

#include <glog/logging.h>

#include "common/byte_size.h"
#include "environ.h"
#include "client_environment_variables.h"

namespace mooncake {
namespace {

constexpr size_t kSize2Mb = 2ULL * 1024 * 1024;
constexpr size_t kSize512Mb = 512ULL * 1024 * 1024;
constexpr size_t kSize1Gb = 1024ULL * 1024 * 1024;

}  // namespace

bool HugepageConfig::IsEnabledFromEnvironment(const Environ& env) {
    using Variables = ClientEnvironmentVariables::Hugepage;
    return env.GetTyped(Variables::MC_STORE_USE_HUGEPAGE).has_value();
}

HugepageConfig HugepageConfig::FromEnvironment(const Environ& env) {
    HugepageConfig config;
    if (!IsEnabledFromEnvironment(env)) {
        return config;
    }

    config.enabled = true;
    config.page_size = kSize2Mb;

    using Variables = ClientEnvironmentVariables::Hugepage;
    const auto raw_size = env.GetTyped(Variables::MC_STORE_HUGEPAGE_SIZE);
    if (!raw_size.has_value()) {
        return config;
    }

    const size_t parsed_size = string_to_byte_size(*raw_size);
    if (parsed_size == kSize2Mb || parsed_size == kSize512Mb ||
        parsed_size == kSize1Gb) {
        config.page_size = parsed_size;
    } else {
        LOG(WARNING) << "Invalid MC_STORE_HUGEPAGE_SIZE='" << *raw_size
                     << "'. Supported: 2MB, 512MB, 1GB. Fallback to 2MB.";
    }
    return config;
}

}  // namespace mooncake
