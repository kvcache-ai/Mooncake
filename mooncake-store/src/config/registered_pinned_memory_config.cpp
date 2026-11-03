#include "registered_pinned_memory_config.h"

#include <glog/logging.h>

#include "ascii_string.h"
#include "environ.h"
#include "environment_variables.h"
#include "integer_parser.h"

namespace mooncake {

RegisteredPinnedMemoryConfig RegisteredPinnedMemoryConfig::FromEnvironment() {
    RegisteredPinnedMemoryConfig config;
    using Variables = RegisteredPinnedMemoryEnvironmentVariables;

    const auto raw_value =
        Environ::Read(Variables::MC_STORE_PIN_MEMORY_MAX_BYTES);
    if (!raw_value.has_value() || raw_value->empty()) {
        return config;
    }

    const auto limit =
        TryParseInteger<uint64_t>(TrimAsciiWhitespace(*raw_value));
    if (!limit.has_value()) {
        LOG(WARNING) << "Invalid MC_STORE_PIN_MEMORY_MAX_BYTES='" << *raw_value
                     << "', disabling Store segment pinning";
        return config;
    }

    config.max_bytes = *limit;
    return config;
}

}  // namespace mooncake
