#include "cxl_segment_config.h"

#include "environ.h"
#include "environment_variables.h"
#include "integer_parser.h"

namespace mooncake {

CxlSegmentConfig CxlSegmentConfig::FromEnvironment() {
    CxlSegmentConfig config;
    using Variables = CxlSegmentEnvironmentVariables;

    const auto raw_value = Environ::Read(Variables::MC_CXL_DEV_SIZE);
    if (!raw_value.has_value()) {
        return config;
    }

    config.device_size =
        TryParseInteger<size_t>(*raw_value, {.trim_ascii_whitespace = true,
                                             .allow_leading_plus = true})
            .value_or(0);
    return config;
}

}  // namespace mooncake
