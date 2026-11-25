#include "config/offload_read_chunk_config.h"

#include <glog/logging.h>

#include <algorithm>

#include "environ.h"
#include "environment_value_parser.h"
#include "environment_variables.h"

namespace mooncake {

OffloadReadChunkConfig OffloadReadChunkConfig::FromEnvironment() {
    using Variables = OffloadReadChunkEnvironmentVariables;
    OffloadReadChunkConfig config;

    if (const auto raw =
            Environ::Read(Variables::MOONCAKE_OFFLOAD_READ_CHUNK_BYTES);
        raw && !raw->empty()) {
        if (const auto value = TryParseEnvironmentValue<uint64_t>(*raw)) {
            config.chunk_bytes = *value;
        } else {
            LOG(WARNING)
                << "Invalid value for MOONCAKE_OFFLOAD_READ_CHUNK_BYTES: "
                << *raw << ", using default " << config.chunk_bytes;
        }
    }

    if (const auto raw =
            Environ::Read(Variables::MOONCAKE_OFFLOAD_READ_CHUNK_CONCURRENCY);
        raw && !raw->empty()) {
        if (const auto value = TryParseEnvironmentValue<size_t>(*raw)) {
            config.chunk_concurrency =
                std::clamp<size_t>(*value, 1, kMaxChunkConcurrency);
            if (config.chunk_concurrency != *value) {
                LOG(WARNING)
                    << "MOONCAKE_OFFLOAD_READ_CHUNK_CONCURRENCY=" << *raw
                    << " is outside 1.." << kMaxChunkConcurrency << ", using "
                    << config.chunk_concurrency;
            }
        } else {
            LOG(WARNING)
                << "Invalid value for MOONCAKE_OFFLOAD_READ_CHUNK_CONCURRENCY: "
                << *raw << ", using default " << config.chunk_concurrency;
        }
    }
    return config;
}

}  // namespace mooncake
