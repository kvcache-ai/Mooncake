#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "client_liveness.h"
#include "types.h"

namespace mooncake {

struct PreparedSegmentOffboarding {
    UUID segment_id;
    std::string segment_name;
    size_t metrics_dec_capacity{0};
    bool committed{false};
    bool http_cleanup_submitted{false};
};

enum class ClientOffboardingStage {
    PREPARING,
    READY_FOR_METADATA_CLEANUP,
    COMMITTING,
    COMPLETE,
};

struct ClientOffboardingJob {
    UUID client_id;
    std::shared_ptr<ClientLivenessRecord> record;
    std::vector<PreparedSegmentOffboarding> segments;
    ClientOffboardingStage stage{ClientOffboardingStage::PREPARING};
    bool metadata_cleaned{false};
    bool local_disk_unmounted{false};
    uint32_t attempts{0};
    std::chrono::steady_clock::time_point started_at;
};

}  // namespace mooncake
