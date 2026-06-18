#pragma once

#include <memory>

#include <json/value.h>

#include "tiered_cache/scheduler/client_scheduler_interface.h"

namespace mooncake {

class TieredBackend;  // Forward declaration

/**
 * @brief Construct the configured client scheduler.
 *
 * Reads config["scheduler"]["type"]:
 *   - absent / "legacy"       -> LegacyClientScheduler (SIMPLE/LRU policies)
 *   - "event_driven"          -> EventDrivenClientScheduler (MultiLRU)
 */
std::unique_ptr<IClientScheduler> MakeClientScheduler(
    TieredBackend* backend, const Json::Value& config);

}  // namespace mooncake
