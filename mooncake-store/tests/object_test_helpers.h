// object_test_helpers.h
//
// Builders shared by the per-object metadata suites.

#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "object_entry.h"
#include "object_metadata.h"

namespace mooncake {
namespace test {

// A minimal 128 B, replica-less envelope. These suites exercise the object
// model, not replica validity, so nothing here sets a real replica. The write
// time is fixed rather than read from the clock so a suite that ages a lease
// cannot pick up a moving value.
inline std::unique_ptr<ObjectMetadata> MakeObjectMetadata(
    const std::string& user_key, const std::string& group_id = {}) {
    constexpr auto kWriteTime =
        std::chrono::system_clock::time_point(std::chrono::seconds(1));
    return std::make_unique<ObjectMetadata>(
        UUID{1, 2}, kWriteTime, 128, std::vector<Replica>{}, std::nullopt,
        false, ObjectDataType::UNKNOWN, group_id, TenantId(), user_key);
}

// The same envelope inside the per-object shell the route stores.
inline std::shared_ptr<ObjectEntry> MakeObjectEntry(
    const std::string& key, const std::string& group_id = {}) {
    return std::make_shared<ObjectEntry>(MakeObjectMetadata(key, group_id));
}

}  // namespace test
}  // namespace mooncake
