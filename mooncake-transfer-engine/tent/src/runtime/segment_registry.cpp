// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/runtime/segment_registry.h"

#include <cassert>
#include <exception>
#include <mutex>
#include <set>

#include "tent/common/status.h"
#include "tent/common/utils/os.h"
#include "tent/runtime/control_plane.h"

namespace mooncake {
namespace tent {

static inline std::string getFullMetadataKey(const std::string &segment_name) {
    const static std::string kCommonKeyPrefix = "mooncake/tent/";
    return kCommonKeyPrefix + segment_name;
}

// TENT publication and ownership-checked deletion are process-serialized so a
// candidate handoff cannot interleave with the old instance's cleanup. The
// remote backends do not provide a common compare-and-delete primitive, so
// this is deliberately not presented as a distributed atomic operation.
std::mutex &registryIoMutex() {
    static std::mutex mutex;
    return mutex;
}

CentralSegmentRegistry::CentralSegmentRegistry(const std::string &type,
                                               const std::string &servers) {
    plugin_ = MetaStore::Create(type, servers);
}

Status CentralSegmentRegistry::getSegmentDesc(SegmentDescRef &desc,
                                              const std::string &segment_name) {
    if (!plugin_)
        return Status::MetadataError(
            "Central metadata store not started" LOC_MARK);
    std::string jstr;
    desc = nullptr;

    auto status = plugin_->get(getFullMetadataKey(segment_name), jstr);
    if (!status.ok()) return status;
    desc = std::make_shared<SegmentDesc>();
    try {
        *desc = json::parse(jstr).get<SegmentDesc>();
    } catch (const std::exception &error) {
        desc = nullptr;
        return Status::MetadataError("Failed to parse segment descriptor " +
                                     segment_name + ": " + error.what() +
                                     LOC_MARK);
    }
    return Status::OK();
}

Status CentralSegmentRegistry::putSegmentDesc(SegmentDescRef &desc) {
    if (!plugin_)
        return Status::MetadataError(
            "Central metadata store not started" LOC_MARK);
    json j = *desc;
    std::lock_guard<std::mutex> lock(registryIoMutex());
    return plugin_->set(getFullMetadataKey(desc->name), j.dump());
}

Status CentralSegmentRegistry::deleteSegmentDesc(
    const std::string &segment_name) {
    if (!plugin_)
        return Status::MetadataError(
            "Central metadata store not started" LOC_MARK);
    return plugin_->remove(getFullMetadataKey(segment_name));
}

Status CentralSegmentRegistry::deleteSegmentDescIfOwned(
    const std::string &segment_name, const SegmentDescRef &expected) {
    if (!plugin_)
        return Status::MetadataError(
            "Central metadata store not started" LOC_MARK);
    if (!expected || expected->owner_id.empty())
        return Status::InvalidArgument(
            "Publication owner is required" LOC_MARK);

    std::lock_guard<std::mutex> lock(registryIoMutex());
    SegmentDescRef current;
    auto status = getSegmentDesc(current, segment_name);
    if (status.IsInvalidEntry()) return Status::OK();
    if (!status.ok()) return status;
    if (current->owner_id != expected->owner_id) {
        if (current->owner_id.empty()) {
            json current_legacy = *current;
            json expected_legacy = *expected;
            current_legacy.erase("owner_id");
            expected_legacy.erase("owner_id");
            if (current_legacy == expected_legacy) {
                return plugin_->remove(getFullMetadataKey(segment_name));
            }
        }
        LOG(INFO) << "Skipping deletion of segment " << segment_name
                  << ": the registry belongs to a different publication";
        return Status::OK();
    }
    return plugin_->remove(getFullMetadataKey(segment_name));
}

Status PeerSegmentRegistry::getSegmentDesc(SegmentDescRef &desc,
                                           const std::string &segment_name) {
    std::string response;
    if (segment_name.empty())
        return Status::InvalidArgument("Empty segment name" LOC_MARK);
    CHECK_STATUS(ControlClient::getSegmentDesc(segment_name, response));
    if (response.empty()) {
        return Status::InvalidEntry(std::string("Segment ") + segment_name +
                                    "not found" + LOC_MARK);
    }
    desc = std::make_shared<SegmentDesc>();
    try {
        *desc = json::parse(response).get<SegmentDesc>();
    } catch (const std::exception &error) {
        desc = nullptr;
        return Status::MetadataError("Failed to parse segment descriptor " +
                                     segment_name + ": " + error.what() +
                                     LOC_MARK);
    }
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
