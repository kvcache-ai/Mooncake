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

CentralSegmentRegistry::CentralSegmentRegistry(const std::string &type,
                                               const std::string &servers) {
    plugin_ = MetaStore::Create(type, servers);
}

CentralSegmentRegistry::CentralSegmentRegistry(const std::string &type,
                                               const std::string &servers,
                                               const std::string &password,
                                               uint8_t db_index) {
    plugin_ = MetaStore::Create(type, servers, password, db_index);
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
    *desc = json::parse(jstr).get<SegmentDesc>();
    return Status::OK();
}

Status CentralSegmentRegistry::putSegmentDesc(SegmentDescRef &desc) {
    if (!plugin_)
        return Status::MetadataError(
            "Central metadata store not started" LOC_MARK);
    json j = *desc;
    return plugin_->set(getFullMetadataKey(desc->name), j.dump());
}

Status CentralSegmentRegistry::deleteSegmentDesc(
    const std::string &segment_name) {
    if (!plugin_)
        return Status::MetadataError(
            "Central metadata store not started" LOC_MARK);
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
    *desc = json::parse(response).get<SegmentDesc>();
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
