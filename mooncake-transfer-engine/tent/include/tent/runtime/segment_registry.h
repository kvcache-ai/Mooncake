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

#ifndef SEGMENT_REGISTRY_H
#define SEGMENT_REGISTRY_H

#include <glog/logging.h>
#include <netdb.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <variant>

#include "tent/runtime/segment.h"

namespace mooncake {
namespace tent {
class SegmentRegistry {
   public:
    SegmentRegistry() {}

    virtual ~SegmentRegistry() {}

    SegmentRegistry(const SegmentRegistry &) = delete;
    SegmentRegistry &operator=(const SegmentRegistry &) = delete;

   public:
    virtual Status getSegmentDesc(SegmentDescRef &desc,
                                  const std::string &segment_name) = 0;

    virtual Status putSegmentDesc(SegmentDescRef &desc) = 0;

    virtual Status deleteSegmentDesc(const std::string &segment_name) = 0;
};

class CentralSegmentRegistry : public SegmentRegistry {
   public:
    CentralSegmentRegistry(const std::string &type, const std::string &servers);

    CentralSegmentRegistry(const std::string &type, const std::string &servers,
                           const std::string &password, uint8_t db_index);

    virtual ~CentralSegmentRegistry() {}

   public:
    virtual Status getSegmentDesc(SegmentDescRef &desc,
                                  const std::string &segment_name);

    virtual Status putSegmentDesc(SegmentDescRef &desc);

    virtual Status deleteSegmentDesc(const std::string &segment_name);

   private:
    std::shared_ptr<MetaStore> plugin_;
};

class PeerSegmentRegistry : public SegmentRegistry {
   public:
    PeerSegmentRegistry() {}

    virtual ~PeerSegmentRegistry() {}

   public:
    virtual Status getSegmentDesc(SegmentDescRef &desc,
                                  const std::string &segment_name);

    virtual Status putSegmentDesc(SegmentDescRef &desc) {
        return Status::OK();  // no operation in purpose
    }

    virtual Status deleteSegmentDesc(const std::string &segment_name) {
        return Status::OK();  // no operation in purpose
    }
};

}  // namespace tent
}  // namespace mooncake

#endif  // SEGMENT_REGISTRY_H