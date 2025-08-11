// Copyright 2025 KVCache.AI
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

#ifndef METADATA_PLUGIN_H
#define METADATA_PLUGIN_H

#include <memory>
#include <string>

#include "v1/common/status.h"

namespace mooncake {
namespace v1 {
struct MetadataPlugin {
    static std::shared_ptr<MetadataPlugin> Create(
        const std::string &type, const std::string &servers);

    MetadataPlugin() {}

    virtual ~MetadataPlugin() {}

    virtual Status connect(const std::string &endpoint) = 0;

    virtual Status get(const std::string &key, std::string &value) = 0;

    virtual Status set(const std::string &key, const std::string &value) = 0;

    virtual Status remove(const std::string &key) = 0;
};
}  // namespace v1
}  // namespace mooncake

#endif  // METADATA_PLUGIN_H