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

#ifndef METASTORE_H
#define METASTORE_H

#include <memory>
#include <string>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {
struct MetaStore {
    static std::shared_ptr<MetaStore> Create(const std::string &type,
                                             const std::string &servers);

    static std::shared_ptr<MetaStore> Create(const std::string &type,
                                             const std::string &servers,
                                             const std::string &password,
                                             uint8_t db_index);

    MetaStore() {}

    virtual ~MetaStore() {}

    virtual Status connect(const std::string &endpoint) = 0;

    virtual Status get(const std::string &key, std::string &value) = 0;

    virtual Status set(const std::string &key, const std::string &value) = 0;

    virtual Status remove(const std::string &key) = 0;
};
}  // namespace tent
}  // namespace mooncake

#endif  // METASTORE_H