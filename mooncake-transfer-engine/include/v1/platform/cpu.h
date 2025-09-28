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

#ifndef CUDA_H
#define CUDA_H

#include "v1/runtime/platform.h"
#include "v1/common/config.h"

namespace mooncake {
namespace v1 {

class CpuPlatform : public Platform {
   public:
    CpuPlatform(std::shared_ptr<ConfigManager> config)
        : conf(std::move(config)) {}

    virtual Status probe(std::vector<Topology::NicEntry> &nic_list,
                         std::vector<Topology::MemEntry> &mem_list);

    virtual Status allocate(void **pptr, size_t size, MemoryOptions &options);

    virtual Status free(void *ptr, size_t size);

    virtual Status copy(void *dst, void *src, size_t length);

    virtual const std::vector<MemoryLocationEntry> getLocation(void *start,
                                                               size_t len);

   private:
    std::shared_ptr<ConfigManager> conf;
};

}  // namespace v1
}  // namespace mooncake

#endif  // CUDA_H