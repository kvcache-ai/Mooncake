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

#include "tent/runtime/platform.h"
#include "tent/common/config.h"

namespace mooncake {
namespace tent {

class CudaPlatform : public Platform {
   public:
    CudaPlatform(std::shared_ptr<Config> config) : conf(std::move(config)) {}

    virtual ~CudaPlatform() {}

    virtual Status probe(std::vector<Topology::NicEntry> &nic_list,
                         std::vector<Topology::MemEntry> &mem_list);

    virtual Status allocate(void **pptr, size_t size, MemoryOptions &options);

    virtual Status free(void *ptr, size_t size);

    virtual Status copy(void *dst, void *src, size_t length);

    virtual MemoryType getMemoryType(void *addr);

    virtual const std::vector<RangeLocation> getLocation(void *start,
                                                         size_t len);

    virtual const std::string type() const { return "cuda"; }

   private:
    std::shared_ptr<Config> conf;
};

}  // namespace tent
}  // namespace mooncake

#endif  // CUDA_H