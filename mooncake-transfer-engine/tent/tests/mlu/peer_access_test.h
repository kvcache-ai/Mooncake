// Copyright 2026 KVCache.AI
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

#ifndef TENT_MLU_PEER_ACCESS_TEST_H_
#define TENT_MLU_PEER_ACCESS_TEST_H_

#include <cn_api.h>
#include <cnrt.h>

#include <atomic>

namespace mlu_test {
inline std::atomic<CNaddr> inaccessible_base{0};
inline std::atomic<size_t> inaccessible_size{0};
inline std::atomic<int> device_ordinal_queries{0};
inline std::atomic<int> peer_access_queries{0};

// Model an unreachable second device even on single-device test hosts. Only
// peer capability queries change; allocations, queues and H2D/D2H stay real.
class InaccessiblePeer {
   public:
    InaccessiblePeer(void *ptr, size_t size) {
        inaccessible_size = size;
        inaccessible_base = reinterpret_cast<CNaddr>(ptr);
    }
    ~InaccessiblePeer() { inaccessible_base = 0; }
};
}  // namespace mlu_test

extern "C" CNresult __real_cnGetMemAttribute(
    void *, decltype(CN_MEM_ATTRIBUTE_DEVICE_ORDINAL), CNaddr);
extern "C" CNresult __wrap_cnGetMemAttribute(
    void *value, decltype(CN_MEM_ATTRIBUTE_DEVICE_ORDINAL) attr, CNaddr ptr) {
    if (attr == CN_MEM_ATTRIBUTE_DEVICE_ORDINAL)
        ++mlu_test::device_ordinal_queries;
    auto result = __real_cnGetMemAttribute(value, attr, ptr);
    const auto base = mlu_test::inaccessible_base.load();
    if (result == CN_SUCCESS && attr == CN_MEM_ATTRIBUTE_DEVICE_ORDINAL &&
        base && ptr >= base && ptr - base < mlu_test::inaccessible_size.load())
        ++*static_cast<int *>(value);
    return result;
}

extern "C" cnrtRet_t __real_cnrtGetPeerAccessibility(unsigned int *, int, int);
extern "C" cnrtRet_t __wrap_cnrtGetPeerAccessibility(unsigned int *accessible,
                                                     int src, int dst) {
    ++mlu_test::peer_access_queries;
    if (mlu_test::inaccessible_base.load()) {
        *accessible = 0;
        return cnrtSuccess;
    }
    return __real_cnrtGetPeerAccessibility(accessible, src, dst);
}

#endif  // TENT_MLU_PEER_ACCESS_TEST_H_
