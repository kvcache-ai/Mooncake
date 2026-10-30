// Copyright 2026 KVCache.AI
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

#include <transfer_engine.h>

#if defined(MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL) != \
    MC_EXPECT_ADAPTIVE_CONTROL
#error "Installed headers do not match the library build configuration"
#endif

struct SliceCacheConsumer : mooncake::Transport {
    using Transport::getSliceCache;
};

int main() {
    auto &cache = SliceCacheConsumer::getSliceCache();
    auto *slice = cache.allocate();
    slice->length = 64;
    cache.deallocate(slice);
    return 0;
}
