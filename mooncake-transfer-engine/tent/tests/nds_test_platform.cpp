// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
// Linker-wrapped platform probe for CPU-only NDS contract tests.
#include <cstdlib>
#include <cstring>
#include "nds_test_backend.h"
#include "tent/runtime/platform.h"

namespace {
using namespace mooncake::tent;
class TestNpuPlatform : public Platform {
   public:
    Status probe(std::vector<Topology::NicEntry>&,
                 std::vector<Topology::MemEntry>& memories) override {
        Topology::MemEntry host{}, device{};
        host.name = "cpu:0";
        host.type = Topology::MEM_HOST;
        device.name = "npu:0";
        device.type = Topology::MEM_ASCEND;
        memories = {host, device};
        return Status::OK();
    }
    Status allocate(void** ptr, size_t size, MemoryOptions&) override {
        return posix_memalign(ptr, 4096, size) == 0
                   ? Status::OK()
                   : Status::InternalError("Test allocation failed");
    }
    Status free(void* ptr, size_t) override {
        std::free(ptr);
        return Status::OK();
    }
    Status copy(void* dst, void* src, size_t size) override {
        std::memcpy(dst, src, size);
        return Status::OK();
    }
    MemoryType getMemoryType(void* ptr) override {
        const auto addr = reinterpret_cast<uint64_t>(ptr);
        const auto& state = nds_test::state;
        // AscendPlatform currently uses the common device memory category.
        return addr >= state.npu_base && addr - state.npu_base < state.npu_size
                   ? MTYPE_CUDA
                   : MTYPE_CPU;
    }
    const std::vector<RangeLocation> getLocation(void* ptr, size_t len,
                                                 bool) override {
        return {{reinterpret_cast<uint64_t>(ptr), len,
                 getMemoryType(ptr) == MTYPE_CUDA ? "npu:0" : "cpu:0"}};
    }
    const std::string type() const override { return "ascend"; }
};
}  // namespace

extern "C" mooncake::tent::Platform&
__wrap__ZN8mooncake4tent8Platform9getLoaderESt10shared_ptrINS0_6ConfigEE(
    std::shared_ptr<mooncake::tent::Config>) {
    static TestNpuPlatform platform;
    return platform;
}
