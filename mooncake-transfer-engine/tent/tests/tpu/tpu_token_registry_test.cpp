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

// Unit tests for the TPU adapter's token registry
// (tent/plugins/tpu/tpu_token_registry.cpp) with a host-memory fake region.
// No TPU hardware, PJRT runtime or buffer provider required.

#include "tpu_token_registry.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake {
namespace tent {
namespace {

// ProxyManager stages in 4 MiB chunks; tests use the same unit so "interior
// pointer" cases look like real chunk #1, #2, ... accesses.
constexpr size_t kChunk = 4ul << 20;

class FakeRegion : public DeviceRegion {
   public:
    FakeRegion(size_t size, int device, int numa = -1)
        : data_(size), device_(device), numa_(numa) {
        for (size_t i = 0; i < size; ++i) data_[i] = (uint8_t)(i % 251);
    }

    size_t size() const override { return data_.size(); }
    int deviceIndex() const override { return device_; }
    int numaNode() const override { return numa_; }

    bool copyToHost(void *host_dst, size_t offset, size_t len) override {
        last_offset_ = offset;
        ++calls_;
        if (fail_) return false;
        std::memcpy(host_dst, data_.data() + offset, len);
        return true;
    }
    bool copyFromHost(const void *host_src, size_t offset,
                      size_t len) override {
        last_offset_ = offset;
        ++calls_;
        if (fail_) return false;
        std::memcpy(data_.data() + offset, host_src, len);
        return true;
    }

    const uint8_t *data() const { return data_.data(); }
    size_t lastOffset() const { return last_offset_; }
    int calls() const { return calls_; }
    void setFail(bool fail) { fail_ = fail; }

   private:
    std::vector<uint8_t> data_;
    int device_;
    int numa_;
    std::atomic<size_t> last_offset_{~size_t{0}};
    std::atomic<int> calls_{0};
    std::atomic<bool> fail_{false};
};

struct Added {
    uintptr_t token;
    FakeRegion *region;  // owned by the registry
};

Added add(TpuTokenRegistry &registry, size_t size, int device, int numa = -1) {
    auto region = std::make_unique<FakeRegion>(size, device, numa);
    FakeRegion *raw = region.get();
    return {registry.add(std::move(region)), raw};
}

void *at(uintptr_t token, size_t offset) {
    return reinterpret_cast<void *>(token + offset);
}

TEST(TpuTokenRegistryTest, ClassifiesBaseAndInteriorButNotOutside) {
    TpuTokenRegistry registry;
    auto r = add(registry, 3 * kChunk, /*device=*/2);
    ASSERT_NE(r.token, 0u);

    EXPECT_TRUE(registry.contains(at(r.token, 0)));
    EXPECT_TRUE(registry.contains(at(r.token, kChunk + 17)));
    EXPECT_TRUE(registry.contains(at(r.token, 3 * kChunk - 1)));
    EXPECT_FALSE(registry.contains(at(r.token, 3 * kChunk)));
    EXPECT_FALSE(registry.contains(reinterpret_cast<void *>(r.token - 1)));
    EXPECT_EQ(registry.deviceIndex(at(r.token, kChunk)), 2);
    EXPECT_EQ(registry.deviceIndex(at(r.token, 3 * kChunk)), -1);
    EXPECT_EQ(registry.regionSize(r.token), 3 * kChunk);
    EXPECT_EQ(registry.regionSize(r.token + 1), 0u);
}

// The case that silently corrupted data before interior pointers were part of
// the adapter contract (#2815).
TEST(TpuTokenRegistryTest, D2HFromInteriorAddressCopiesThatChunk) {
    TpuTokenRegistry registry;
    auto r = add(registry, 3 * kChunk, 0);
    std::vector<uint8_t> staging(kChunk, 0);

    ASSERT_EQ(registry.copyD2H(staging.data(), at(r.token, kChunk), kChunk), 0);
    EXPECT_EQ(r.region->lastOffset(), kChunk);
    EXPECT_EQ(0,
              std::memcmp(staging.data(), r.region->data() + kChunk, kChunk));
}

TEST(TpuTokenRegistryTest, H2DToInteriorAddressWritesThatChunk) {
    TpuTokenRegistry registry;
    auto r = add(registry, 3 * kChunk, 0);
    std::vector<uint8_t> staging(kChunk, 0xAB);

    ASSERT_EQ(registry.copyH2D(at(r.token, 2 * kChunk), staging.data(), kChunk),
              0);
    EXPECT_EQ(r.region->lastOffset(), 2 * kChunk);
    EXPECT_EQ(r.region->data()[2 * kChunk], 0xAB);
    EXPECT_EQ(r.region->data()[3 * kChunk - 1], 0xAB);
    EXPECT_NE(r.region->data()[2 * kChunk - 1], 0xAB);  // previous chunk
}

TEST(TpuTokenRegistryTest, CopyRunningPastTheEndFailsWithoutTouchingDevice) {
    TpuTokenRegistry registry;
    auto r = add(registry, 2 * kChunk, 0);
    std::vector<uint8_t> staging(kChunk + 1);

    EXPECT_EQ(registry.copyD2H(staging.data(), at(r.token, kChunk), kChunk + 1),
              1);
    EXPECT_EQ(registry.copyH2D(at(r.token, kChunk), staging.data(), kChunk + 1),
              1);
    EXPECT_EQ(registry.copyD2H(staging.data(), at(r.token, 0), SIZE_MAX), 1);
    EXPECT_EQ(r.region->calls(), 0);
}

TEST(TpuTokenRegistryTest, UnknownAddressesAndNullArgumentsFail) {
    TpuTokenRegistry registry;
    auto r = add(registry, kChunk, 0);
    uint8_t host[16];
    int not_a_token = 0;

    EXPECT_EQ(registry.copyD2H(host, &not_a_token, sizeof(not_a_token)), 1);
    EXPECT_EQ(registry.copyH2D(&not_a_token, host, sizeof(not_a_token)), 1);
    EXPECT_EQ(registry.copyD2H(nullptr, at(r.token, 0), 1), 1);
    EXPECT_EQ(registry.copyH2D(at(r.token, 0), nullptr, 1), 1);
    EXPECT_FALSE(registry.contains(&not_a_token));
    EXPECT_FALSE(registry.contains(nullptr));
    EXPECT_EQ(r.region->calls(), 0);
}

TEST(TpuTokenRegistryTest, RegionCopyFailureIsReported) {
    TpuTokenRegistry registry;
    auto r = add(registry, kChunk, 0);
    r.region->setFail(true);
    std::vector<uint8_t> staging(kChunk);

    EXPECT_EQ(registry.copyD2H(staging.data(), at(r.token, 0), kChunk), 1);
    EXPECT_EQ(registry.copyH2D(at(r.token, 0), staging.data(), kChunk), 1);
}

// Sizes that are not a page multiple leave slack between the region's end and
// the end of its reservation; that slack must not classify as device memory.
TEST(TpuTokenRegistryTest, ReservationSlackIsNotPartOfTheRegion) {
    TpuTokenRegistry registry;
    auto r = add(registry, 100, 0);
    EXPECT_TRUE(registry.contains(at(r.token, 99)));
    EXPECT_FALSE(registry.contains(at(r.token, 100)));
}

TEST(TpuTokenRegistryTest, TokensOfDifferentRegionsDoNotOverlap) {
    TpuTokenRegistry registry;
    auto a = add(registry, kChunk, 0);
    auto b = add(registry, kChunk, 1);
    ASSERT_NE(a.token, 0u);
    ASSERT_NE(b.token, 0u);
    EXPECT_TRUE(a.token + kChunk <= b.token || b.token + kChunk <= a.token);
    EXPECT_EQ(registry.deviceIndex(at(a.token, 5)), 0);
    EXPECT_EQ(registry.deviceIndex(at(b.token, 5)), 1);
}

TEST(TpuTokenRegistryTest, RemoveDropsRegionButKeepsDevice) {
    TpuTokenRegistry registry;
    auto r = add(registry, kChunk, 3, /*numa=*/1);
    ASSERT_EQ(registry.size(), 1u);

    EXPECT_FALSE(registry.remove(r.token + 1));  // must be the exact token
    EXPECT_TRUE(registry.remove(r.token));
    EXPECT_FALSE(registry.remove(r.token));
    EXPECT_FALSE(registry.contains(at(r.token, 0)));
    EXPECT_EQ(registry.size(), 0u);
    EXPECT_EQ(registry.deviceCount(), 4);
    EXPECT_EQ(registry.deviceNuma(3), 1);
}

TEST(TpuTokenRegistryTest, DeviceTableTracksHighestIndexAndNuma) {
    TpuTokenRegistry registry;
    EXPECT_EQ(registry.deviceCount(), 0);
    add(registry, kChunk, 0, /*numa=*/0);
    add(registry, kChunk, 3, /*numa=*/-1);
    add(registry, kChunk, 3, /*numa=*/1);  // fills in the unknown NUMA node

    EXPECT_EQ(registry.deviceCount(), 4);
    EXPECT_EQ(registry.deviceNuma(0), 0);
    EXPECT_EQ(registry.deviceNuma(1), -1);  // never seen
    EXPECT_EQ(registry.deviceNuma(3), 1);
}

TEST(TpuTokenRegistryTest, RejectsInvalidRegions) {
    TpuTokenRegistry registry;
    EXPECT_EQ(registry.add(nullptr), 0u);
    EXPECT_EQ(registry.add(std::make_unique<FakeRegion>(0, 0)), 0u);
    EXPECT_EQ(registry.add(std::make_unique<FakeRegion>(kChunk, -1)), 0u);
    EXPECT_EQ(registry.size(), 0u);
    EXPECT_EQ(registry.deviceCount(), 0);
}

// TENT may stage several chunks of one region at once while other regions are
// being registered and unregistered.
TEST(TpuTokenRegistryTest, ConcurrentCopiesAndRegistrationChurn) {
    TpuTokenRegistry registry;
    constexpr size_t kChunks = 8;
    auto r = add(registry, kChunks * kChunk, 0);
    std::atomic<bool> stop{false};

    std::thread churn([&] {
        while (!stop.load()) {
            auto other = add(registry, kChunk, 1);
            registry.remove(other.token);
        }
    });

    std::vector<std::thread> copiers;
    std::atomic<int> mismatches{0};
    for (size_t c = 0; c < kChunks; ++c) {
        copiers.emplace_back([&, c] {
            std::vector<uint8_t> staging(kChunk);
            for (int iter = 0; iter < 20; ++iter) {
                if (registry.copyD2H(staging.data(), at(r.token, c * kChunk),
                                     kChunk) != 0 ||
                    std::memcmp(staging.data(), r.region->data() + c * kChunk,
                                kChunk) != 0) {
                    ++mismatches;
                }
            }
        });
    }
    for (auto &t : copiers) t.join();
    stop = true;
    churn.join();

    EXPECT_EQ(mismatches.load(), 0);
    EXPECT_EQ(registry.size(), 1u);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
