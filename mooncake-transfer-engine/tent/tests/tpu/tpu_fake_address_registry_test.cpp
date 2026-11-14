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

// Tests the TPU adapter's synthetic-address registry against host-memory
// regions.

#include "tpu_synthetic_address_registry.h"

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

// ProxyManager's chunk size, so interior addresses mimic real chunk accesses.
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
    uintptr_t fake_addr;
    FakeRegion *region;  // owned by the registry
};

Added add(TpuSyntheticAddrRegistry &registry, size_t size, int device,
          int numa = -1) {
    auto region = std::make_unique<FakeRegion>(size, device, numa);
    FakeRegion *raw = region.get();
    return {registry.add(std::move(region)), raw};
}

void *at(uintptr_t fake_addr, size_t offset) {
    return reinterpret_cast<void *>(fake_addr + offset);
}

TEST(TpuSyntheticAddrRegistryTest, ClassifiesBaseAndInteriorButNotOutside) {
    TpuSyntheticAddrRegistry registry;
    auto r = add(registry, 3 * kChunk, /*device=*/2);
    ASSERT_NE(r.fake_addr, 0u);

    EXPECT_TRUE(registry.contains(at(r.fake_addr, 0)));
    EXPECT_TRUE(registry.contains(at(r.fake_addr, kChunk + 17)));
    EXPECT_TRUE(registry.contains(at(r.fake_addr, 3 * kChunk - 1)));
    EXPECT_FALSE(registry.contains(at(r.fake_addr, 3 * kChunk)));
    EXPECT_FALSE(registry.contains(reinterpret_cast<void *>(r.fake_addr - 1)));
    EXPECT_EQ(registry.deviceIndex(at(r.fake_addr, kChunk)), 2);
    EXPECT_EQ(registry.deviceIndex(at(r.fake_addr, 3 * kChunk)), -1);
}

// Regression test for #2815 (silent corruption via interior addresses).
TEST(TpuSyntheticAddrRegistryTest, D2HFromInteriorAddressCopiesThatChunk) {
    TpuSyntheticAddrRegistry registry;
    auto r = add(registry, 3 * kChunk, 0);
    std::vector<uint8_t> staging(kChunk, 0);

    ASSERT_EQ(registry.copyD2H(staging.data(), at(r.fake_addr, kChunk), kChunk),
              0);
    EXPECT_EQ(r.region->lastOffset(), kChunk);
    EXPECT_EQ(0,
              std::memcmp(staging.data(), r.region->data() + kChunk, kChunk));
}

TEST(TpuSyntheticAddrRegistryTest, H2DToInteriorAddressWritesThatChunk) {
    TpuSyntheticAddrRegistry registry;
    auto r = add(registry, 3 * kChunk, 0);
    std::vector<uint8_t> staging(kChunk, 0xAB);

    ASSERT_EQ(
        registry.copyH2D(at(r.fake_addr, 2 * kChunk), staging.data(), kChunk),
        0);
    EXPECT_EQ(r.region->lastOffset(), 2 * kChunk);
    EXPECT_EQ(r.region->data()[2 * kChunk], 0xAB);
    EXPECT_EQ(r.region->data()[3 * kChunk - 1], 0xAB);
    EXPECT_NE(r.region->data()[2 * kChunk - 1], 0xAB);  // previous chunk
}

TEST(TpuSyntheticAddrRegistryTest,
     CopyRunningPastTheEndFailsWithoutTouchingDevice) {
    TpuSyntheticAddrRegistry registry;
    auto r = add(registry, 2 * kChunk, 0);
    std::vector<uint8_t> staging(kChunk + 1);

    EXPECT_EQ(
        registry.copyD2H(staging.data(), at(r.fake_addr, kChunk), kChunk + 1),
        1);
    EXPECT_EQ(
        registry.copyH2D(at(r.fake_addr, kChunk), staging.data(), kChunk + 1),
        1);
    EXPECT_EQ(registry.copyD2H(staging.data(), at(r.fake_addr, 0), SIZE_MAX),
              1);
    EXPECT_EQ(r.region->calls(), 0);
}

TEST(TpuSyntheticAddrRegistryTest, UnknownAddressesAndNullArgumentsFail) {
    TpuSyntheticAddrRegistry registry;
    auto r = add(registry, kChunk, 0);
    uint8_t host[16];
    int not_a_fake_addr = 0;

    EXPECT_EQ(registry.copyD2H(host, &not_a_fake_addr, sizeof(not_a_fake_addr)),
              1);
    EXPECT_EQ(registry.copyH2D(&not_a_fake_addr, host, sizeof(not_a_fake_addr)),
              1);
    EXPECT_EQ(registry.copyD2H(nullptr, at(r.fake_addr, 0), 1), 1);
    EXPECT_EQ(registry.copyH2D(at(r.fake_addr, 0), nullptr, 1), 1);
    EXPECT_FALSE(registry.contains(&not_a_fake_addr));
    EXPECT_FALSE(registry.contains(nullptr));
    EXPECT_EQ(r.region->calls(), 0);
}

TEST(TpuSyntheticAddrRegistryTest, RegionCopyFailureIsReported) {
    TpuSyntheticAddrRegistry registry;
    auto r = add(registry, kChunk, 0);
    r.region->setFail(true);
    std::vector<uint8_t> staging(kChunk);

    EXPECT_EQ(registry.copyD2H(staging.data(), at(r.fake_addr, 0), kChunk), 1);
    EXPECT_EQ(registry.copyH2D(at(r.fake_addr, 0), staging.data(), kChunk), 1);
}

// A size that is not a page multiple leaves slack at the reservation's end.
TEST(TpuSyntheticAddrRegistryTest, ReservationSlackIsNotPartOfTheRegion) {
    TpuSyntheticAddrRegistry registry;
    auto r = add(registry, 100, 0);
    EXPECT_TRUE(registry.contains(at(r.fake_addr, 99)));
    EXPECT_FALSE(registry.contains(at(r.fake_addr, 100)));
}

TEST(TpuSyntheticAddrRegistryTest,
     FakeAddressesOfDifferentRegionsDoNotOverlap) {
    TpuSyntheticAddrRegistry registry;
    auto a = add(registry, kChunk, 0);
    auto b = add(registry, kChunk, 1);
    ASSERT_NE(a.fake_addr, 0u);
    ASSERT_NE(b.fake_addr, 0u);
    EXPECT_TRUE(a.fake_addr + kChunk <= b.fake_addr ||
                b.fake_addr + kChunk <= a.fake_addr);
    EXPECT_EQ(registry.deviceIndex(at(a.fake_addr, 5)), 0);
    EXPECT_EQ(registry.deviceIndex(at(b.fake_addr, 5)), 1);
}

TEST(TpuSyntheticAddrRegistryTest, RemoveDropsRegionButKeepsDevice) {
    TpuSyntheticAddrRegistry registry;
    auto r = add(registry, kChunk, 3, /*numa=*/1);

    EXPECT_FALSE(
        registry.remove(r.fake_addr + 1));  // must be the exact fake address
    EXPECT_TRUE(registry.remove(r.fake_addr));
    EXPECT_FALSE(registry.remove(r.fake_addr));
    EXPECT_FALSE(registry.contains(at(r.fake_addr, 0)));
    EXPECT_EQ(registry.deviceCount(), 4);
    EXPECT_EQ(registry.deviceNuma(3), 1);
}

TEST(TpuSyntheticAddrRegistryTest, DeviceTableTracksHighestIndexAndNuma) {
    TpuSyntheticAddrRegistry registry;
    EXPECT_EQ(registry.deviceCount(), 0);
    add(registry, kChunk, 0, /*numa=*/0);
    add(registry, kChunk, 3, /*numa=*/-1);
    add(registry, kChunk, 3, /*numa=*/1);  // fills in the unknown NUMA node

    EXPECT_EQ(registry.deviceCount(), 4);
    EXPECT_EQ(registry.deviceNuma(0), 0);
    EXPECT_EQ(registry.deviceNuma(1), -1);  // never seen
    EXPECT_EQ(registry.deviceNuma(3), 1);
}

TEST(TpuSyntheticAddrRegistryTest, RejectsInvalidRegions) {
    TpuSyntheticAddrRegistry registry;
    EXPECT_EQ(registry.add(nullptr), 0u);
    EXPECT_EQ(registry.add(std::make_unique<FakeRegion>(0, 0)), 0u);
    EXPECT_EQ(registry.add(std::make_unique<FakeRegion>(kChunk, -1)), 0u);
    EXPECT_EQ(registry.deviceCount(), 0);
}

TEST(TpuSyntheticAddrRegistryTest, ConcurrentCopiesAndRegistrationChurn) {
    TpuSyntheticAddrRegistry registry;
    constexpr size_t kChunks = 8;
    auto r = add(registry, kChunks * kChunk, 0);
    std::atomic<bool> stop{false};

    std::thread churn([&] {
        while (!stop.load()) {
            auto other = add(registry, kChunk, 1);
            EXPECT_TRUE(registry.remove(other.fake_addr));
        }
    });

    std::vector<std::thread> copiers;
    std::atomic<int> mismatches{0};
    for (size_t c = 0; c < kChunks; ++c) {
        copiers.emplace_back([&, c] {
            std::vector<uint8_t> staging(kChunk);
            for (int iter = 0; iter < 20; ++iter) {
                if (registry.copyD2H(staging.data(),
                                     at(r.fake_addr, c * kChunk),
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
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
