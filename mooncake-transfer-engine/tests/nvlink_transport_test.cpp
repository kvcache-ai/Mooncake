#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <thread>
#include <memory>
#include <cstring>
#include <cstdlib>
#include <condition_variable>
#include <future>

#include "cuda_alike.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/nvlink_transport/nvlink_host_numa_allocation.h"
#include "transport/nvlink_transport/nvlink_transport.h"
#include "transport/transport.h"

using namespace mooncake;

// Select protocol based on build configuration
#ifdef USE_HIP
#define MNNVL_PROTOCOL "hip"
#else
#define MNNVL_PROTOCOL "nvlink"
#endif

DEFINE_string(metadata_server, "127.0.0.1:2379", "etcd server host address");
DEFINE_string(local_server_name, "cuda_server:12345", "Local server name");
DEFINE_string(segment_id, "cuda_server:12345", "Segment ID to access data");
DEFINE_int32(gpu_id, 0, "GPU ID to use");

#if MOONCAKE_NVLINK_HOST_NUMA_ENABLED
namespace mooncake {

class NvlinkTransportTestPeer {
   public:
    using DriverApi = NvlinkHostNumaAllocation::DriverApi;
    using AddLocalMemoryBufferOp = NvlinkTransport::AddLocalMemoryBufferOp;
    using RemoveLocalMemoryBufferOp =
        NvlinkTransport::RemoveLocalMemoryBufferOp;
    using UpdateLocalSegmentDescOp = NvlinkTransport::UpdateLocalSegmentDescOp;

    static std::unique_ptr<NvlinkHostNumaAllocation> MakeOwnedRange(
        void* base, size_t length, const DriverApi& api, bool mapped = false) {
        auto allocation = std::unique_ptr<NvlinkHostNumaAllocation>(
            new NvlinkHostNumaAllocation());
        allocation->base_ = base;
        allocation->length_ = length;
        allocation->numa_node_ = 0;
        allocation->driver_api_ = api;
        allocation->mapped_ = mapped;
        allocation->address_reserved_ = mapped;
        allocation->handle_owned_ = mapped;
        allocation->allocation_handle_ = 99;
        allocation->owned_range_registered_ =
            NvlinkHostNumaAllocation::RegisterOwnedRange(base, length, api);
        return allocation->owned_range_registered_ ? std::move(allocation)
                                                   : nullptr;
    }

    static std::shared_ptr<TransferMetadata> Configure(
        NvlinkTransport& transport) {
        auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        auto segment = std::make_shared<TransferMetadata::SegmentDesc>();
        segment->name = "local";
        segment->protocol = "nvlink";
        EXPECT_EQ(metadata->addLocalSegment(LOCAL_SEGMENT_ID, "local",
                                            std::move(segment)),
                  0);
        transport.meta() = metadata;
        transport.use_fabric_mem_ = true;
        return metadata;
    }

    static int Register(NvlinkTransport& transport, void* base, size_t length) {
        return transport.registerLocalMemory(base, length, "cpu:0", true, true);
    }

    static int Unregister(NvlinkTransport& transport, void* base) {
        return transport.unregisterLocalMemory(base, true);
    }

    static int RegisterWithOperations(
        NvlinkTransport& transport, void* base, size_t length,
        const AddLocalMemoryBufferOp& add_buffer,
        const RemoveLocalMemoryBufferOp& remove_buffer,
        const UpdateLocalSegmentDescOp& update_segment,
        bool update_metadata = true) {
        std::lock_guard<std::mutex> lock(transport.register_mutex_);
        return transport
            .registerHostNumaMemoryLocked(base, length, "cpu:0",
                                          update_metadata, add_buffer,
                                          remove_buffer, update_segment)
            .value_or(ERR_ADDRESS_NOT_REGISTERED);
    }

    static int UnregisterWithOperations(
        NvlinkTransport& transport, void* base,
        const RemoveLocalMemoryBufferOp& remove_buffer,
        const UpdateLocalSegmentDescOp& update_segment,
        bool update_metadata = true) {
        std::lock_guard<std::mutex> lock(transport.register_mutex_);
        return transport.unregisterHostNumaMemoryLocked(
            base, update_metadata, remove_buffer, update_segment);
    }

    static int UnregisterBatch(NvlinkTransport& transport,
                               const std::vector<void*>& addresses,
                               const RemoveLocalMemoryBufferOp& remove_buffer,
                               const UpdateLocalSegmentDescOp& update_segment) {
        std::lock_guard<std::mutex> lock(transport.register_mutex_);
        return transport.unregisterHostNumaMemoryBatchLocked(
            addresses, remove_buffer, update_segment);
    }

    static size_t RegistrationCount(const NvlinkTransport& transport) {
        return transport.host_numa_registration_handles_.size();
    }

    static void MarkMetadataRemovedLocally(NvlinkTransport& transport,
                                           void* base) {
        transport.host_numa_registration_handles_.at(base)
            .metadata_removed_locally = true;
    }
};

}  // namespace mooncake

namespace {

struct FakeRegistrationDriver {
    NvlinkTransportTestPeer::DriverApi api() {
        NvlinkTransportTestPeer::DriverApi api;
        api.mem_retain_allocation_handle =
            [this](CUmemGenericAllocationHandle* handle, void*) {
                if (before_retain) before_retain();
                ++retain_calls;
                if (retain_result != CUDA_SUCCESS) return retain_result;
                ++live_handles;
                *handle = 17;
                return CUDA_SUCCESS;
            };
        api.mem_export_to_shareable_handle =
            [this](void* output, CUmemGenericAllocationHandle,
                   CUmemAllocationHandleType type, unsigned long long) {
                ++export_calls;
                EXPECT_EQ(type, CU_MEM_HANDLE_TYPE_FABRIC);
                if (export_result != CUDA_SUCCESS) return export_result;
                std::memset(output, 0x5a, sizeof(CUmemFabricHandle));
                return CUDA_SUCCESS;
            };
        api.mem_release = [this](CUmemGenericAllocationHandle handle) {
            if (handle == 99) {
                ++allocation_release_calls;
                return CUDA_SUCCESS;
            }
            EXPECT_EQ(handle, 17);
            if (before_release) before_release();
            ++release_calls;
            if (release_failures > 0) {
                --release_failures;
                return CUDA_ERROR_INVALID_VALUE;
            }
            --live_handles;
            return CUDA_SUCCESS;
        };
        api.mem_unmap = [this](CUdeviceptr, size_t) {
            if (before_unmap) before_unmap();
            ++unmap_calls;
            if (unmap_failures-- > 0) return CUDA_ERROR_INVALID_VALUE;
            return CUDA_SUCCESS;
        };
        api.mem_address_free = [this](CUdeviceptr, size_t) {
            ++free_calls;
            if (free_failures-- > 0) return CUDA_ERROR_INVALID_VALUE;
            return CUDA_SUCCESS;
        };
        return api;
    }

    std::function<void()> before_retain;
    std::function<void()> before_release;
    std::function<void()> before_unmap;
    int unmap_calls = 0;
    int free_calls = 0;
    int unmap_failures = 0;
    int free_failures = 0;
    int allocation_release_calls = 0;
    int retain_calls = 0;
    int export_calls = 0;
    int release_calls = 0;
    int release_failures = 0;
    int live_handles = 0;
    CUresult retain_result = CUDA_SUCCESS;
    CUresult export_result = CUDA_SUCCESS;
};

TEST(NvlinkHostNumaRegistrationTest,
     RetainFailureIsReportedWithoutPublication) {
    constexpr size_t kLength = 64 * 1024;
    void* const base = reinterpret_cast<void*>(0x080000000ULL);
    FakeRegistrationDriver driver;
    auto owner =
        NvlinkTransportTestPeer::MakeOwnedRange(base, kLength, driver.api());
    ASSERT_NE(owner, nullptr);
    driver.retain_result = CUDA_ERROR_INVALID_VALUE;
    NvlinkTransport transport;
    int publication_calls = 0;

    EXPECT_EQ(NvlinkTransportTestPeer::RegisterWithOperations(
                  transport, base, kLength,
                  [&](const TransferMetadata::BufferDesc&, bool) {
                      ++publication_calls;
                      return 0;
                  },
                  [](void*, bool) { return 0; }, []() { return 0; }),
              ERR_MEMORY);
    EXPECT_EQ(driver.retain_calls, 1);
    EXPECT_EQ(driver.export_calls, 0);
    EXPECT_EQ(driver.release_calls, 0);
    EXPECT_EQ(driver.live_handles, 0);
    EXPECT_EQ(publication_calls, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 0U);
}

TEST(NvlinkHostNumaRegistrationTest,
     ExportFailureReleasesRetainedHandleWithoutPublication) {
    constexpr size_t kLength = 64 * 1024;
    void* const base = reinterpret_cast<void*>(0x090000000ULL);
    FakeRegistrationDriver driver;
    auto owner =
        NvlinkTransportTestPeer::MakeOwnedRange(base, kLength, driver.api());
    ASSERT_NE(owner, nullptr);
    driver.export_result = CUDA_ERROR_NOT_SUPPORTED;
    NvlinkTransport transport;
    int publication_calls = 0;

    EXPECT_EQ(NvlinkTransportTestPeer::RegisterWithOperations(
                  transport, base, kLength,
                  [&](const TransferMetadata::BufferDesc&, bool) {
                      ++publication_calls;
                      return 0;
                  },
                  [](void*, bool) { return 0; }, []() { return 0; }),
              ERR_MEMORY);
    EXPECT_EQ(driver.retain_calls, 1);
    EXPECT_EQ(driver.export_calls, 1);
    EXPECT_EQ(driver.release_calls, 1);
    EXPECT_EQ(driver.live_handles, 0);
    EXPECT_EQ(publication_calls, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 0U);
}

TEST(NvlinkHostNumaRegistrationTest,
     ExportAndReleaseFailureRetainsHandleForUnregisterRetry) {
    constexpr size_t kLength = 64 * 1024;
    void* const base = reinterpret_cast<void*>(0x0a0000000ULL);
    FakeRegistrationDriver driver;
    auto owner =
        NvlinkTransportTestPeer::MakeOwnedRange(base, kLength, driver.api());
    ASSERT_NE(owner, nullptr);
    driver.export_result = CUDA_ERROR_NOT_SUPPORTED;
    driver.release_failures = 1;
    NvlinkTransport transport;
    int remove_calls = 0;

    EXPECT_EQ(NvlinkTransportTestPeer::RegisterWithOperations(
                  transport, base, kLength,
                  [](const TransferMetadata::BufferDesc&, bool) { return 0; },
                  [&](void*, bool) {
                      ++remove_calls;
                      return 0;
                  },
                  []() { return 0; }),
              ERR_MEMORY);
    EXPECT_EQ(driver.release_calls, 1);
    EXPECT_EQ(driver.live_handles, 1);
    EXPECT_EQ(remove_calls, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 1U);

    EXPECT_EQ(NvlinkTransportTestPeer::UnregisterWithOperations(
                  transport, base,
                  [&](void*, bool) {
                      ++remove_calls;
                      return 0;
                  },
                  []() { return 0; }),
              0);
    EXPECT_EQ(driver.release_calls, 2);
    EXPECT_EQ(driver.live_handles, 0);
    EXPECT_EQ(remove_calls, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 0U);
}

TEST(NvlinkHostNumaRegistrationTest, PublishesExactRangeAndRetainsHandle) {
    constexpr size_t kLength = 64 * 1024;
    void* const base = reinterpret_cast<void*>(0x100000000ULL);
    FakeRegistrationDriver driver;
    auto owner =
        NvlinkTransportTestPeer::MakeOwnedRange(base, kLength, driver.api());
    ASSERT_NE(owner, nullptr);

    NvlinkTransport transport;
    auto metadata = NvlinkTransportTestPeer::Configure(transport);
    ASSERT_EQ(NvlinkTransportTestPeer::Register(transport, base, kLength), 0);
    EXPECT_EQ(driver.retain_calls, 1);
    EXPECT_EQ(driver.export_calls, 1);
    EXPECT_EQ(driver.release_calls, 0);
    EXPECT_EQ(driver.live_handles, 1);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 1U);

    auto segment = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(segment, nullptr);
    ASSERT_EQ(segment->buffers.size(), 1U);
    EXPECT_EQ(segment->buffers[0].addr, reinterpret_cast<uint64_t>(base));
    EXPECT_EQ(segment->buffers[0].length, kLength);
    EXPECT_EQ(segment->buffers[0].shm_name.size(),
              sizeof(CUmemFabricHandle) * 2);

    EXPECT_EQ(NvlinkTransportTestPeer::Unregister(transport, base), 0);
    EXPECT_EQ(driver.release_calls, 1);
    EXPECT_EQ(driver.live_handles, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 0U);
    EXPECT_TRUE(owner->Release().ok());
}

TEST(NvlinkHostNumaRegistrationTest, FailedUnregisterKeepsHandleOwned) {
    constexpr size_t kLength = 64 * 1024;
    void* const base = reinterpret_cast<void*>(0x200000000ULL);
    FakeRegistrationDriver driver;
    auto owner =
        NvlinkTransportTestPeer::MakeOwnedRange(base, kLength, driver.api());
    ASSERT_NE(owner, nullptr);

    NvlinkTransport transport;
    NvlinkTransportTestPeer::Configure(transport);
    ASSERT_EQ(NvlinkTransportTestPeer::Register(transport, base, kLength), 0);
    EXPECT_NE(NvlinkTransportTestPeer::Unregister(
                  transport, reinterpret_cast<void*>(0x300000000ULL)),
              0);
    EXPECT_EQ(driver.release_calls, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 1U);
    EXPECT_EQ(NvlinkTransportTestPeer::Unregister(transport, base), 0);
    EXPECT_EQ(driver.release_calls, 1);
    EXPECT_TRUE(owner->Release().ok());
}

TEST(NvlinkHostNumaRegistrationTest, RejectsNonExactOwnedRange) {
    constexpr size_t kLength = 64 * 1024;
    void* const base = reinterpret_cast<void*>(0x400000000ULL);
    FakeRegistrationDriver driver;
    auto owner =
        NvlinkTransportTestPeer::MakeOwnedRange(base, kLength, driver.api());
    ASSERT_NE(owner, nullptr);

    NvlinkTransport transport;
    NvlinkTransportTestPeer::Configure(transport);
    EXPECT_EQ(NvlinkTransportTestPeer::Register(transport, base, kLength / 2),
              ERR_INVALID_ARGUMENT);
    EXPECT_EQ(driver.retain_calls, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 0U);
    EXPECT_TRUE(owner->Release().ok());
}

TEST(NvlinkHostNumaRegistrationTest, RetriesMetadataPublicationBeforeRelease) {
    constexpr size_t kLength = 64 * 1024;
    void* const base = reinterpret_cast<void*>(0x500000000ULL);
    FakeRegistrationDriver driver;
    auto owner =
        NvlinkTransportTestPeer::MakeOwnedRange(base, kLength, driver.api());
    ASSERT_NE(owner, nullptr);

    NvlinkTransport transport;
    auto metadata = NvlinkTransportTestPeer::Configure(transport);
    ASSERT_EQ(NvlinkTransportTestPeer::Register(transport, base, kLength), 0);
    ASSERT_EQ(metadata->removeLocalMemoryBuffer(base, false), 0);
    NvlinkTransportTestPeer::MarkMetadataRemovedLocally(transport, base);

    EXPECT_EQ(NvlinkTransportTestPeer::Unregister(transport, base), 0);
    EXPECT_EQ(driver.release_calls, 1);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 0U);
    EXPECT_TRUE(owner->Release().ok());
}

TEST(NvlinkHostNumaRegistrationTest,
     ReleaseFailureKeepsRegistrationWithoutRepeatingMetadataDelete) {
    constexpr size_t kLength = 64 * 1024;
    void* const base = reinterpret_cast<void*>(0x600000000ULL);
    FakeRegistrationDriver driver;
    auto owner =
        NvlinkTransportTestPeer::MakeOwnedRange(base, kLength, driver.api());
    ASSERT_NE(owner, nullptr);
    driver.release_failures = 1;
    NvlinkTransport transport;
    int remove_calls = 0;
    int update_calls = 0;
    auto remove_buffer = [&](void*, bool) {
        ++remove_calls;
        return 0;
    };
    auto update_segment = [&]() {
        ++update_calls;
        return 0;
    };

    ASSERT_EQ(NvlinkTransportTestPeer::RegisterWithOperations(
                  transport, base, kLength,
                  [](const TransferMetadata::BufferDesc&, bool) { return 0; },
                  remove_buffer, update_segment),
              0);
    EXPECT_EQ(NvlinkTransportTestPeer::UnregisterWithOperations(
                  transport, base, remove_buffer, update_segment),
              ERR_MEMORY);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 1U);
    EXPECT_EQ(remove_calls, 1);
    EXPECT_EQ(driver.release_calls, 1);
    EXPECT_EQ(driver.live_handles, 1);

    EXPECT_EQ(NvlinkTransportTestPeer::UnregisterWithOperations(
                  transport, base, remove_buffer, update_segment),
              0);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 0U);
    EXPECT_EQ(remove_calls, 1);
    EXPECT_EQ(update_calls, 0);
    EXPECT_EQ(driver.release_calls, 2);
    EXPECT_EQ(driver.live_handles, 0);
}

TEST(NvlinkHostNumaRegistrationTest,
     PublicationFailureRollsBackAndReturnsOriginalError) {
    constexpr size_t kLength = 64 * 1024;
    void* const base = reinterpret_cast<void*>(0x700000000ULL);
    FakeRegistrationDriver driver;
    auto owner =
        NvlinkTransportTestPeer::MakeOwnedRange(base, kLength, driver.api());
    ASSERT_NE(owner, nullptr);
    NvlinkTransport transport;
    int remove_calls = 0;

    EXPECT_EQ(NvlinkTransportTestPeer::RegisterWithOperations(
                  transport, base, kLength,
                  [](const TransferMetadata::BufferDesc&, bool) {
                      return ERR_METADATA;
                  },
                  [&](void*, bool) {
                      ++remove_calls;
                      return 0;
                  },
                  []() { return 0; }),
              ERR_METADATA);
    EXPECT_EQ(remove_calls, 1);
    EXPECT_EQ(driver.release_calls, 1);
    EXPECT_EQ(driver.live_handles, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 0U);
}

TEST(NvlinkHostNumaRegistrationTest,
     FailedPublicationRollbackCanBeCompletedByUnregister) {
    constexpr size_t kLength = 64 * 1024;
    void* const base = reinterpret_cast<void*>(0x800000000ULL);
    FakeRegistrationDriver driver;
    auto owner =
        NvlinkTransportTestPeer::MakeOwnedRange(base, kLength, driver.api());
    ASSERT_NE(owner, nullptr);
    NvlinkTransport transport;
    int remove_calls = 0;
    int update_calls = 0;

    EXPECT_EQ(NvlinkTransportTestPeer::RegisterWithOperations(
                  transport, base, kLength,
                  [](const TransferMetadata::BufferDesc&, bool) {
                      return ERR_METADATA;
                  },
                  [&](void*, bool) {
                      ++remove_calls;
                      return ERR_METADATA;
                  },
                  [&]() {
                      ++update_calls;
                      return 0;
                  }),
              ERR_METADATA);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 1U);
    EXPECT_EQ(driver.release_calls, 0);
    EXPECT_EQ(driver.live_handles, 1);

    EXPECT_EQ(NvlinkTransportTestPeer::UnregisterWithOperations(
                  transport, base,
                  [&](void*, bool) {
                      ++remove_calls;
                      return 0;
                  },
                  [&]() {
                      ++update_calls;
                      return 0;
                  }),
              0);
    EXPECT_EQ(remove_calls, 1);
    EXPECT_EQ(update_calls, 1);
    EXPECT_EQ(driver.release_calls, 1);
    EXPECT_EQ(driver.live_handles, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 0U);
}

TEST(NvlinkHostNumaRegistrationTest,
     MissingLocalMetadataIsTreatedAsIdempotentDelete) {
    constexpr size_t kLength = 64 * 1024;
    void* const base = reinterpret_cast<void*>(0x900000000ULL);
    FakeRegistrationDriver driver;
    auto owner =
        NvlinkTransportTestPeer::MakeOwnedRange(base, kLength, driver.api());
    ASSERT_NE(owner, nullptr);
    NvlinkTransport transport;
    int update_calls = 0;

    ASSERT_EQ(NvlinkTransportTestPeer::RegisterWithOperations(
                  transport, base, kLength,
                  [](const TransferMetadata::BufferDesc&, bool) { return 0; },
                  [](void*, bool) { return ERR_ADDRESS_NOT_REGISTERED; },
                  [&]() {
                      ++update_calls;
                      return 0;
                  }),
              0);
    EXPECT_EQ(NvlinkTransportTestPeer::UnregisterWithOperations(
                  transport, base,
                  [](void*, bool) { return ERR_ADDRESS_NOT_REGISTERED; },
                  [&]() {
                      ++update_calls;
                      return 0;
                  }),
              0);
    EXPECT_EQ(update_calls, 1);
    EXPECT_EQ(driver.release_calls, 1);
    EXPECT_EQ(driver.live_handles, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 0U);
}

TEST(NvlinkHostNumaRegistrationTest, DestructorRemovesMetadataAndReleasesPin) {
    constexpr size_t length = 64 * 1024;
    void* base = reinterpret_cast<void*>(0xe00000000ULL);
    FakeRegistrationDriver driver;
    auto owner = NvlinkTransportTestPeer::MakeOwnedRange(base, length,
                                                         driver.api(), true);
    ASSERT_NE(owner, nullptr);
    std::shared_ptr<TransferMetadata> metadata;
    {
        NvlinkTransport transport;
        metadata = NvlinkTransportTestPeer::Configure(transport);
        ASSERT_EQ(NvlinkTransportTestPeer::Register(transport, base, length),
                  0);
        EXPECT_EQ(driver.live_handles, 1);
        EXPECT_FALSE(owner->Release().ok());
    }
    auto segment = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(segment, nullptr);
    EXPECT_TRUE(segment->buffers.empty());
    EXPECT_EQ(driver.release_calls, 1);
    EXPECT_EQ(driver.live_handles, 0);
    EXPECT_TRUE(owner->Release().ok());
    EXPECT_EQ(driver.unmap_calls, 1);
    EXPECT_EQ(driver.free_calls, 1);
}

TEST(NvlinkHostNumaRegistrationTest,
     DestructorCleanupFailureKeepsAllocationPinned) {
    // Intentionally unrecoverable ownership is confined to this child. Every
    // acceptance condition contributes to its exit status; gtest assertions
    // alone would not make an EXPECT_EXIT child fail.
    EXPECT_EXIT(
        {
            constexpr size_t length = 64 * 1024;
            void* base = reinterpret_cast<void*>(0xf00000000ULL);
            FakeRegistrationDriver driver;
            driver.release_failures = 100;
            auto owner = NvlinkTransportTestPeer::MakeOwnedRange(
                base, length, driver.api(), true);
            if (!owner) std::_Exit(1);
            std::shared_ptr<TransferMetadata> metadata;
            {
                NvlinkTransport transport;
                metadata = NvlinkTransportTestPeer::Configure(transport);
                if (NvlinkTransportTestPeer::Register(transport, base,
                                                      length) != 0)
                    std::_Exit(2);
            }
            auto segment = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
            const bool blocked = !owner->Release().ok();
            const bool retained = driver.live_handles == 1 &&
                                  driver.release_calls == 1 &&
                                  driver.retain_calls == 1;
            const bool untouched = driver.unmap_calls == 0 &&
                                   driver.free_calls == 0 &&
                                   driver.allocation_release_calls == 0;
            const bool unpublished = segment && segment->buffers.empty();
            std::_Exit(blocked && retained && untouched && unpublished ? 0 : 3);
        },
        ::testing::ExitedWithCode(0), "");
}

struct RegistrationScenario {
    static constexpr size_t length = 64 * 1024;
    void* base = reinterpret_cast<void*>(0xb00000000ULL);
    FakeRegistrationDriver driver;
    std::unique_ptr<NvlinkHostNumaAllocation> owner =
        NvlinkTransportTestPeer::MakeOwnedRange(base, length, driver.api(),
                                                true);
    NvlinkTransport transport;
    int add_result = 0;
    int remove_result = 0;
    int update_result = 0;
    int adds = 0;
    int removes = 0;
    int updates = 0;
    std::vector<bool> add_flags;
    std::vector<bool> remove_flags;

    int Register(bool update = true) {
        return NvlinkTransportTestPeer::RegisterWithOperations(
            transport, base, length,
            [&](const TransferMetadata::BufferDesc&, bool publish) {
                ++adds;
                add_flags.push_back(publish);
                return add_result;
            },
            [&](void*, bool publish) {
                ++removes;
                remove_flags.push_back(publish);
                return remove_result;
            },
            [&] {
                ++updates;
                return update_result;
            },
            update);
    }
    int Unregister(bool update = true) {
        return NvlinkTransportTestPeer::UnregisterWithOperations(
            transport, base,
            [&](void*, bool publish) {
                ++removes;
                remove_flags.push_back(publish);
                return remove_result;
            },
            [&] {
                ++updates;
                return update_result;
            },
            update);
    }
};

TEST(NvlinkHostNumaRegistrationTest,
     RegisterRetriesExportRollbackBeforeRetain) {
    RegistrationScenario s;
    ASSERT_NE(s.owner, nullptr);
    s.driver.export_result = CUDA_ERROR_NOT_SUPPORTED;
    s.driver.release_failures = 2;
    EXPECT_EQ(s.Register(), ERR_MEMORY);
    EXPECT_FALSE(s.owner->Release().ok());
    EXPECT_EQ(s.driver.unmap_calls, 0);
    s.driver.export_result = CUDA_SUCCESS;
    EXPECT_EQ(s.Register(), ERR_MEMORY);
    EXPECT_EQ(s.driver.retain_calls, 1);
    EXPECT_EQ(s.Register(), 0);
    EXPECT_EQ(s.driver.retain_calls, 2);
    EXPECT_EQ(s.driver.live_handles, 1);
    EXPECT_EQ(s.Register(), ERR_ADDRESS_OVERLAPPED);
    EXPECT_EQ(s.driver.retain_calls, 2);
    EXPECT_EQ(s.Unregister(), 0);
    EXPECT_TRUE(s.owner->Release().ok());
}

TEST(NvlinkHostNumaRegistrationTest,
     RegisterRetriesPublicationAndReleaseRollback) {
    RegistrationScenario s;
    s.add_result = ERR_METADATA;
    s.driver.release_failures = 1;
    EXPECT_EQ(s.Register(), ERR_METADATA);
    EXPECT_EQ(s.removes, 1);
    EXPECT_EQ(s.driver.live_handles, 1);
    s.add_result = 0;
    EXPECT_EQ(s.Register(), 0);
    EXPECT_EQ(s.removes, 1);
    EXPECT_EQ(s.driver.release_calls, 2);
    EXPECT_EQ(s.driver.retain_calls, 2);
    EXPECT_EQ(s.Unregister(), 0);
}

TEST(NvlinkHostNumaRegistrationTest,
     RegistrationRetryProtectsOwnerAndDoesNotLeakTransientPins) {
    RegistrationScenario s;
    s.driver.export_result = CUDA_ERROR_NOT_SUPPORTED;
    s.driver.release_failures = 2;
    ASSERT_EQ(s.Register(), ERR_MEMORY);
    int release_callbacks = 0;
    s.driver.before_release = [&] {
        ++release_callbacks;
        EXPECT_FALSE(s.owner->Release().ok());
        EXPECT_EQ(s.driver.unmap_calls, 0);
    };
    s.driver.export_result = CUDA_SUCCESS;
    EXPECT_EQ(s.Register(), ERR_MEMORY);
    EXPECT_EQ(release_callbacks, 1);
    EXPECT_EQ(s.driver.retain_calls, 1);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(s.transport), 1U);
    s.driver.before_retain = [&] {
        // Old registration cleanup has completed by this point. The new
        // registration must already protect the allocation before retain.
        EXPECT_FALSE(s.owner->Release().ok());
        EXPECT_EQ(s.driver.unmap_calls, 0);
    };
    EXPECT_EQ(s.Register(), 0);
    EXPECT_EQ(release_callbacks, 2);
    EXPECT_EQ(s.driver.retain_calls, 2);
    s.driver.before_release = {};
    s.driver.before_retain = {};
    EXPECT_EQ(s.Unregister(), 0);
    // Neither the failed retry nor the successful transfer may leak a pin.
    EXPECT_TRUE(s.owner->Release().ok());
    EXPECT_EQ(s.driver.unmap_calls, 1);
    EXPECT_EQ(s.driver.live_handles, 0);
}

TEST(NvlinkHostNumaRegistrationTest,
     DuplicateBatchAddressAttemptsRetainedReleaseOncePerCall) {
    RegistrationScenario s;
    ASSERT_EQ(s.Register(), 0);
    s.driver.release_failures = 1;
    int removes = 0;
    auto remove = [&](void*, bool publish) {
        EXPECT_FALSE(publish);
        ++removes;
        return 0;
    };
    auto update = [] { return 0; };
    EXPECT_EQ(NvlinkTransportTestPeer::UnregisterBatch(
                  s.transport, {s.base, s.base}, remove, update),
              ERR_MEMORY);
    EXPECT_EQ(s.driver.release_calls, 1);
    EXPECT_EQ(s.driver.live_handles, 1);
    EXPECT_EQ(removes, 1);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(s.transport), 1U);
    EXPECT_FALSE(s.owner->Release().ok());
    EXPECT_EQ(s.driver.unmap_calls, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::UnregisterBatch(
                  s.transport, {s.base, s.base}, remove, update),
              0);
    EXPECT_EQ(s.driver.release_calls, 2);
    EXPECT_EQ(removes, 1);
    EXPECT_EQ(s.driver.live_handles, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(s.transport), 0U);
    EXPECT_TRUE(s.owner->Release().ok());
}

TEST(NvlinkHostNumaRegistrationTest,
     RegisterFalseCannotDowngradePendingPublication) {
    RegistrationScenario s;
    s.add_result = ERR_METADATA;
    s.remove_result = ERR_METADATA;
    EXPECT_EQ(s.Register(), ERR_METADATA);
    s.add_result = 0;
    s.remove_result = 0;
    s.update_result = ERR_METADATA;
    EXPECT_EQ(s.Register(false), ERR_METADATA);
    EXPECT_EQ(s.removes, 1);
    EXPECT_EQ(s.driver.release_calls, 0);
    EXPECT_EQ(s.driver.retain_calls, 1);
    s.update_result = 0;
    EXPECT_EQ(s.Register(false), 0);
    EXPECT_EQ(s.updates, 2);
    EXPECT_EQ(s.removes, 1);
    ASSERT_EQ(s.add_flags.size(), 2U);
    EXPECT_FALSE(s.add_flags.back());
    EXPECT_EQ(s.Unregister(), 0);
}

TEST(NvlinkHostNumaRegistrationTest,
     DeferredUnregisterRetainsPinUntilPublished) {
    RegistrationScenario s;
    ASSERT_EQ(s.Register(), 0);
    EXPECT_EQ(s.Unregister(false), 0);
    EXPECT_EQ(s.removes, 1);
    EXPECT_EQ(s.driver.release_calls, 0);
    EXPECT_FALSE(s.owner->Release().ok());
    EXPECT_EQ(s.driver.unmap_calls, 0);
    EXPECT_EQ(s.Unregister(), 0);
    EXPECT_EQ(s.removes, 1);
    EXPECT_EQ(s.updates, 1);
    EXPECT_EQ(s.driver.release_calls, 1);
    EXPECT_TRUE(s.owner->Release().ok());
}

TEST(NvlinkHostNumaRegistrationTest, MultipleTransportsPinSameAllocation) {
    RegistrationScenario s;
    NvlinkTransport other;
    NvlinkTransportTestPeer::Configure(other);
    ASSERT_EQ(s.Register(), 0);
    ASSERT_EQ(NvlinkTransportTestPeer::Register(other, s.base, s.length), 0);
    EXPECT_FALSE(s.owner->Release().ok());
    EXPECT_EQ(s.Unregister(), 0);
    EXPECT_FALSE(s.owner->Release().ok());
    EXPECT_EQ(NvlinkTransportTestPeer::Unregister(other, s.base), 0);
    EXPECT_TRUE(s.owner->Release().ok());
    EXPECT_EQ(s.driver.unmap_calls, 1);
    EXPECT_EQ(s.driver.free_calls, 1);
}

TEST(NvlinkHostNumaRegistrationTest,
     FailedTeardownRejectsExactAndPartialRegistration) {
    for (bool fail_unmap : {true, false}) {
        RegistrationScenario s;
        s.driver.unmap_failures = fail_unmap ? 1 : 0;
        s.driver.free_failures = fail_unmap ? 0 : 1;
        EXPECT_FALSE(s.owner->Release().ok());
        NvlinkTransportTestPeer::Configure(s.transport);
        EXPECT_NE(
            NvlinkTransportTestPeer::Register(s.transport, s.base, s.length),
            0);
        EXPECT_NE(NvlinkTransportTestPeer::Register(s.transport, s.base,
                                                    s.length / 2),
                  0);
        EXPECT_NE(NvlinkTransportTestPeer::Register(
                      s.transport, static_cast<char*>(s.base) + 4096, 4096),
                  0);
        EXPECT_EQ(s.driver.retain_calls, 0);
        EXPECT_TRUE(s.owner->Release().ok());
    }
}

// The promise marks entry into a driver call, after the registry decision.
// A second promise releases it; there are no timing-dependent sleeps.
TEST(NvlinkHostNumaRegistrationTest, RegisterPinWinsBeforeRelease) {
    RegistrationScenario s;
    std::promise<void> entered, resume;
    auto gate = resume.get_future();
    s.driver.before_retain = [&] {
        entered.set_value();
        gate.wait();
    };
    auto registered =
        std::async(std::launch::async, [&] { return s.Register(); });
    entered.get_future().wait();
    EXPECT_FALSE(s.owner->Release().ok());
    EXPECT_EQ(s.driver.unmap_calls, 0);
    resume.set_value();
    EXPECT_EQ(registered.get(), 0);
    EXPECT_EQ(s.Unregister(), 0);
    EXPECT_TRUE(s.owner->Release().ok());
}

TEST(NvlinkHostNumaRegistrationTest, ReleaseWinsBeforeRegisterPin) {
    RegistrationScenario s;
    s.driver.unmap_failures = 1;
    std::promise<void> entered, resume;
    auto gate = resume.get_future();
    s.driver.before_unmap = [&] {
        entered.set_value();
        gate.wait();
    };
    auto released =
        std::async(std::launch::async, [&] { return s.owner->Release(); });
    entered.get_future().wait();
    std::promise<void> register_started;
    auto registered = std::async(std::launch::async, [&] {
        register_started.set_value();
        return s.Register();
    });
    register_started.get_future().wait();
    resume.set_value();
    EXPECT_FALSE(released.get().ok());
    EXPECT_NE(registered.get(), 0);
    EXPECT_EQ(s.driver.retain_calls, 0);
    s.driver.before_unmap = {};
    EXPECT_TRUE(s.owner->Release().ok());
}

TEST(NvlinkHostNumaRegistrationTest,
     FailedOldHandleMustBeCleanedBeforeAddressReuse) {
    RegistrationScenario s;
    ASSERT_EQ(s.Register(), 0);
    s.driver.release_failures = 1;
    EXPECT_EQ(s.Unregister(), ERR_MEMORY);
    EXPECT_FALSE(s.owner->Release().ok());
    EXPECT_EQ(s.driver.free_calls, 0);
    EXPECT_EQ(s.Unregister(), 0);
    EXPECT_TRUE(s.owner->Release().ok());

    FakeRegistrationDriver next_driver;
    auto next = NvlinkTransportTestPeer::MakeOwnedRange(
        s.base, s.length, next_driver.api(), true);
    ASSERT_NE(next, nullptr);
    EXPECT_EQ(s.Register(), 0);
    EXPECT_EQ(next_driver.retain_calls, 1);
    EXPECT_EQ(s.driver.retain_calls, 1);
    EXPECT_EQ(s.Unregister(), 0);
    EXPECT_EQ(next_driver.release_calls, 1);
    EXPECT_EQ(s.driver.release_calls, 2);
    EXPECT_TRUE(next->Release().ok());
}

TEST(NvlinkHostNumaRegistrationTest,
     BatchRetriesPublicationThenPartialHandleRelease) {
    constexpr size_t length = 64 * 1024;
    void* a = reinterpret_cast<void*>(0xc00000000ULL);
    void* b = reinterpret_cast<void*>(0xd00000000ULL);
    FakeRegistrationDriver da, db;
    auto oa =
        NvlinkTransportTestPeer::MakeOwnedRange(a, length, da.api(), true);
    auto ob =
        NvlinkTransportTestPeer::MakeOwnedRange(b, length, db.api(), true);
    ASSERT_NE(oa, nullptr);
    ASSERT_NE(ob, nullptr);
    NvlinkTransport transport;
    NvlinkTransportTestPeer::Configure(transport);
    ASSERT_EQ(NvlinkTransportTestPeer::Register(transport, a, length), 0);
    ASSERT_EQ(NvlinkTransportTestPeer::Register(transport, b, length), 0);
    int removes = 0, updates = 0;
    int publication_result = ERR_METADATA;
    auto remove = [&](void*, bool publish) {
        EXPECT_FALSE(publish);
        ++removes;
        return 0;
    };
    auto update = [&] {
        ++updates;
        return publication_result;
    };
    EXPECT_EQ(NvlinkTransportTestPeer::UnregisterBatch(transport, {a, b},
                                                       remove, update),
              ERR_METADATA);
    EXPECT_EQ(removes, 2);
    EXPECT_EQ(da.release_calls + db.release_calls, 0);
    EXPECT_FALSE(oa->Release().ok());
    EXPECT_FALSE(ob->Release().ok());
    publication_result = 0;
    db.release_failures = 2;
    EXPECT_EQ(NvlinkTransportTestPeer::UnregisterBatch(transport, {a, b},
                                                       remove, update),
              ERR_MEMORY);
    EXPECT_EQ(removes, 2);
    EXPECT_EQ(da.release_calls, 1);
    EXPECT_EQ(db.release_calls, 1);
    EXPECT_EQ(NvlinkTransportTestPeer::UnregisterBatch(transport, {a, b},
                                                       remove, update),
              ERR_MEMORY);
    EXPECT_EQ(da.release_calls, 1);
    EXPECT_EQ(db.release_calls, 2);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 1U);
    EXPECT_EQ(removes, 2);
    EXPECT_FALSE(ob->Release().ok());
    EXPECT_EQ(NvlinkTransportTestPeer::UnregisterBatch(transport, {a, b},
                                                       remove, update),
              0);
    EXPECT_EQ(da.release_calls, 1);
    EXPECT_EQ(db.release_calls, 3);
    EXPECT_EQ(NvlinkTransportTestPeer::RegistrationCount(transport), 0U);
    EXPECT_TRUE(oa->Release().ok());
    EXPECT_TRUE(ob->Release().ok());
}

}  // namespace
#endif

static void checkCudaError(cudaError_t result, const char* message) {
    if (result != cudaSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << cudaGetErrorString(result) << ")";
        exit(EXIT_FAILURE);
    }
}

static void* allocateCudaBuffer(size_t size, int gpu_id) {
    checkCudaError(cudaSetDevice(gpu_id), "Failed to set device");
    void* d_buf = nullptr;
    checkCudaError(cudaMalloc(&d_buf, size),
                   "Failed to allocate device memory");
    return d_buf;
}

static void freeCudaBuffer(void* addr) {
    checkCudaError(cudaFree(addr), "Failed to free device memory");
}

TEST(NvlinkTransportTest, WriteAndRead) {
    const size_t kDataLength = 4096000;
    int gpu_id = FLAGS_gpu_id;

    // Server (target) setup
    auto server_engine = std::make_unique<TransferEngine>(false);
    server_engine->init(FLAGS_metadata_server, FLAGS_local_server_name);

    // Install MNNVL transport (nvlink or hip) on server
    Transport* server_transport =
        server_engine->installTransport(MNNVL_PROTOCOL, nullptr);
    ASSERT_NE(server_transport, nullptr);

    void* server_buffer = allocateCudaBuffer(kDataLength * 2, gpu_id);
    int rc = server_engine->registerLocalMemory(server_buffer, kDataLength * 2,
                                                "cuda:0");
    ASSERT_EQ(rc, 0);

    auto segment_id = server_engine->openSegment(FLAGS_segment_id);

    // Client (initiator) setup
    auto client_engine = std::make_unique<TransferEngine>(false);
    client_engine->init(FLAGS_metadata_server, "cuda_client:12346");

    // Install MNNVL transport (nvlink or hip) on client
    Transport* client_transport =
        client_engine->installTransport(MNNVL_PROTOCOL, nullptr);
    ASSERT_NE(client_transport, nullptr);

    void* client_buffer = allocateCudaBuffer(kDataLength * 2, gpu_id);
    rc = client_engine->registerLocalMemory(client_buffer, kDataLength * 2,
                                            "cuda:" + std::to_string(gpu_id));
    ASSERT_EQ(rc, 0);

    // Write: client -> server
    {
        // Fill client buffer with data
        std::vector<char> host_data(kDataLength, 'A');
        checkCudaError(cudaMemcpy(client_buffer, host_data.data(), kDataLength,
                                  cudaMemcpyHostToDevice),
                       "Memcpy to client_buffer");

        auto batch_id = client_engine->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = client_buffer;
        entry.target_id = segment_id;
        entry.target_offset = (uint64_t)server_buffer;
        Status s = client_engine->submitTransfer(batch_id, {entry});
        ASSERT_TRUE(s.ok());

        // Wait for completion
        TransferStatus status;
        do {
            s = client_engine->getTransferStatus(batch_id, 0, status);
            ASSERT_TRUE(s.ok());
        } while (status.s == TransferStatusEnum::WAITING);

        ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
        s = client_engine->freeBatchID(batch_id);
        ASSERT_TRUE(s.ok());
    }

    // Read: server -> client
    {
        auto batch_id = client_engine->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = (char*)client_buffer + kDataLength;
        entry.target_id = segment_id;
        entry.target_offset = (uint64_t)server_buffer;
        Status s = client_engine->submitTransfer(batch_id, {entry});
        ASSERT_TRUE(s.ok());

        // Wait for completion
        TransferStatus status;
        do {
            s = client_engine->getTransferStatus(batch_id, 0, status);
            ASSERT_TRUE(s.ok());
        } while (status.s == TransferStatusEnum::WAITING);

        ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
        s = client_engine->freeBatchID(batch_id);
        ASSERT_TRUE(s.ok());
    }

    // Check data
    std::vector<char> host_check(kDataLength);
    checkCudaError(
        cudaMemcpy(host_check.data(), (char*)client_buffer + kDataLength,
                   kDataLength, cudaMemcpyDeviceToHost),
        "Memcpy from client_buffer");
    for (size_t i = 0; i < kDataLength; ++i) {
        ASSERT_EQ(host_check[i], 'A');
    }

    // Cleanup
    client_engine->unregisterLocalMemory(client_buffer);
    freeCudaBuffer(client_buffer);
    server_engine->unregisterLocalMemory(server_buffer);
    freeCudaBuffer(server_buffer);
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
