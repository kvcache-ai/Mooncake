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

#include "transport/nvlink_transport/nvlink_transport.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace mooncake {

class NvlinkTransportTestPeer {
   public:
    using DriverApi = NvlinkVmmAllocation::DriverApi;

    static Status CreateWithDriverApi(
        const NvlinkVmmAllocation::Options& options, const DriverApi& api,
        std::unique_ptr<NvlinkVmmAllocation>& allocation) {
        return NvlinkVmmAllocation::CreateWithDriverApi(options, api,
                                                        allocation);
    }

    static Status CheckStrictFabricCapabilityWithDriverApi(
        const DriverApi& api) {
        return NvlinkVmmAllocation::CheckStrictFabricCapabilityWithDriverApi(
            api);
    }

    static bool IsExactOwnedRange(void* base, size_t length) {
        return NvlinkVmmAllocation::IsExactOwnedRange(base, length);
    }

    static bool RegisterOwnedRange(void* base, size_t length) {
        return NvlinkVmmAllocation::RegisterOwnedRange(base, length);
    }

    static bool UnregisterOwnedRange(void* base, size_t length) {
        return NvlinkVmmAllocation::UnregisterOwnedRange(base, length);
    }

    static bool TrackPinnedVmmAllocation(
        std::unique_ptr<NvlinkVmmAllocation> allocation) {
        return NvlinkTransport::trackPinnedVmmAllocation(std::move(allocation));
    }

    static bool RetryCleanupPendingOwners() {
        return NvlinkVmmAllocation::RetryCleanupPendingOwners();
    }

    static size_t CleanupPendingOwnerCount() {
        return NvlinkVmmAllocation::CleanupPendingOwnerCount();
    }
};

namespace {

class FakeCudaDriver {
   public:
    enum class Failure {
        NONE,
        GRANULARITY,
        CREATE,
        RESERVE,
        MAP,
    };

    NvlinkTransportTestPeer::DriverApi api() {
        NvlinkTransportTestPeer::DriverApi result;
        result.device_get_count = [this](int* count) {
            *count = device_count;
            return CUDA_SUCCESS;
        };
        result.device_get = [](CUdevice* device, int ordinal) {
            *device = ordinal;
            return CUDA_SUCCESS;
        };
        result.device_get_attribute = [this](int* value,
                                             CUdevice_attribute attribute,
                                             CUdevice device) {
            if (attribute == CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED) {
                *value = device == unsupported_fabric_device ? 0 : 1;
            } else if (
                attribute ==
                CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED) {
                *value = 1;
            } else {
                *value = 0;
            }
            return CUDA_SUCCESS;
        };
        result.mem_get_allocation_granularity =
            [this](size_t* value, const CUmemAllocationProp* prop,
                   CUmemAllocationGranularity_flags) {
                operations.push_back("granularity");
                last_prop = *prop;
                if (failure == Failure::GRANULARITY)
                    return CUDA_ERROR_INVALID_VALUE;
                *value = granularity;
                return CUDA_SUCCESS;
            };
        result.mem_create = [this](CUmemGenericAllocationHandle* handle,
                                   size_t size, const CUmemAllocationProp* prop,
                                   unsigned long long) {
            operations.push_back("create");
            last_prop = *prop;
            created_size = size;
            if (failure == Failure::CREATE) return CUDA_ERROR_INVALID_VALUE;
            *handle = next_handle++;
            ++owned_handles;
            return CUDA_SUCCESS;
        };
        result.mem_address_reserve = [this](CUdeviceptr* ptr, size_t size,
                                            size_t alignment, CUdeviceptr,
                                            unsigned long long) {
            operations.push_back("reserve");
            reserved_size = size;
            reserved_alignment = alignment;
            if (failure == Failure::RESERVE) return CUDA_ERROR_INVALID_VALUE;
            *ptr = next_address;
            ++reserved_ranges;
            return CUDA_SUCCESS;
        };
        result.mem_map = [this](CUdeviceptr, size_t, size_t,
                                CUmemGenericAllocationHandle,
                                unsigned long long) {
            operations.push_back("map");
            if (failure == Failure::MAP) return CUDA_ERROR_INVALID_VALUE;
            ++mapped_ranges;
            return CUDA_SUCCESS;
        };
        result.mem_set_access = [this](CUdeviceptr, size_t,
                                       const CUmemAccessDesc* desc,
                                       size_t count) {
            operations.push_back("access");
            const int call = access_call_count++;
            for (size_t i = 0; i < count; ++i)
                access_descriptors.push_back(desc[i]);
            if (call == fail_access_call) return CUDA_ERROR_INVALID_VALUE;
            return CUDA_SUCCESS;
        };
        result.mem_unmap = [this](CUdeviceptr, size_t) {
            operations.push_back("unmap");
            ++unmap_calls;
            if (unmap_failures_remaining > 0) {
                --unmap_failures_remaining;
                return CUDA_ERROR_INVALID_VALUE;
            }
            if (mapped_ranges > 0) --mapped_ranges;
            return CUDA_SUCCESS;
        };
        result.mem_address_free = [this](CUdeviceptr, size_t) {
            operations.push_back("address_free");
            ++address_free_calls;
            if (address_free_failures_remaining > 0) {
                --address_free_failures_remaining;
                return CUDA_ERROR_INVALID_VALUE;
            }
            if (reserved_ranges > 0) --reserved_ranges;
            return CUDA_SUCCESS;
        };
        result.mem_release = [this](CUmemGenericAllocationHandle) {
            operations.push_back("release");
            ++release_calls;
            if (release_failures_remaining > 0) {
                --release_failures_remaining;
                if (release_error_consumes_handle && owned_handles > 0)
                    --owned_handles;
                return CUDA_ERROR_INVALID_VALUE;
            }
            if (owned_handles > 0) --owned_handles;
            return CUDA_SUCCESS;
        };
        result.mem_retain_allocation_handle = [](CUmemGenericAllocationHandle*,
                                                 void*) {
            return CUDA_ERROR_NOT_SUPPORTED;
        };
        result.mem_get_address_range = [](CUdeviceptr*, size_t*, CUdeviceptr) {
            return CUDA_ERROR_NOT_SUPPORTED;
        };
        return result;
    }

    void expectNoResources() const {
        EXPECT_EQ(owned_handles, 0);
        EXPECT_EQ(reserved_ranges, 0);
        EXPECT_EQ(mapped_ranges, 0);
    }

    Failure failure = Failure::NONE;
    size_t granularity = 64 * 1024;
    int device_count = 2;
    int unsupported_fabric_device = -1;
    int fail_access_call = -1;
    int unmap_failures_remaining = 0;
    int address_free_failures_remaining = 0;
    int release_failures_remaining = 0;
    bool release_error_consumes_handle = false;

    int owned_handles = 0;
    int reserved_ranges = 0;
    int mapped_ranges = 0;
    int access_call_count = 0;
    int unmap_calls = 0;
    int address_free_calls = 0;
    int release_calls = 0;
    size_t created_size = 0;
    size_t reserved_size = 0;
    size_t reserved_alignment = 0;
    CUmemAllocationProp last_prop = {};
    std::vector<CUmemAccessDesc> access_descriptors;
    std::vector<std::string> operations;

   private:
    CUmemGenericAllocationHandle next_handle = 1;
    CUdeviceptr next_address = static_cast<CUdeviceptr>(0x100000000ULL);
};

class ScopedIpcEnvironment {
   public:
    ScopedIpcEnvironment() {
        const char* value = std::getenv("MC_USE_NVLINK_IPC");
        if (value != nullptr) {
            was_set_ = true;
            value_ = value;
        }
        unsetenv("MC_USE_NVLINK_IPC");
    }

    ~ScopedIpcEnvironment() {
        if (was_set_)
            setenv("MC_USE_NVLINK_IPC", value_.c_str(), 1);
        else
            unsetenv("MC_USE_NVLINK_IPC");
    }

   private:
    bool was_set_ = false;
    std::string value_;
};

TEST(NvlinkVmmAllocationTest, DeviceExportableHonorsRequiredVaAlignment) {
    ScopedIpcEnvironment environment;
    FakeCudaDriver driver;
    NvlinkVmmAllocation::Options options;
    options.location_type = NvlinkVmmAllocation::LocationType::DEVICE;
    options.location_id = 1;
    options.requested_length = 100000;
    options.fabric_exportable = true;
    options.required_va_alignment = 256 * 1024;

    std::unique_ptr<NvlinkVmmAllocation> allocation;
    ASSERT_TRUE(NvlinkTransportTestPeer::CreateWithDriverApi(
                    options, driver.api(), allocation)
                    .ok());
    ASSERT_NE(allocation, nullptr);
    EXPECT_EQ(allocation->base(), reinterpret_cast<void*>(0x100000000ULL));
    EXPECT_EQ(allocation->length(), 256 * 1024);
    EXPECT_EQ(allocation->granularity(), 64 * 1024);
    EXPECT_EQ(allocation->va_alignment(), 256 * 1024);
    EXPECT_EQ(allocation->location_type(),
              NvlinkVmmAllocation::LocationType::DEVICE);
    EXPECT_EQ(allocation->location_id(), 1);
    EXPECT_TRUE(allocation->fabric_exportable());
    EXPECT_EQ(driver.last_prop.location.type, CU_MEM_LOCATION_TYPE_DEVICE);
    EXPECT_EQ(driver.last_prop.location.id, 1);
    EXPECT_EQ(driver.last_prop.requestedHandleTypes, CU_MEM_HANDLE_TYPE_FABRIC);
    EXPECT_EQ(driver.created_size, 256 * 1024);
    EXPECT_EQ(driver.reserved_alignment, 256 * 1024);
    ASSERT_EQ(driver.access_descriptors.size(), 2);
    EXPECT_EQ(driver.access_call_count, 1);
    EXPECT_EQ(driver.access_descriptors[0].location.type,
              CU_MEM_LOCATION_TYPE_DEVICE);
    EXPECT_EQ(driver.access_descriptors[1].location.type,
              CU_MEM_LOCATION_TYPE_DEVICE);
    EXPECT_EQ(driver.release_calls, 1);
    EXPECT_EQ(driver.owned_handles, 0);
    EXPECT_EQ(driver.reserved_ranges, 1);
    EXPECT_EQ(driver.mapped_ranges, 1);

    {
        NvlinkVmmAllocation moved(std::move(*allocation));
        allocation.reset();
        EXPECT_EQ(moved.base(), reinterpret_cast<void*>(0x100000000ULL));
        EXPECT_EQ(driver.reserved_ranges, 1);
        EXPECT_EQ(driver.mapped_ranges, 1);
    }
    driver.expectNoResources();
    EXPECT_EQ(driver.unmap_calls, 1);
    EXPECT_EQ(driver.address_free_calls, 1);
}

TEST(NvlinkVmmAllocationTest, HostNumaLocalOnlyGrantsCpuAndAllGpuAccess) {
    ScopedIpcEnvironment environment;
    FakeCudaDriver driver;
    NvlinkVmmAllocation::Options options;
    options.location_type = NvlinkVmmAllocation::LocationType::HOST_NUMA;
    options.location_id = 3;
    options.requested_length = 64 * 1024;
    options.fabric_exportable = false;

    std::unique_ptr<NvlinkVmmAllocation> allocation;
    ASSERT_TRUE(NvlinkTransportTestPeer::CreateWithDriverApi(
                    options, driver.api(), allocation)
                    .ok());
    EXPECT_EQ(driver.last_prop.location.type, CU_MEM_LOCATION_TYPE_HOST_NUMA);
    EXPECT_EQ(driver.last_prop.location.id, 3);
    EXPECT_EQ(driver.last_prop.requestedHandleTypes,
              static_cast<CUmemAllocationHandleType>(0));
    ASSERT_EQ(driver.access_descriptors.size(), 3);
    EXPECT_EQ(driver.access_call_count, 1);
    EXPECT_EQ(driver.access_descriptors[0].location.type,
              CU_MEM_LOCATION_TYPE_HOST_NUMA);
    EXPECT_EQ(driver.access_descriptors[0].location.id, 3);
    EXPECT_EQ(driver.access_descriptors[1].location.type,
              CU_MEM_LOCATION_TYPE_DEVICE);
    EXPECT_EQ(driver.access_descriptors[1].location.id, 0);
    EXPECT_EQ(driver.access_descriptors[2].location.type,
              CU_MEM_LOCATION_TYPE_DEVICE);
    EXPECT_EQ(driver.access_descriptors[2].location.id, 1);

    allocation.reset();
    driver.expectNoResources();
    EXPECT_EQ(driver.unmap_calls, 1);
    EXPECT_EQ(driver.address_free_calls, 1);
}

TEST(NvlinkVmmAllocationTest, CleansEveryCreateFailureStage) {
    ScopedIpcEnvironment environment;
    const std::vector<FakeCudaDriver::Failure> failures = {
        FakeCudaDriver::Failure::GRANULARITY,
        FakeCudaDriver::Failure::CREATE,
        FakeCudaDriver::Failure::RESERVE,
        FakeCudaDriver::Failure::MAP,
    };

    for (FakeCudaDriver::Failure failure : failures) {
        FakeCudaDriver driver;
        driver.failure = failure;
        NvlinkVmmAllocation::Options options;
        options.location_type = NvlinkVmmAllocation::LocationType::HOST_NUMA;
        options.location_id = 0;
        options.requested_length = 64 * 1024;
        options.fabric_exportable = false;

        std::unique_ptr<NvlinkVmmAllocation> allocation;
        EXPECT_FALSE(NvlinkTransportTestPeer::CreateWithDriverApi(
                         options, driver.api(), allocation)
                         .ok());
        EXPECT_EQ(allocation, nullptr);
        driver.expectNoResources();

        if (failure == FakeCudaDriver::Failure::RESERVE) {
            EXPECT_EQ(driver.operations,
                      (std::vector<std::string>{"granularity", "create",
                                                "reserve", "release"}));
        } else if (failure == FakeCudaDriver::Failure::MAP) {
            EXPECT_EQ(
                driver.operations,
                (std::vector<std::string>{"granularity", "create", "reserve",
                                          "map", "address_free", "release"}));
        }
    }
}

TEST(NvlinkVmmAllocationTest, CleansBatchedAccessDescriptorFailure) {
    ScopedIpcEnvironment environment;
    FakeCudaDriver driver;
    driver.fail_access_call = 0;
    NvlinkVmmAllocation::Options options;
    options.location_type = NvlinkVmmAllocation::LocationType::HOST_NUMA;
    options.location_id = 0;
    options.requested_length = 64 * 1024;

    std::unique_ptr<NvlinkVmmAllocation> allocation;
    EXPECT_FALSE(NvlinkTransportTestPeer::CreateWithDriverApi(
                     options, driver.api(), allocation)
                     .ok());
    EXPECT_EQ(allocation, nullptr);
    driver.expectNoResources();
    EXPECT_EQ(driver.access_call_count, 1);
    EXPECT_EQ(driver.access_descriptors.size(), 3U);
    EXPECT_EQ(driver.unmap_calls, 1);
    EXPECT_EQ(driver.address_free_calls, 1);
    EXPECT_EQ(driver.release_calls, 1);
    ASSERT_GE(driver.operations.size(), 3);
    EXPECT_EQ(std::vector<std::string>(driver.operations.end() - 3,
                                       driver.operations.end()),
              (std::vector<std::string>{"unmap", "address_free", "release"}));
}

TEST(NvlinkVmmAllocationTest, PostMapReleaseErrorIsNotRetried) {
    ScopedIpcEnvironment environment;
    FakeCudaDriver driver;
    // Model a CUDA call that returns an earlier asynchronous error after
    // consuming the handle reference. Retrying would double-release it.
    driver.release_failures_remaining = 1;
    driver.release_error_consumes_handle = true;
    NvlinkVmmAllocation::Options options;
    options.location_type = NvlinkVmmAllocation::LocationType::HOST_NUMA;
    options.location_id = 0;
    options.requested_length = 64 * 1024;

    std::unique_ptr<NvlinkVmmAllocation> allocation;
    EXPECT_FALSE(NvlinkTransportTestPeer::CreateWithDriverApi(
                     options, driver.api(), allocation)
                     .ok());
    EXPECT_EQ(allocation, nullptr);
    EXPECT_EQ(NvlinkTransportTestPeer::CleanupPendingOwnerCount(), 0U);
    EXPECT_EQ(driver.owned_handles, 0);
    EXPECT_EQ(driver.release_calls, 1);
    EXPECT_EQ(driver.unmap_calls, 1);
    EXPECT_EQ(driver.address_free_calls, 1);
    driver.expectNoResources();
    EXPECT_EQ(driver.operations,
              (std::vector<std::string>{"granularity", "create", "reserve",
                                        "map", "access", "release", "unmap",
                                        "address_free"}));
}

TEST(NvlinkVmmAllocationTest,
     PersistentRollbackUnmapFailureRetainsCompleteOwnerForRetry) {
    ScopedIpcEnvironment environment;
    FakeCudaDriver driver;
    driver.fail_access_call = 0;
    driver.unmap_failures_remaining = 1000;
    NvlinkVmmAllocation::Options options;
    options.location_type = NvlinkVmmAllocation::LocationType::HOST_NUMA;
    options.location_id = 0;
    options.requested_length = 64 * 1024;

    std::unique_ptr<NvlinkVmmAllocation> allocation;
    EXPECT_FALSE(NvlinkTransportTestPeer::CreateWithDriverApi(
                     options, driver.api(), allocation)
                     .ok());
    EXPECT_EQ(allocation, nullptr);
    EXPECT_EQ(NvlinkTransportTestPeer::CleanupPendingOwnerCount(), 1U);
    EXPECT_EQ(driver.mapped_ranges, 1);
    EXPECT_EQ(driver.reserved_ranges, 1);
    EXPECT_EQ(driver.owned_handles, 1);
    EXPECT_EQ(driver.address_free_calls, 0);
    EXPECT_EQ(driver.release_calls, 0);

    std::unique_ptr<NvlinkVmmAllocation> blocked_allocation;
    Status blocked = NvlinkTransportTestPeer::CreateWithDriverApi(
        options, driver.api(), blocked_allocation);
    EXPECT_FALSE(blocked.ok());
    EXPECT_EQ(blocked_allocation, nullptr);
    EXPECT_NE(blocked.ToString().find("1 owner(s) retained"),
              std::string::npos);

    driver.unmap_failures_remaining = 0;
    EXPECT_TRUE(NvlinkTransportTestPeer::RetryCleanupPendingOwners());
    EXPECT_EQ(NvlinkTransportTestPeer::CleanupPendingOwnerCount(), 0U);
    driver.expectNoResources();
    EXPECT_EQ(driver.address_free_calls, 1);
    EXPECT_EQ(driver.release_calls, 1);
}

TEST(NvlinkVmmAllocationTest, ReleaseRetriesUnmapBeforeLaterStages) {
    ScopedIpcEnvironment environment;
    FakeCudaDriver driver;
    NvlinkVmmAllocation::Options options;
    options.location_type = NvlinkVmmAllocation::LocationType::HOST_NUMA;
    options.location_id = 0;
    options.requested_length = 64 * 1024;
    options.fabric_exportable = true;

    std::unique_ptr<NvlinkVmmAllocation> allocation;
    ASSERT_TRUE(NvlinkTransportTestPeer::CreateWithDriverApi(
                    options, driver.api(), allocation)
                    .ok());
    ASSERT_TRUE(NvlinkTransportTestPeer::IsExactOwnedRange(
        allocation->base(), allocation->length()));
    driver.unmap_failures_remaining = 1;

    Status first = allocation->Release();
    EXPECT_FALSE(first.ok());
    EXPECT_EQ(allocation->base(), reinterpret_cast<void*>(0x100000000ULL));
    EXPECT_EQ(allocation->length(), 64 * 1024);
    EXPECT_EQ(driver.unmap_calls, 1);
    EXPECT_EQ(driver.address_free_calls, 0);
    EXPECT_EQ(driver.mapped_ranges, 1);
    EXPECT_EQ(driver.reserved_ranges, 1);
    EXPECT_FALSE(NvlinkTransportTestPeer::IsExactOwnedRange(
        allocation->base(), allocation->length()));

    EXPECT_TRUE(allocation->Release().ok());
    EXPECT_EQ(driver.unmap_calls, 2);
    EXPECT_EQ(driver.address_free_calls, 1);
    EXPECT_EQ(allocation->base(), nullptr);
    EXPECT_EQ(allocation->length(), 0U);
    driver.expectNoResources();

    allocation.reset();
    EXPECT_EQ(driver.unmap_calls, 2);
    EXPECT_EQ(driver.address_free_calls, 1);
}

TEST(NvlinkVmmAllocationTest, ReleaseRetriesAddressFreeWithoutRepeatingUnmap) {
    ScopedIpcEnvironment environment;
    FakeCudaDriver driver;
    NvlinkVmmAllocation::Options options;
    options.location_type = NvlinkVmmAllocation::LocationType::HOST_NUMA;
    options.location_id = 0;
    options.requested_length = 64 * 1024;

    std::unique_ptr<NvlinkVmmAllocation> allocation;
    ASSERT_TRUE(NvlinkTransportTestPeer::CreateWithDriverApi(
                    options, driver.api(), allocation)
                    .ok());
    driver.address_free_failures_remaining = 1;

    Status first = allocation->Release();
    EXPECT_FALSE(first.ok());
    EXPECT_EQ(allocation->base(), reinterpret_cast<void*>(0x100000000ULL));
    EXPECT_EQ(allocation->length(), 64 * 1024);
    EXPECT_EQ(driver.unmap_calls, 1);
    EXPECT_EQ(driver.address_free_calls, 1);
    EXPECT_EQ(driver.mapped_ranges, 0);
    EXPECT_EQ(driver.reserved_ranges, 1);

    EXPECT_TRUE(allocation->Release().ok());
    EXPECT_EQ(driver.unmap_calls, 1);
    EXPECT_EQ(driver.address_free_calls, 2);
    EXPECT_EQ(allocation->base(), nullptr);
    EXPECT_EQ(allocation->length(), 0U);
    driver.expectNoResources();

    allocation.reset();
    EXPECT_EQ(driver.unmap_calls, 1);
    EXPECT_EQ(driver.address_free_calls, 2);
}

TEST(NvlinkVmmAllocationTest,
     ReleaseStopsBeforeCudaWhenProvenanceRemovalFails) {
    ScopedIpcEnvironment environment;
    FakeCudaDriver driver;
    NvlinkVmmAllocation::Options options;
    options.location_type = NvlinkVmmAllocation::LocationType::HOST_NUMA;
    options.location_id = 0;
    options.requested_length = 64 * 1024;
    options.fabric_exportable = true;

    std::unique_ptr<NvlinkVmmAllocation> allocation;
    ASSERT_TRUE(NvlinkTransportTestPeer::CreateWithDriverApi(
                    options, driver.api(), allocation)
                    .ok());
    void* const base = allocation->base();
    const size_t length = allocation->length();
    ASSERT_TRUE(NvlinkTransportTestPeer::IsExactOwnedRange(base, length));
    ASSERT_TRUE(NvlinkTransportTestPeer::UnregisterOwnedRange(base, length));

    Status first = allocation->Release();
    EXPECT_FALSE(first.ok());
    EXPECT_TRUE(first.IsMemory());
    EXPECT_EQ(driver.unmap_calls, 0);
    EXPECT_EQ(driver.address_free_calls, 0);
    EXPECT_EQ(driver.mapped_ranges, 1);
    EXPECT_EQ(driver.reserved_ranges, 1);

    ASSERT_TRUE(NvlinkTransportTestPeer::RegisterOwnedRange(base, length));
    EXPECT_TRUE(allocation->Release().ok());
    driver.expectNoResources();
}

TEST(NvlinkVmmAllocationTest,
     MoveConstructorTransfersOwnerWithoutEarlyRelease) {
    ScopedIpcEnvironment environment;
    FakeCudaDriver driver;
    NvlinkVmmAllocation::Options options;
    options.location_type = NvlinkVmmAllocation::LocationType::HOST_NUMA;
    options.location_id = 0;
    options.requested_length = 64 * 1024;

    std::unique_ptr<NvlinkVmmAllocation> allocation;
    ASSERT_TRUE(NvlinkTransportTestPeer::CreateWithDriverApi(
                    options, driver.api(), allocation)
                    .ok());
    void* const base = allocation->base();

    {
        NvlinkVmmAllocation moved(std::move(*allocation));
        allocation.reset();
        EXPECT_EQ(moved.base(), base);
        EXPECT_EQ(driver.unmap_calls, 0);
        EXPECT_EQ(driver.address_free_calls, 0);
        EXPECT_EQ(driver.mapped_ranges, 1);
        EXPECT_EQ(driver.reserved_ranges, 1);
    }
    driver.expectNoResources();
    EXPECT_EQ(driver.unmap_calls, 1);
    EXPECT_EQ(driver.address_free_calls, 1);
}

TEST(NvlinkVmmAllocationTest, PinnedOwnerFreeRetainsAndRetriesFailedRelease) {
    ScopedIpcEnvironment environment;
    FakeCudaDriver driver;
    NvlinkVmmAllocation::Options options;
    options.location_type = NvlinkVmmAllocation::LocationType::HOST_NUMA;
    options.location_id = 0;
    options.requested_length = 64 * 1024;

    std::unique_ptr<NvlinkVmmAllocation> allocation;
    ASSERT_TRUE(NvlinkTransportTestPeer::CreateWithDriverApi(
                    options, driver.api(), allocation)
                    .ok());
    void* const base = allocation->base();
    ASSERT_TRUE(NvlinkTransportTestPeer::TrackPinnedVmmAllocation(
        std::move(allocation)));
    driver.unmap_failures_remaining = 1;

    NvlinkTransport::freePinnedLocalMemory(base);
    ASSERT_EQ(driver.mapped_ranges, 1)
        << "failed release must retain the pinned owner map entry";
    EXPECT_EQ(driver.reserved_ranges, 1);
    EXPECT_EQ(driver.unmap_calls, 1);
    EXPECT_EQ(driver.address_free_calls, 0);

    NvlinkTransport::freePinnedLocalMemory(base);
    driver.expectNoResources();
    EXPECT_EQ(driver.unmap_calls, 2);
    EXPECT_EQ(driver.address_free_calls, 1);
}

TEST(NvlinkVmmAllocationTest, RejectsInvalidAlignmentBeforeCreatingResources) {
    ScopedIpcEnvironment environment;
    FakeCudaDriver driver;
    NvlinkVmmAllocation::Options options;
    options.location_type = NvlinkVmmAllocation::LocationType::HOST_NUMA;
    options.location_id = 0;
    options.requested_length = 64 * 1024;
    options.required_va_alignment = 96 * 1024;

    std::unique_ptr<NvlinkVmmAllocation> allocation;
    Status status = NvlinkTransportTestPeer::CreateWithDriverApi(
        options, driver.api(), allocation);
    EXPECT_TRUE(status.IsInvalidArgument());
    EXPECT_EQ(allocation, nullptr);
    driver.expectNoResources();
}

TEST(NvlinkVmmAllocationTest, StrictCapabilityRequiresEveryVisibleDevice) {
    ScopedIpcEnvironment environment;
    FakeCudaDriver driver;
    EXPECT_TRUE(
        NvlinkTransportTestPeer::CheckStrictFabricCapabilityWithDriverApi(
            driver.api())
            .ok());

    driver.unsupported_fabric_device = 1;
    Status status =
        NvlinkTransportTestPeer::CheckStrictFabricCapabilityWithDriverApi(
            driver.api());
    EXPECT_TRUE(status.IsNotSupportedTransport());

    setenv("MC_USE_NVLINK_IPC", "1", 1);
    status = NvlinkTransportTestPeer::CheckStrictFabricCapabilityWithDriverApi(
        driver.api());
    EXPECT_TRUE(status.IsNotSupportedTransport());
}

static_assert(
    std::is_same_v<decltype(&NvlinkTransport::allocatePinnedLocalMemory),
                   void* (*)(size_t)>);
static_assert(std::is_same_v<decltype(&NvlinkTransport::freePinnedLocalMemory),
                             void (*)(void*)>);
static_assert(std::is_move_constructible_v<NvlinkVmmAllocation>);
static_assert(!std::is_move_assignable_v<NvlinkVmmAllocation>);

}  // namespace
}  // namespace mooncake
