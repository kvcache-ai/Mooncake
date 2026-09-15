// Copyright 2026 KVCache.AI

#include "transport/nvlink_transport/nvlink_host_numa_allocation.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

namespace mooncake {

#if MOONCAKE_NVLINK_HOST_NUMA_ENABLED

class NvlinkHostNumaAllocationTestPeer {
   public:
    using DriverApi = NvlinkHostNumaAllocation::DriverApi;

    static Status Discover(const DriverApi& api, std::vector<int>& nodes) {
        return NvlinkHostNumaAllocation::DiscoverHostNumaNodesWithDriverApi(
            api, nodes);
    }

    static Status Create(
        int node, size_t length, size_t alignment, const DriverApi& api,
        std::unique_ptr<NvlinkHostNumaAllocation>& allocation) {
        return NvlinkHostNumaAllocation::CreateWithDriverApi(
            node, length, alignment, api, allocation);
    }

    static bool IsExact(void* base, size_t length) {
        DriverApi ignored;
        return NvlinkHostNumaAllocation::FindExactOwnedRange(base, length,
                                                             &ignored);
    }
};

namespace {

class FakeCudaDriver {
   public:
    enum class Failure { NONE, GRANULARITY, CREATE, RESERVE, MAP, ACCESS };

    NvlinkHostNumaAllocationTestPeer::DriverApi api() {
        NvlinkHostNumaAllocationTestPeer::DriverApi api;
        api.device_get_count = [this](int* count) {
            *count = static_cast<int>(host_nodes.size());
            return CUDA_SUCCESS;
        };
        api.device_get = [](CUdevice* device, int ordinal) {
            *device = ordinal;
            return CUDA_SUCCESS;
        };
        api.device_get_attribute = [this](int* value,
                                          CUdevice_attribute attribute,
                                          CUdevice device) {
            if (attribute ==
                CU_DEVICE_ATTRIBUTE_VIRTUAL_MEMORY_MANAGEMENT_SUPPORTED) {
                *value = vmm_supported;
            } else if (attribute ==
                       CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED) {
                *value = fabric_supported;
            } else if (attribute == CU_DEVICE_ATTRIBUTE_HOST_NUMA_ID) {
                if (device == host_numa_id_failure_device)
                    return CUDA_ERROR_INVALID_DEVICE;
                *value = host_nodes.at(static_cast<size_t>(device));
#if CUDA_VERSION >= 12090
            } else if (
                attribute ==
                CU_DEVICE_ATTRIBUTE_HOST_NUMA_VIRTUAL_MEMORY_MANAGEMENT_SUPPORTED) {
                *value = vmm_supported;
#endif
            } else {
                *value = 0;
            }
            return CUDA_SUCCESS;
        };
        api.mem_get_allocation_granularity =
            [this](size_t* value, const CUmemAllocationProp* prop,
                   CUmemAllocationGranularity_flags) {
                operations.push_back("granularity");
                last_prop = *prop;
                if (failure == Failure::GRANULARITY ||
                    prop->location.id == granularity_failure_node)
                    return CUDA_ERROR_INVALID_VALUE;
                *value = granularity;
                return CUDA_SUCCESS;
            };
        api.mem_create = [this](CUmemGenericAllocationHandle* handle,
                                size_t size, const CUmemAllocationProp* prop,
                                unsigned long long) {
            operations.push_back("create");
            last_prop = *prop;
            created_size = size;
            if (failure == Failure::CREATE) return CUDA_ERROR_INVALID_VALUE;
            *handle = 7;
            ++handles;
            return CUDA_SUCCESS;
        };
        api.mem_address_reserve = [this](CUdeviceptr* address, size_t,
                                         size_t alignment, CUdeviceptr,
                                         unsigned long long) {
            operations.push_back("reserve");
            reserved_alignment = alignment;
            if (failure == Failure::RESERVE) return CUDA_ERROR_INVALID_VALUE;
            *address = kAddress;
            ++reservations;
            return CUDA_SUCCESS;
        };
        api.mem_map = [this](CUdeviceptr, size_t, size_t,
                             CUmemGenericAllocationHandle, unsigned long long) {
            operations.push_back("map");
            if (failure == Failure::MAP) return CUDA_ERROR_INVALID_VALUE;
            ++mappings;
            return CUDA_SUCCESS;
        };
        api.mem_set_access = [this](CUdeviceptr, size_t,
                                    const CUmemAccessDesc* access,
                                    size_t count) {
            operations.push_back("access");
            accesses.assign(access, access + count);
            if (failure == Failure::ACCESS) return CUDA_ERROR_INVALID_VALUE;
            return CUDA_SUCCESS;
        };
        api.mem_unmap = [this](CUdeviceptr, size_t) {
            operations.push_back("unmap");
            ++unmap_calls;
            if (unmap_failures > 0) {
                --unmap_failures;
                return CUDA_ERROR_INVALID_VALUE;
            }
            --mappings;
            return CUDA_SUCCESS;
        };
        api.mem_address_free = [this](CUdeviceptr, size_t) {
            operations.push_back("address_free");
            ++address_free_calls;
            if (address_free_failures > 0) {
                --address_free_failures;
                return CUDA_ERROR_INVALID_VALUE;
            }
            --reservations;
            return CUDA_SUCCESS;
        };
        api.mem_release = [this](CUmemGenericAllocationHandle) {
            operations.push_back("release");
            ++release_calls;
            if (release_failures > 0) {
                --release_failures;
                return CUDA_ERROR_INVALID_VALUE;
            }
            --handles;
            return CUDA_SUCCESS;
        };
        api.mem_retain_allocation_handle =
            [](CUmemGenericAllocationHandle* handle, void*) {
                *handle = 9;
                return CUDA_SUCCESS;
            };
        api.mem_export_to_shareable_handle =
            [](void*, CUmemGenericAllocationHandle, CUmemAllocationHandleType,
               unsigned long long) { return CUDA_SUCCESS; };
        return api;
    }

    static constexpr CUdeviceptr kAddress = 0x100000000ULL;
    Failure failure = Failure::NONE;
    std::vector<int> host_nodes{2, 0, 2};
    int vmm_supported = 1;
    int fabric_supported = 1;
    int host_numa_id_failure_device = -1;
    int granularity_failure_node = -1;
    size_t granularity = 64 * 1024;
    int unmap_failures = 0;
    int address_free_failures = 0;
    int release_failures = 0;
    int handles = 0;
    int reservations = 0;
    int mappings = 0;
    int unmap_calls = 0;
    int address_free_calls = 0;
    int release_calls = 0;
    size_t created_size = 0;
    size_t reserved_alignment = 0;
    CUmemAllocationProp last_prop = {};
    std::vector<CUmemAccessDesc> accesses;
    std::vector<std::string> operations;
};

TEST(NvlinkHostNumaAllocationTest, DiscoversSortedGpuLocalNodes) {
    FakeCudaDriver driver;
    std::vector<int> nodes;
    ASSERT_TRUE(
        NvlinkHostNumaAllocationTestPeer::Discover(driver.api(), nodes).ok());
    EXPECT_EQ(nodes, (std::vector<int>{0, 2}));

    driver.fabric_supported = 0;
    EXPECT_TRUE(NvlinkHostNumaAllocationTestPeer::Discover(driver.api(), nodes)
                    .IsNotSupportedTransport());
    EXPECT_TRUE(nodes.empty());
}

TEST(NvlinkHostNumaAllocationTest, AlignsAndGrantsCpuAndGpuAccess) {
    FakeCudaDriver driver;
    std::unique_ptr<NvlinkHostNumaAllocation> allocation;
    ASSERT_TRUE(NvlinkHostNumaAllocationTestPeer::Create(
                    2, 100000, 256 * 1024, driver.api(), allocation)
                    .ok());
    ASSERT_NE(allocation, nullptr);
    EXPECT_EQ(allocation->base(),
              reinterpret_cast<void*>(FakeCudaDriver::kAddress));
    EXPECT_EQ(allocation->length(), 256U * 1024);
    EXPECT_EQ(allocation->numaNode(), 2);
    EXPECT_EQ(driver.created_size, 256U * 1024);
    EXPECT_EQ(driver.reserved_alignment, 256U * 1024);
    EXPECT_EQ(driver.last_prop.location.type, CU_MEM_LOCATION_TYPE_HOST_NUMA);
    EXPECT_EQ(driver.last_prop.location.id, 2);
    EXPECT_EQ(driver.last_prop.requestedHandleTypes, CU_MEM_HANDLE_TYPE_FABRIC);
    ASSERT_EQ(driver.accesses.size(), 4U);
    EXPECT_EQ(driver.accesses.front().location.type,
              CU_MEM_LOCATION_TYPE_HOST_NUMA);
    EXPECT_TRUE(std::all_of(driver.accesses.begin(), driver.accesses.end(),
                            [](const CUmemAccessDesc& access) {
                                return access.flags ==
                                       CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
                            }));
    EXPECT_TRUE(NvlinkHostNumaAllocationTestPeer::IsExact(
        allocation->base(), allocation->length()));
    EXPECT_FALSE(NvlinkHostNumaAllocationTestPeer::IsExact(
        allocation->base(), allocation->length() / 2));
    EXPECT_TRUE(allocation->Release().ok());
    EXPECT_EQ(driver.handles, 0);
    EXPECT_EQ(driver.reservations, 0);
    EXPECT_EQ(driver.mappings, 0);
}

TEST(NvlinkHostNumaAllocationTest,
     ExplicitNodeIgnoresUnselectedLocalityAndGranularityFailures) {
    FakeCudaDriver locality_driver;
    locality_driver.host_numa_id_failure_device = 1;
    std::vector<int> nodes;
    EXPECT_FALSE(
        NvlinkHostNumaAllocationTestPeer::Discover(locality_driver.api(), nodes)
            .ok());

    std::unique_ptr<NvlinkHostNumaAllocation> allocation;
    ASSERT_TRUE(NvlinkHostNumaAllocationTestPeer::Create(
                    2, 64 * 1024, 0, locality_driver.api(), allocation)
                    .ok());
    ASSERT_NE(allocation, nullptr);
    ASSERT_EQ(locality_driver.accesses.size(), 4U);
    EXPECT_TRUE(allocation->Release().ok());

    FakeCudaDriver granularity_driver;
    granularity_driver.granularity_failure_node = 0;
    EXPECT_FALSE(NvlinkHostNumaAllocationTestPeer::Discover(
                     granularity_driver.api(), nodes)
                     .ok());
    ASSERT_TRUE(NvlinkHostNumaAllocationTestPeer::Create(
                    2, 64 * 1024, 0, granularity_driver.api(), allocation)
                    .ok());
    ASSERT_NE(allocation, nullptr);
    EXPECT_TRUE(allocation->Release().ok());
}

TEST(NvlinkHostNumaAllocationTest,
     ExplicitNodeStillRequiresVisibleGpuFabricCapabilities) {
    FakeCudaDriver driver;
    driver.fabric_supported = 0;
    std::unique_ptr<NvlinkHostNumaAllocation> allocation;
    EXPECT_TRUE(NvlinkHostNumaAllocationTestPeer::Create(
                    2, 64 * 1024, 0, driver.api(), allocation)
                    .IsNotSupportedTransport());
    EXPECT_EQ(allocation, nullptr);
    EXPECT_TRUE(driver.operations.empty());
}

TEST(NvlinkHostNumaAllocationTest, RollsBackEveryCreateStage) {
    for (auto failure :
         {FakeCudaDriver::Failure::GRANULARITY, FakeCudaDriver::Failure::CREATE,
          FakeCudaDriver::Failure::RESERVE, FakeCudaDriver::Failure::MAP,
          FakeCudaDriver::Failure::ACCESS}) {
        FakeCudaDriver driver;
        driver.failure = failure;
        std::unique_ptr<NvlinkHostNumaAllocation> allocation;
        EXPECT_FALSE(NvlinkHostNumaAllocationTestPeer::Create(
                         2, 64 * 1024, 0, driver.api(), allocation)
                         .ok());
        EXPECT_EQ(allocation, nullptr);
        EXPECT_EQ(driver.handles, 0);
        EXPECT_EQ(driver.reservations, 0);
        EXPECT_EQ(driver.mappings, 0);
        if (failure == FakeCudaDriver::Failure::MAP) {
            EXPECT_EQ(std::vector<std::string>(driver.operations.end() - 2,
                                               driver.operations.end()),
                      (std::vector<std::string>{"address_free", "release"}));
        }
        if (failure == FakeCudaDriver::Failure::ACCESS) {
            EXPECT_EQ(
                std::vector<std::string>(driver.operations.end() - 3,
                                         driver.operations.end()),
                (std::vector<std::string>{"unmap", "address_free", "release"}));
        }
    }
}

TEST(NvlinkHostNumaAllocationTest, IncompleteRollbackCanBeRetried) {
    FakeCudaDriver driver;
    driver.failure = FakeCudaDriver::Failure::ACCESS;
    driver.unmap_failures = 1;
    std::unique_ptr<NvlinkHostNumaAllocation> allocation;
    EXPECT_FALSE(NvlinkHostNumaAllocationTestPeer::Create(
                     2, 64 * 1024, 0, driver.api(), allocation)
                     .ok());
    ASSERT_NE(allocation, nullptr);
    driver.failure = FakeCudaDriver::Failure::NONE;
    EXPECT_TRUE(allocation->Release().ok());
    EXPECT_EQ(driver.unmap_calls, 2);
    EXPECT_EQ(driver.address_free_calls, 1);
    EXPECT_EQ(driver.release_calls, 1);
}

TEST(NvlinkHostNumaAllocationTest, ReleaseResumesWithoutRepeatingStages) {
    FakeCudaDriver driver;
    std::unique_ptr<NvlinkHostNumaAllocation> allocation;
    ASSERT_TRUE(NvlinkHostNumaAllocationTestPeer::Create(
                    2, 64 * 1024, 0, driver.api(), allocation)
                    .ok());
    driver.address_free_failures = 1;
    EXPECT_FALSE(allocation->Release().ok());
    EXPECT_EQ(driver.unmap_calls, 1);
    EXPECT_EQ(driver.address_free_calls, 1);
    EXPECT_EQ(driver.release_calls, 0);
    EXPECT_TRUE(allocation->Release().ok());
    EXPECT_EQ(driver.unmap_calls, 1);
    EXPECT_EQ(driver.address_free_calls, 2);
    EXPECT_EQ(driver.release_calls, 1);
}

TEST(NvlinkHostNumaAllocationTest, HandleReleaseFailureCanBeRetried) {
    FakeCudaDriver driver;
    std::unique_ptr<NvlinkHostNumaAllocation> allocation;
    ASSERT_TRUE(NvlinkHostNumaAllocationTestPeer::Create(
                    2, 64 * 1024, 0, driver.api(), allocation)
                    .ok());
    driver.release_failures = 1;
    EXPECT_FALSE(allocation->Release().ok());
    EXPECT_TRUE(allocation->Release().ok());
    EXPECT_EQ(driver.release_calls, 2);
}

}  // namespace

#else

TEST(NvlinkHostNumaAllocationTest, UnsupportedToolchainReturnsStatus) {
    std::vector<int> nodes;
    EXPECT_TRUE(NvlinkHostNumaAllocation::DiscoverHostNumaNodes(nodes)
                    .IsNotSupportedTransport());
}

#endif

}  // namespace mooncake
