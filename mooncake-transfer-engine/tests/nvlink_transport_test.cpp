#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <cstring>
#include <cstdlib>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/serialization.h"
#include "cuda_alike.h"
#include "error.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
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

namespace {

bool ParseStrictFabricRequirement(bool& strict, std::string& error) {
    strict = false;
    error.clear();
    const char* value = std::getenv("MC_REQUIRE_MNNVL_FABRIC");
    if (value == nullptr || std::string(value) == "0") return true;
    if (std::string(value) == "1") {
        strict = true;
        return true;
    }
    error = "MC_REQUIRE_MNNVL_FABRIC must be either 0 or 1";
    return false;
}

class ScopedEnvironmentVariable {
   public:
    ScopedEnvironmentVariable(const char* name, const char* value)
        : name_(name) {
        const char* previous = std::getenv(name);
        if (previous != nullptr) {
            had_previous_ = true;
            previous_ = previous;
        }
        if (value != nullptr) {
            setenv(name, value, 1);
        } else {
            unsetenv(name);
        }
    }

    ScopedEnvironmentVariable(const ScopedEnvironmentVariable&) = delete;
    ScopedEnvironmentVariable& operator=(const ScopedEnvironmentVariable&) =
        delete;

    ~ScopedEnvironmentVariable() {
        if (had_previous_) {
            setenv(name_.c_str(), previous_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::string previous_;
    bool had_previous_ = false;
};

}  // namespace

#if defined(USE_MNNVL) && defined(USE_CUDA)
namespace mooncake {

class NvlinkTransportTestPeer {
   public:
    using DriverApi = NvlinkVmmAllocation::DriverApi;

    static void configureFabric(
        NvlinkTransport& transport, DriverApi driver_api,
        std::function<int(const TransferMetadata::BufferDesc&, bool)> add = {},
        std::function<int(void*, bool)> remove = {},
        std::function<std::shared_ptr<TransferMetadata::SegmentDesc>(uint64_t)>
            get_segment = {}) {
        transport.use_fabric_mem_ = true;
        transport.fabric_driver_api_ = std::move(driver_api);
        transport.add_buffer_for_testing_ = std::move(add);
        transport.remove_buffer_for_testing_ = std::move(remove);
        transport.get_segment_for_testing_ = std::move(get_segment);
    }

    static int registerRemote(NvlinkTransport& transport, void* addr,
                              size_t length) {
        return transport.registerLocalMemory(addr, length, "cuda:0", true,
                                             true);
    }

    static int registerLocalOnly(NvlinkTransport& transport, void* addr,
                                 size_t length) {
        return transport.registerLocalMemory(addr, length, "cpu:0", false,
                                             true);
    }

    static int unregister(NvlinkTransport& transport, void* addr) {
        return transport.unregisterLocalMemory(addr, true);
    }

    static int relocate(NvlinkTransport& transport, uint64_t& addr,
                        uint64_t length, uint64_t target_id) {
        return transport.relocateSharedMemoryAddress(addr, length, target_id);
    }

    static size_t mappingCount(const NvlinkTransport& transport) {
        return transport.remap_entries_.size();
    }

    static size_t registrationCount(const NvlinkTransport& transport) {
        return transport.local_registrations_.size();
    }

    static size_t quarantinedRetainedHandleCount(
        const NvlinkTransport& transport) {
        return transport.quarantined_retained_handles_.size();
    }

    static size_t quarantinedFabricMappingCount(
        const NvlinkTransport& transport) {
        return transport.quarantined_fabric_mappings_.size();
    }

    static int publishIpcDescriptorForTesting(
        NvlinkTransport& transport, void* registration_addr,
        std::function<int(const TransferMetadata::BufferDesc&, bool)> add,
        std::function<int(void*, bool)> remove) {
        transport.use_fabric_mem_ = false;
        transport.add_buffer_for_testing_ = std::move(add);
        transport.remove_buffer_for_testing_ = std::move(remove);
        NvlinkTransport::LocalRegistration registration;
        registration.requested_addr = registration_addr;
        registration.requested_length = 4096;
        registration.mapped_base = registration_addr;
        registration.mapped_length = 4096;
        registration.remote_accessible = true;
        transport.local_registrations_.emplace(registration_addr, registration);
        TransferMetadata::BufferDesc descriptor;
        descriptor.addr = reinterpret_cast<uint64_t>(registration_addr);
        descriptor.length = 4096;
        descriptor.shm_name = "fake-ipc-handle";
        return transport.publishLocalRegistration(registration_addr, descriptor,
                                                  true);
    }

    static std::unique_ptr<NvlinkVmmAllocation> makeOwnedHostNumaRange(
        void* base, size_t length) {
        std::unique_ptr<NvlinkVmmAllocation> allocation(
            new NvlinkVmmAllocation());
        allocation->base_ = base;
        allocation->length_ = length;
        allocation->location_type_ =
            NvlinkVmmAllocation::LocationType::HOST_NUMA;
        allocation->fabric_exportable_ = true;
        allocation->owned_range_registered_ =
            NvlinkVmmAllocation::RegisterOwnedRange(base, length);
        if (!allocation->owned_range_registered_) return nullptr;
        return allocation;
    }
};

}  // namespace mooncake

namespace {

class FakeFabricDriver {
   public:
    enum class Failure {
        NONE,
        ALLOCATION_PROPERTIES,
        ADDRESS_RANGE,
        EXPORT,
        IMPORT,
        RESERVE,
        MAP,
        ACCESS,
        POST_MAP_RELEASE,
    };

    NvlinkTransportTestPeer::DriverApi api() {
        NvlinkTransportTestPeer::DriverApi api;
        api.device_get_count = [](int* count) {
            *count = 2;
            return CUDA_SUCCESS;
        };
        api.device_get = [](CUdevice* device, int ordinal) {
            *device = ordinal;
            return CUDA_SUCCESS;
        };
        api.device_get_attribute = [](int* value, CUdevice_attribute attribute,
                                      CUdevice) {
            *value =
                attribute == CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED
                    ? 1
                    : 0;
            return CUDA_SUCCESS;
        };
        api.mem_retain_allocation_handle =
            [this](CUmemGenericAllocationHandle* handle, void*) {
                *handle = kRetainedHandle;
                ++live_handles;
                return CUDA_SUCCESS;
            };
        api.mem_get_allocation_properties_from_handle =
            [this](CUmemAllocationProp* prop, CUmemGenericAllocationHandle) {
                if (failure == Failure::ALLOCATION_PROPERTIES)
                    return CUDA_ERROR_INVALID_HANDLE;
                *prop = {};
                prop->location.type = allocation_location_type;
                return CUDA_SUCCESS;
            };
        api.mem_get_address_range = [this](CUdeviceptr* base, size_t* length,
                                           CUdeviceptr) {
            ++address_range_calls;
            if (failure == Failure::ADDRESS_RANGE)
                return CUDA_ERROR_INVALID_CONTEXT;
            *base = kPublishedBase;
            *length = kMappedLength;
            return CUDA_SUCCESS;
        };
        api.mem_export_to_shareable_handle =
            [this](void* shareable, CUmemGenericAllocationHandle,
                   CUmemAllocationHandleType, unsigned long long) {
                if (failure == Failure::EXPORT)
                    return CUDA_ERROR_INVALID_HANDLE;
                std::memset(shareable, 0, sizeof(CUmemFabricHandle));
                return CUDA_SUCCESS;
            };
        api.mem_import_from_shareable_handle =
            [this](CUmemGenericAllocationHandle* handle, void*,
                   CUmemAllocationHandleType) {
                ++import_calls;
                if (failure == Failure::IMPORT) return CUDA_ERROR_INVALID_VALUE;
                *handle = kImportedHandle;
                ++live_handles;
                return CUDA_SUCCESS;
            };
        api.mem_address_reserve = [this](CUdeviceptr* address, size_t, size_t,
                                         CUdeviceptr, unsigned long long) {
            if (failure == Failure::RESERVE) return CUDA_ERROR_INVALID_VALUE;
            *address = kImportedBase;
            ++reserved_ranges;
            return CUDA_SUCCESS;
        };
        api.mem_map = [this](CUdeviceptr, size_t, size_t,
                             CUmemGenericAllocationHandle, unsigned long long) {
            if (failure == Failure::MAP) return CUDA_ERROR_INVALID_VALUE;
            ++mapped_ranges;
            return CUDA_SUCCESS;
        };
        api.mem_set_access = [this](CUdeviceptr, size_t, const CUmemAccessDesc*,
                                    size_t) {
            ++access_calls;
            if (failure == Failure::ACCESS) return CUDA_ERROR_INVALID_VALUE;
            return CUDA_SUCCESS;
        };
        api.mem_unmap = [this](CUdeviceptr, size_t) {
            ++unmap_calls;
            if (unmap_failures_remaining > 0) {
                --unmap_failures_remaining;
                return CUDA_ERROR_INVALID_VALUE;
            }
            if (mapped_ranges > 0) --mapped_ranges;
            return CUDA_SUCCESS;
        };
        api.mem_address_free = [this](CUdeviceptr, size_t) {
            ++address_free_calls;
            if (address_free_failures_remaining > 0) {
                --address_free_failures_remaining;
                return CUDA_ERROR_INVALID_VALUE;
            }
            if (reserved_ranges > 0) --reserved_ranges;
            return CUDA_SUCCESS;
        };
        api.mem_release = [this](CUmemGenericAllocationHandle handle) {
            ++release_calls;
            if (handle == kRetainedHandle &&
                retained_release_failures_remaining > 0) {
                --retained_release_failures_remaining;
                return CUDA_ERROR_INVALID_VALUE;
            }
            if (handle == kImportedHandle) {
                ++imported_release_calls;
                if (imported_release_failures_remaining > 0) {
                    --imported_release_failures_remaining;
                    return CUDA_ERROR_INVALID_VALUE;
                }
            }
            if (failure == Failure::POST_MAP_RELEASE &&
                handle == kImportedHandle && !release_failed_once) {
                release_failed_once = true;
                return CUDA_ERROR_INVALID_VALUE;
            }
            if (live_handles > 0) --live_handles;
            return CUDA_SUCCESS;
        };
        return api;
    }

    void expectNoResources() const {
        EXPECT_EQ(live_handles, 0);
        EXPECT_EQ(reserved_ranges, 0);
        EXPECT_EQ(mapped_ranges, 0);
    }

    static constexpr CUmemGenericAllocationHandle kRetainedHandle = 101;
    static constexpr CUmemGenericAllocationHandle kImportedHandle = 202;
    static constexpr CUdeviceptr kPublishedBase = 0x10000000ULL;
    static constexpr CUdeviceptr kImportedBase = 0x20000000ULL;
    static constexpr size_t kMappedLength = 64 * 1024;

    Failure failure = Failure::NONE;
    CUmemLocationType allocation_location_type = CU_MEM_LOCATION_TYPE_DEVICE;
    int live_handles = 0;
    int reserved_ranges = 0;
    int mapped_ranges = 0;
    int import_calls = 0;
    int address_range_calls = 0;
    int access_calls = 0;
    int release_calls = 0;
    int unmap_calls = 0;
    int address_free_calls = 0;
    int imported_release_calls = 0;
    int unmap_failures_remaining = 0;
    int address_free_failures_remaining = 0;
    int imported_release_failures_remaining = 0;
    int retained_release_failures_remaining = 0;
    bool release_failed_once = false;
};

std::shared_ptr<TransferMetadata::SegmentDesc> fakeFabricSegment() {
    auto segment = std::make_shared<TransferMetadata::SegmentDesc>();
    segment->name = "fake-provider";
    segment->protocol = "nvlink";
    TransferMetadata::BufferDesc buffer;
    buffer.addr = FakeFabricDriver::kPublishedBase;
    buffer.length = FakeFabricDriver::kMappedLength;
    CUmemFabricHandle handle = {};
    buffer.shm_name = serializeBinaryData(&handle, sizeof(handle));
    segment->buffers.push_back(std::move(buffer));
    return segment;
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

static void* allocatePublishedBuffer(size_t size, int gpu_id) {
#ifdef USE_CUDA
    if (std::getenv("MC_USE_NVLINK_IPC") != nullptr)
        return allocateCudaBuffer(size, gpu_id);
    checkCudaError(cudaSetDevice(gpu_id), "Failed to set device");
    return NvlinkTransport::allocatePinnedLocalMemory(size);
#else
    return allocateCudaBuffer(size, gpu_id);
#endif
}

static void freePublishedBuffer(void* addr) {
#ifdef USE_CUDA
    if (std::getenv("MC_USE_NVLINK_IPC") != nullptr)
        freeCudaBuffer(addr);
    else
        NvlinkTransport::freePinnedLocalMemory(addr);
#else
    freeCudaBuffer(addr);
#endif
}

#if defined(USE_MNNVL) && defined(USE_CUDA)
TEST(NvlinkTransportUnitTest, StrictFabricRequirementParsingIsExact) {
    bool strict = true;
    std::string error = "stale";
    {
        ScopedEnvironmentVariable unset_strict("MC_REQUIRE_MNNVL_FABRIC",
                                               nullptr);
        EXPECT_TRUE(ParseStrictFabricRequirement(strict, error));
        EXPECT_FALSE(strict);
        EXPECT_TRUE(error.empty());
    }
    {
        ScopedEnvironmentVariable disabled_strict("MC_REQUIRE_MNNVL_FABRIC",
                                                  "0");
        EXPECT_TRUE(ParseStrictFabricRequirement(strict, error));
        EXPECT_FALSE(strict);
        EXPECT_TRUE(error.empty());
    }
    {
        ScopedEnvironmentVariable enabled_strict("MC_REQUIRE_MNNVL_FABRIC",
                                                 "1");
        EXPECT_TRUE(ParseStrictFabricRequirement(strict, error));
        EXPECT_TRUE(strict);
        EXPECT_TRUE(error.empty());
    }
    {
        ScopedEnvironmentVariable invalid_strict("MC_REQUIRE_MNNVL_FABRIC",
                                                 "true");
        EXPECT_FALSE(ParseStrictFabricRequirement(strict, error));
        EXPECT_FALSE(strict);
        EXPECT_EQ(error, "MC_REQUIRE_MNNVL_FABRIC must be either 0 or 1");
    }
}

TEST(NvlinkTransportUnitTest, LocalOnlyCpuRegistrationNeverPublishesMetadata) {
    NvlinkTransport transport;
    int add_calls = 0;
    int remove_calls = 0;
    NvlinkTransportTestPeer::configureFabric(
        transport, {},
        [&](const TransferMetadata::BufferDesc&, bool) {
            ++add_calls;
            return 0;
        },
        [&](void*, bool) {
            ++remove_calls;
            return 0;
        });

    std::vector<char> cpu_buffer(4096);
    ASSERT_EQ(NvlinkTransportTestPeer::registerLocalOnly(
                  transport, cpu_buffer.data(), cpu_buffer.size()),
              0);
    EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 1);
    EXPECT_EQ(add_calls, 0);
    EXPECT_EQ(remove_calls, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::registerLocalOnly(
                  transport, cpu_buffer.data(), cpu_buffer.size()),
              ERR_ADDRESS_OVERLAPPED);

    EXPECT_EQ(NvlinkTransportTestPeer::unregister(transport, cpu_buffer.data()),
              0);
    EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 0);
    EXPECT_EQ(add_calls, 0);
    EXPECT_EQ(remove_calls, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::unregister(transport, cpu_buffer.data()),
              ERR_ADDRESS_NOT_REGISTERED);
}

TEST(NvlinkTransportUnitTest, LazyImportFailuresCleanUpAndRetry) {
    const std::vector<FakeFabricDriver::Failure> failures = {
        FakeFabricDriver::Failure::IMPORT,
        FakeFabricDriver::Failure::RESERVE,
        FakeFabricDriver::Failure::MAP,
        FakeFabricDriver::Failure::ACCESS,
        FakeFabricDriver::Failure::POST_MAP_RELEASE,
    };

    for (auto failure : failures) {
        FakeFabricDriver driver;
        {
            NvlinkTransport transport;
            NvlinkTransportTestPeer::configureFabric(
                transport, driver.api(), {}, {},
                [](uint64_t) { return fakeFabricSegment(); });
            driver.failure = failure;

            uint64_t address = FakeFabricDriver::kPublishedBase + 128;
            EXPECT_NE(
                NvlinkTransportTestPeer::relocate(transport, address, 256, 7),
                0);
            EXPECT_EQ(NvlinkTransportTestPeer::mappingCount(transport), 0);
            driver.expectNoResources();

            driver.failure = FakeFabricDriver::Failure::NONE;
            address = FakeFabricDriver::kPublishedBase + 128;
            ASSERT_EQ(
                NvlinkTransportTestPeer::relocate(transport, address, 256, 7),
                0);
            EXPECT_EQ(address, FakeFabricDriver::kImportedBase + 128);
            EXPECT_EQ(NvlinkTransportTestPeer::mappingCount(transport), 1);
            EXPECT_EQ(driver.live_handles, 0);
            EXPECT_EQ(driver.reserved_ranges, 1);
            EXPECT_EQ(driver.mapped_ranges, 1);

            const int imports_after_success = driver.import_calls;
            address = FakeFabricDriver::kPublishedBase + 256;
            ASSERT_EQ(
                NvlinkTransportTestPeer::relocate(transport, address, 128, 7),
                0);
            EXPECT_EQ(address, FakeFabricDriver::kImportedBase + 256);
            EXPECT_EQ(driver.import_calls, imports_after_success);
        }
        driver.expectNoResources();
    }
}

TEST(NvlinkTransportUnitTest,
     LazyImportCleanupIsStagedQuarantinedAndRetriedBeforeNewImport) {
    enum class CleanupFailure { UNMAP, ADDRESS_FREE, RELEASE };
    struct FailureCase {
        const char* name;
        CleanupFailure failure;
        int expected_unmap_calls_after_first;
        int expected_address_free_calls_after_first;
        int expected_release_calls_after_first;
        int expected_unmap_calls_after_retry;
        int expected_address_free_calls_after_retry;
        int expected_release_calls_after_retry;
    };
    const std::vector<FailureCase> cases = {
        {"unmap", CleanupFailure::UNMAP, 1, 0, 0, 2, 0, 0},
        {"address_free", CleanupFailure::ADDRESS_FREE, 1, 1, 0, 1, 2, 0},
        {"release", CleanupFailure::RELEASE, 1, 1, 1, 1, 1, 2},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        FakeFabricDriver driver;
        {
            NvlinkTransport transport;
            NvlinkTransportTestPeer::configureFabric(
                transport, driver.api(), {}, {},
                [](uint64_t) { return fakeFabricSegment(); });
            driver.failure = FakeFabricDriver::Failure::ACCESS;
            switch (test_case.failure) {
                case CleanupFailure::UNMAP:
                    driver.unmap_failures_remaining = 2;
                    break;
                case CleanupFailure::ADDRESS_FREE:
                    driver.address_free_failures_remaining = 2;
                    break;
                case CleanupFailure::RELEASE:
                    driver.imported_release_failures_remaining = 2;
                    break;
            }

            uint64_t address = FakeFabricDriver::kPublishedBase + 128;
            EXPECT_NE(
                NvlinkTransportTestPeer::relocate(transport, address, 256, 7),
                0);
            EXPECT_EQ(NvlinkTransportTestPeer::mappingCount(transport), 0);
            EXPECT_EQ(NvlinkTransportTestPeer::quarantinedFabricMappingCount(
                          transport),
                      1);
            EXPECT_EQ(driver.import_calls, 1);
            EXPECT_EQ(driver.unmap_calls,
                      test_case.expected_unmap_calls_after_first);
            EXPECT_EQ(driver.address_free_calls,
                      test_case.expected_address_free_calls_after_first);
            EXPECT_EQ(driver.imported_release_calls,
                      test_case.expected_release_calls_after_first);

            // A persistent earlier cleanup failure must stop at that stage and
            // block a second import rather than accumulating more CUDA state.
            address = FakeFabricDriver::kPublishedBase + 128;
            EXPECT_NE(
                NvlinkTransportTestPeer::relocate(transport, address, 256, 7),
                0);
            EXPECT_EQ(driver.import_calls, 1);
            EXPECT_EQ(NvlinkTransportTestPeer::quarantinedFabricMappingCount(
                          transport),
                      1);
            EXPECT_EQ(driver.unmap_calls,
                      test_case.expected_unmap_calls_after_retry);
            EXPECT_EQ(driver.address_free_calls,
                      test_case.expected_address_free_calls_after_retry);
            EXPECT_EQ(driver.imported_release_calls,
                      test_case.expected_release_calls_after_retry);

            driver.failure = FakeFabricDriver::Failure::NONE;
            driver.unmap_failures_remaining = 0;
            driver.address_free_failures_remaining = 0;
            driver.imported_release_failures_remaining = 0;
            address = FakeFabricDriver::kPublishedBase + 128;
            ASSERT_EQ(
                NvlinkTransportTestPeer::relocate(transport, address, 256, 7),
                0);
            EXPECT_EQ(address, FakeFabricDriver::kImportedBase + 128);
            EXPECT_EQ(driver.import_calls, 2);
            EXPECT_EQ(NvlinkTransportTestPeer::quarantinedFabricMappingCount(
                          transport),
                      0);
            EXPECT_EQ(driver.live_handles, 0);
            EXPECT_EQ(driver.reserved_ranges, 1);
            EXPECT_EQ(driver.mapped_ranges, 1);
        }
        driver.expectNoResources();
    }
}

TEST(NvlinkTransportUnitTest,
     CachedFabricMappingTeardownRetriesWithoutFreeingMappedAddress) {
    FakeFabricDriver driver;
    {
        NvlinkTransport transport;
        NvlinkTransportTestPeer::configureFabric(
            transport, driver.api(), {}, {},
            [](uint64_t) { return fakeFabricSegment(); });

        uint64_t address = FakeFabricDriver::kPublishedBase + 128;
        ASSERT_EQ(NvlinkTransportTestPeer::relocate(transport, address, 256, 7),
                  0);
        ASSERT_EQ(NvlinkTransportTestPeer::mappingCount(transport), 1);
        driver.unmap_failures_remaining = 1;
    }

    EXPECT_EQ(driver.unmap_calls, 2);
    EXPECT_EQ(driver.address_free_calls, 1);
    driver.expectNoResources();
}

TEST(NvlinkTransportUnitTest,
     RegistrationMetadataFailureRollsBackDescriptorAndHandle) {
    FakeFabricDriver driver;
    int add_calls = 0;
    int remove_calls = 0;
    bool fail_metadata_write = true;
    bool descriptor_present = false;
    bool rollback_requested_metadata_update = false;
    TransferMetadata::BufferDesc last_descriptor;
    {
        NvlinkTransport transport;
        NvlinkTransportTestPeer::configureFabric(
            transport, driver.api(),
            [&](const TransferMetadata::BufferDesc& descriptor,
                bool update_metadata) {
                ++add_calls;
                EXPECT_TRUE(update_metadata);
                last_descriptor = descriptor;
                descriptor_present = true;
                return fail_metadata_write ? ERR_METADATA : 0;
            },
            [&](void* descriptor_addr, bool update_metadata) {
                ++remove_calls;
                EXPECT_EQ(
                    descriptor_addr,
                    reinterpret_cast<void*>(FakeFabricDriver::kPublishedBase));
                descriptor_present = false;
                rollback_requested_metadata_update = update_metadata;
                return 0;
            });

        void* address =
            reinterpret_cast<void*>(FakeFabricDriver::kPublishedBase + 4096);
        EXPECT_EQ(
            NvlinkTransportTestPeer::registerRemote(transport, address, 4096),
            ERR_METADATA);
        EXPECT_EQ(add_calls, 1);
        EXPECT_EQ(remove_calls, 1);
        EXPECT_EQ(last_descriptor.addr, FakeFabricDriver::kPublishedBase);
        EXPECT_EQ(last_descriptor.length, FakeFabricDriver::kMappedLength);
        EXPECT_FALSE(descriptor_present);
        EXPECT_TRUE(rollback_requested_metadata_update);
        EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 0);
        EXPECT_EQ(driver.release_calls, 1);
        driver.expectNoResources();

        fail_metadata_write = false;
        ASSERT_EQ(
            NvlinkTransportTestPeer::registerRemote(transport, address, 4096),
            0);
        EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 1);
        EXPECT_TRUE(descriptor_present);
        EXPECT_EQ(driver.live_handles, 1);
        EXPECT_EQ(NvlinkTransportTestPeer::unregister(transport, address), 0);
        EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 0);
        EXPECT_EQ(remove_calls, 2);
        EXPECT_FALSE(descriptor_present);
        EXPECT_EQ(driver.release_calls, 2);
        driver.expectNoResources();
    }
    driver.expectNoResources();
}

TEST(NvlinkTransportUnitTest,
     RegistrationMetadataExceptionRetainsSingleHandleOwnerUntilUnregister) {
    FakeFabricDriver driver;
    bool descriptor_present = false;
    bool throw_unregistration = true;
    int remove_calls = 0;
    {
        NvlinkTransport transport;
        NvlinkTransportTestPeer::configureFabric(
            transport, driver.api(),
            [&](const TransferMetadata::BufferDesc&, bool) -> int {
                descriptor_present = true;
                throw std::runtime_error("injected metadata publication");
            },
            [&](void*, bool) {
                ++remove_calls;
                if (throw_unregistration) {
                    throw std::runtime_error(
                        "injected metadata unregistration");
                }
                descriptor_present = false;
                return 0;
            });

        void* address =
            reinterpret_cast<void*>(FakeFabricDriver::kPublishedBase);
        EXPECT_EQ(NvlinkTransportTestPeer::registerRemote(
                      transport, address, FakeFabricDriver::kMappedLength),
                  ERR_MEMORY);
        EXPECT_EQ(remove_calls, 0)
            << "unknown remote publication state must not release eagerly";
        EXPECT_TRUE(descriptor_present);
        EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 1);
        EXPECT_EQ(driver.release_calls, 0)
            << "the retained record, not the guard, owns the live handle";
        EXPECT_EQ(driver.live_handles, 1);

        EXPECT_EQ(NvlinkTransportTestPeer::unregister(transport, address),
                  ERR_MEMORY);
        EXPECT_TRUE(descriptor_present);
        EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 1);
        EXPECT_EQ(driver.release_calls, 0)
            << "a throwing deletion must preserve the retained handle";

        throw_unregistration = false;
        ASSERT_EQ(NvlinkTransportTestPeer::unregister(transport, address), 0);
        EXPECT_EQ(remove_calls, 2);
        EXPECT_FALSE(descriptor_present);
        EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 0);
        EXPECT_EQ(driver.release_calls, 1);
        driver.expectNoResources();
    }
    EXPECT_EQ(driver.release_calls, 1);
    driver.expectNoResources();
}

TEST(NvlinkTransportUnitTest,
     RegistrationErrorCleanupQuarantinesFailedHandleReleaseAndRetries) {
    struct FailureCase {
        const char* name;
        FakeFabricDriver::Failure driver_failure;
        bool metadata_failure;
        int expected_error;
    };
    const std::vector<FailureCase> cases = {
        {"allocation_properties",
         FakeFabricDriver::Failure::ALLOCATION_PROPERTIES, false, ERR_MEMORY},
        {"address_range", FakeFabricDriver::Failure::ADDRESS_RANGE, false,
         ERR_MEMORY},
        {"export", FakeFabricDriver::Failure::EXPORT, false, ERR_MEMORY},
        {"metadata", FakeFabricDriver::Failure::NONE, true, ERR_METADATA},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        FakeFabricDriver driver;
        driver.failure = test_case.driver_failure;
        driver.retained_release_failures_remaining = 1;
        bool metadata_failure = test_case.metadata_failure;
        bool descriptor_present = false;

        NvlinkTransport transport;
        NvlinkTransportTestPeer::configureFabric(
            transport, driver.api(),
            [&](const TransferMetadata::BufferDesc&, bool) {
                descriptor_present = true;
                return metadata_failure ? ERR_METADATA : 0;
            },
            [&](void*, bool) {
                descriptor_present = false;
                return 0;
            });

        void* address =
            reinterpret_cast<void*>(FakeFabricDriver::kPublishedBase);
        EXPECT_EQ(NvlinkTransportTestPeer::registerRemote(
                      transport, address, FakeFabricDriver::kMappedLength),
                  test_case.expected_error);
        EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 0);
        EXPECT_EQ(
            NvlinkTransportTestPeer::quarantinedRetainedHandleCount(transport),
            1);
        EXPECT_FALSE(descriptor_present);
        EXPECT_EQ(driver.live_handles, 1)
            << "failed cuMemRelease must retain handle ownership";

        driver.failure = FakeFabricDriver::Failure::NONE;
        metadata_failure = false;
        ASSERT_EQ(NvlinkTransportTestPeer::registerRemote(
                      transport, address, FakeFabricDriver::kMappedLength),
                  0);
        EXPECT_EQ(
            NvlinkTransportTestPeer::quarantinedRetainedHandleCount(transport),
            0);
        EXPECT_TRUE(descriptor_present);
        EXPECT_EQ(driver.live_handles, 1);

        ASSERT_EQ(NvlinkTransportTestPeer::unregister(transport, address), 0);
        EXPECT_FALSE(descriptor_present);
        EXPECT_EQ(driver.release_calls, 3);
        driver.expectNoResources();
    }
}

TEST(NvlinkTransportUnitTest,
     RegistrationMetadataRollbackFailureRetainsStateForCallerRetry) {
    FakeFabricDriver driver;
    bool descriptor_present = false;
    int remove_calls = 0;

    NvlinkTransport transport;
    NvlinkTransportTestPeer::configureFabric(
        transport, driver.api(),
        [&](const TransferMetadata::BufferDesc&, bool) {
            descriptor_present = true;
            return ERR_METADATA;
        },
        [&](void*, bool) {
            ++remove_calls;
            if (remove_calls == 1) return ERR_METADATA;
            descriptor_present = false;
            return 0;
        });

    void* address = reinterpret_cast<void*>(FakeFabricDriver::kPublishedBase);
    EXPECT_EQ(NvlinkTransportTestPeer::registerRemote(
                  transport, address, FakeFabricDriver::kMappedLength),
              ERR_METADATA);
    EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 1);
    EXPECT_EQ(
        NvlinkTransportTestPeer::quarantinedRetainedHandleCount(transport), 0);
    EXPECT_TRUE(descriptor_present);
    EXPECT_EQ(driver.live_handles, 1);
    EXPECT_EQ(driver.release_calls, 0);

    ASSERT_EQ(NvlinkTransportTestPeer::unregister(transport, address), 0);
    EXPECT_EQ(remove_calls, 2);
    EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 0);
    EXPECT_FALSE(descriptor_present);
    EXPECT_EQ(driver.release_calls, 1);
    driver.expectNoResources();
}

TEST(NvlinkTransportUnitTest,
     IpcMetadataRollbackFailureRetainsStateForCallerRetry) {
    bool descriptor_present = false;
    int remove_calls = 0;
    void* address = reinterpret_cast<void*>(0xabc000);

    NvlinkTransport transport;
    EXPECT_EQ(NvlinkTransportTestPeer::publishIpcDescriptorForTesting(
                  transport, address,
                  [&](const TransferMetadata::BufferDesc&, bool) {
                      descriptor_present = true;
                      return ERR_METADATA;
                  },
                  [&](void*, bool) {
                      ++remove_calls;
                      if (remove_calls == 1) return ERR_METADATA;
                      descriptor_present = false;
                      return 0;
                  }),
              ERR_METADATA);
    EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 1);
    EXPECT_TRUE(descriptor_present);

    ASSERT_EQ(NvlinkTransportTestPeer::unregister(transport, address), 0);
    EXPECT_EQ(remove_calls, 2);
    EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 0);
    EXPECT_FALSE(descriptor_present);
}

TEST(NvlinkTransportUnitTest,
     DestructorPersistentReleaseFailureKeepsHandleLiveForProcessLifetime) {
    FakeFabricDriver driver;
    driver.retained_release_failures_remaining = 2;
    {
        NvlinkTransport transport;
        NvlinkTransportTestPeer::configureFabric(
            transport, driver.api(),
            [](const TransferMetadata::BufferDesc&, bool) { return 0; },
            [](void*, bool) { return 0; });
        void* address =
            reinterpret_cast<void*>(FakeFabricDriver::kPublishedBase);
        ASSERT_EQ(NvlinkTransportTestPeer::registerRemote(
                      transport, address, FakeFabricDriver::kMappedLength),
                  0);
        EXPECT_EQ(driver.live_handles, 1);
    }
    EXPECT_EQ(driver.release_calls, 2);
    EXPECT_EQ(driver.live_handles, 1)
        << "persistent teardown failure must leave the CUDA handle live and "
           "owned by the process-lifetime quarantine";
}

TEST(NvlinkTransportUnitTest, OwnedHostNumaRegistrationSkipsAddressRangeQuery) {
    FakeFabricDriver driver;
    driver.failure = FakeFabricDriver::Failure::ADDRESS_RANGE;
    driver.allocation_location_type = CU_MEM_LOCATION_TYPE_HOST_NUMA;
    int add_calls = 0;
    TransferMetadata::BufferDesc published;
    auto owner = NvlinkTransportTestPeer::makeOwnedHostNumaRange(
        reinterpret_cast<void*>(FakeFabricDriver::kPublishedBase),
        FakeFabricDriver::kMappedLength);
    ASSERT_NE(owner, nullptr);

    NvlinkTransport transport;
    NvlinkTransportTestPeer::configureFabric(
        transport, driver.api(),
        [&](const TransferMetadata::BufferDesc& descriptor, bool) {
            ++add_calls;
            published = descriptor;
            return 0;
        },
        [](void*, bool) { return 0; });

    void* address = reinterpret_cast<void*>(FakeFabricDriver::kPublishedBase);
    ASSERT_EQ(NvlinkTransportTestPeer::registerRemote(
                  transport, address, FakeFabricDriver::kMappedLength),
              0);
    EXPECT_EQ(add_calls, 1);
    EXPECT_EQ(published.addr, FakeFabricDriver::kPublishedBase);
    EXPECT_EQ(published.length, FakeFabricDriver::kMappedLength);
    EXPECT_EQ(driver.address_range_calls, 0);
    EXPECT_EQ(driver.live_handles, 1);

    ASSERT_EQ(NvlinkTransportTestPeer::unregister(transport, address), 0);
    driver.expectNoResources();
}

TEST(NvlinkTransportUnitTest,
     OwnedHostNumaRegistrationRejectsInteriorSubrange) {
    FakeFabricDriver driver;
    driver.allocation_location_type = CU_MEM_LOCATION_TYPE_HOST_NUMA;
    int add_calls = 0;
    auto owner = NvlinkTransportTestPeer::makeOwnedHostNumaRange(
        reinterpret_cast<void*>(FakeFabricDriver::kPublishedBase),
        FakeFabricDriver::kMappedLength);
    ASSERT_NE(owner, nullptr);

    NvlinkTransport transport;
    NvlinkTransportTestPeer::configureFabric(
        transport, driver.api(),
        [&](const TransferMetadata::BufferDesc&, bool) {
            ++add_calls;
            return 0;
        });

    constexpr size_t kInteriorOffset = 4096;
    void* interior = reinterpret_cast<void*>(FakeFabricDriver::kPublishedBase +
                                             kInteriorOffset);
    EXPECT_EQ(NvlinkTransportTestPeer::registerRemote(
                  transport, interior,
                  FakeFabricDriver::kMappedLength - kInteriorOffset),
              ERR_INVALID_ARGUMENT);
    EXPECT_EQ(add_calls, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 0);
    EXPECT_EQ(driver.address_range_calls, 1);
    EXPECT_EQ(driver.release_calls, 1);
    driver.expectNoResources();
}

TEST(NvlinkTransportUnitTest,
     ExternalHostNumaRegistrationRejectsAddressRangeQueryFailure) {
    FakeFabricDriver driver;
    driver.failure = FakeFabricDriver::Failure::ADDRESS_RANGE;
    driver.allocation_location_type = CU_MEM_LOCATION_TYPE_HOST_NUMA;
    int add_calls = 0;

    NvlinkTransport transport;
    NvlinkTransportTestPeer::configureFabric(
        transport, driver.api(),
        [&](const TransferMetadata::BufferDesc&, bool) {
            ++add_calls;
            return 0;
        });

    void* address = reinterpret_cast<void*>(FakeFabricDriver::kPublishedBase);
    EXPECT_EQ(NvlinkTransportTestPeer::registerRemote(
                  transport, address, FakeFabricDriver::kMappedLength),
              ERR_MEMORY);
    EXPECT_EQ(add_calls, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 0);
    EXPECT_EQ(driver.address_range_calls, 1);
    EXPECT_EQ(driver.release_calls, 1);
    driver.expectNoResources();
}

TEST(NvlinkTransportUnitTest,
     ExternalHostNumaRegistrationUsesSuccessfulAddressRangeQuery) {
    FakeFabricDriver driver;
    driver.allocation_location_type = CU_MEM_LOCATION_TYPE_HOST_NUMA;
    int add_calls = 0;
    TransferMetadata::BufferDesc published;

    NvlinkTransport transport;
    NvlinkTransportTestPeer::configureFabric(
        transport, driver.api(),
        [&](const TransferMetadata::BufferDesc& descriptor, bool) {
            ++add_calls;
            published = descriptor;
            return 0;
        },
        [](void*, bool) { return 0; });

    void* address = reinterpret_cast<void*>(FakeFabricDriver::kPublishedBase);
    ASSERT_EQ(NvlinkTransportTestPeer::registerRemote(
                  transport, address, FakeFabricDriver::kMappedLength),
              0);
    EXPECT_EQ(add_calls, 1);
    EXPECT_EQ(driver.address_range_calls, 1);
    EXPECT_EQ(published.addr, FakeFabricDriver::kPublishedBase);
    EXPECT_EQ(published.length, FakeFabricDriver::kMappedLength);
    EXPECT_EQ(driver.live_handles, 1);

    ASSERT_EQ(NvlinkTransportTestPeer::unregister(transport, address), 0);
    driver.expectNoResources();
}

TEST(NvlinkTransportUnitTest,
     RegistrationPropertyQueryFailureReleasesRetainedHandle) {
    FakeFabricDriver driver;
    driver.failure = FakeFabricDriver::Failure::ALLOCATION_PROPERTIES;
    int add_calls = 0;

    NvlinkTransport transport;
    NvlinkTransportTestPeer::configureFabric(
        transport, driver.api(),
        [&](const TransferMetadata::BufferDesc&, bool) {
            ++add_calls;
            return 0;
        });

    void* address = reinterpret_cast<void*>(FakeFabricDriver::kPublishedBase);
    EXPECT_EQ(NvlinkTransportTestPeer::registerRemote(
                  transport, address, FakeFabricDriver::kMappedLength),
              ERR_MEMORY);
    EXPECT_EQ(add_calls, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 0);
    EXPECT_EQ(driver.release_calls, 1);
    driver.expectNoResources();
}

TEST(NvlinkTransportUnitTest,
     DeviceRegistrationStillRejectsAddressRangeQueryFailure) {
    FakeFabricDriver driver;
    driver.failure = FakeFabricDriver::Failure::ADDRESS_RANGE;
    driver.allocation_location_type = CU_MEM_LOCATION_TYPE_DEVICE;
    int add_calls = 0;

    NvlinkTransport transport;
    NvlinkTransportTestPeer::configureFabric(
        transport, driver.api(),
        [&](const TransferMetadata::BufferDesc&, bool) {
            ++add_calls;
            return 0;
        });

    void* address = reinterpret_cast<void*>(FakeFabricDriver::kPublishedBase);
    EXPECT_EQ(NvlinkTransportTestPeer::registerRemote(
                  transport, address, FakeFabricDriver::kMappedLength),
              ERR_MEMORY);
    EXPECT_EQ(add_calls, 0);
    EXPECT_EQ(NvlinkTransportTestPeer::registrationCount(transport), 0);
    EXPECT_EQ(driver.address_range_calls, 1);
    EXPECT_EQ(driver.release_calls, 1);
    driver.expectNoResources();
}
#endif

TEST(NvlinkTransportTest, WriteAndRead) {
    bool strict_fabric = false;
    std::string strict_error;
    ASSERT_TRUE(ParseStrictFabricRequirement(strict_fabric, strict_error))
        << strict_error;

    const size_t kDataLength = 4096000;
    int gpu_id = FLAGS_gpu_id;
#ifdef USE_CUDA
    if (std::getenv("MC_USE_NVLINK_IPC") == nullptr) {
        Status capability = NvlinkVmmAllocation::CheckStrictFabricCapability();
        if (!capability.ok()) {
            if (strict_fabric) {
                FAIL() << "Fabric DEVICE VMM is unavailable in strict mode: "
                       << capability.ToString();
            } else {
                GTEST_SKIP() << "Fabric DEVICE VMM is unavailable: "
                             << capability.ToString();
            }
        }
    }
#endif

    // Server (target) setup
    auto server_engine = std::make_unique<TransferEngine>(false);
    ASSERT_EQ(server_engine->init(P2PHANDSHAKE, "127.0.0.1:0", "127.0.0.1", 0),
              0);

    // Install MNNVL transport (nvlink or hip) on server
    Transport* server_transport =
        server_engine->installTransport(MNNVL_PROTOCOL, nullptr);
    ASSERT_NE(server_transport, nullptr);

    auto server_metadata = server_engine->getMetadata();
    auto local_desc =
        server_metadata->getSegmentDescByID(LOCAL_SEGMENT_ID, false);
    ASSERT_NE(local_desc, nullptr);
    const size_t initial_buffer_count = local_desc->buffers.size();
    int rc = 0;

#ifdef USE_CUDA
    // A local-only legacy CPU workspace is tracked by NVLink but never
    // published to peer metadata.
    std::vector<char> local_workspace(4096);
    rc = server_engine->registerLocalMemory(
        local_workspace.data(), local_workspace.size(), "cpu:0", false);
    ASSERT_EQ(rc, 0);
    local_desc = server_metadata->getSegmentDescByID(LOCAL_SEGMENT_ID, false);
    ASSERT_EQ(local_desc->buffers.size(), initial_buffer_count);
    ASSERT_EQ(server_engine->unregisterLocalMemory(local_workspace.data()), 0);
#endif

    void* server_buffer = allocatePublishedBuffer(kDataLength * 2, gpu_id);
    ASSERT_NE(server_buffer, nullptr);
    rc = server_engine->registerLocalMemory(server_buffer, kDataLength * 2,
                                            "cuda:0");
    ASSERT_EQ(rc, 0);
    local_desc = server_metadata->getSegmentDescByID(LOCAL_SEGMENT_ID, false);
    ASSERT_EQ(local_desc->buffers.size(), initial_buffer_count + 1);
    const auto published_buffer = local_desc->buffers.back();

    // Client (initiator) setup
    auto client_engine = std::make_unique<TransferEngine>(false);
    ASSERT_EQ(client_engine->init(P2PHANDSHAKE, "127.0.0.1:0", "127.0.0.1", 0),
              0);

    // Install MNNVL transport (nvlink or hip) on client
    Transport* client_transport =
        client_engine->installTransport(MNNVL_PROTOCOL, nullptr);
    ASSERT_NE(client_transport, nullptr);

    void* client_buffer = allocateCudaBuffer(kDataLength * 2, gpu_id);
    rc = client_engine->registerLocalMemory(client_buffer, kDataLength * 2,
                                            "cuda:" + std::to_string(gpu_id),
#ifdef USE_CUDA
                                            false);
#else
                                            true);
#endif
    ASSERT_EQ(rc, 0);

    const std::string server_endpoint = server_engine->getLocalIpAndPort();
    auto segment_id = client_engine->openSegment(server_endpoint);
    ASSERT_NE(segment_id, static_cast<SegmentID>(-1));

#ifdef USE_CUDA
    // A relocation failure is terminal at task level even though no Slice was
    // allocated, so status polling and freeBatchID cannot hang or report a
    // false zero-slice success.
    {
        auto batch_id = client_engine->allocateBatchID(1);
        TransferRequest invalid;
        invalid.opcode = TransferRequest::WRITE;
        invalid.length = 64;
        invalid.source = client_buffer;
        invalid.target_id = segment_id;
        invalid.target_offset = published_buffer.addr + published_buffer.length;
        Status submit_status =
            client_engine->submitTransfer(batch_id, {invalid});
        ASSERT_FALSE(submit_status.ok());
        TransferStatus failed_status;
        ASSERT_TRUE(
            client_engine->getTransferStatus(batch_id, 0, failed_status).ok());
        EXPECT_EQ(failed_status.s, TransferStatusEnum::FAILED);
        EXPECT_TRUE(client_engine->freeBatchID(batch_id).ok());
    }

    {
        auto batch_id = client_transport->allocateBatchID(1);
        TransferRequest invalid;
        invalid.opcode = TransferRequest::WRITE;
        invalid.length = 64;
        invalid.source = client_buffer;
        invalid.target_id = segment_id;
        invalid.target_offset = published_buffer.addr + published_buffer.length;
        Status submit_status =
            client_transport->submitTransfer(batch_id, {invalid});
        ASSERT_FALSE(submit_status.ok());
        TransferStatus failed_status;
        ASSERT_TRUE(
            client_transport->getTransferStatus(batch_id, 0, failed_status)
                .ok());
        EXPECT_EQ(failed_status.s, TransferStatusEnum::FAILED);
        EXPECT_TRUE(client_transport->freeBatchID(batch_id).ok());
    }
#endif

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
    local_desc = server_metadata->getSegmentDescByID(LOCAL_SEGMENT_ID, false);
    EXPECT_EQ(local_desc->buffers.size(), initial_buffer_count);
    freePublishedBuffer(server_buffer);
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
