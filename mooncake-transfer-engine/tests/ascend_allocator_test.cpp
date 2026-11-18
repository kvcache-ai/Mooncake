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

#include <gtest/gtest.h>

#include <acl/acl.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "ascend_affinity.h"
#include "ascend_allocator.h"
#include "transport/ascend_transport/ascend_direct_transport/context_manager.h"

// The allocator is a standalone shared library: these tests link it directly
// (no transfer engine) and provide strong ACL stand-ins, mirroring the
// TENT-only consumer scenario. A config provider can be registered exactly as
// the transfer engine does; without one the runtime flags are used.
namespace {

int g_malloc_host_calls = 0;
int g_free_host_calls = 0;
int g_set_device_calls = 0;
int g_set_current_context_calls = 0;
uint32_t g_device_count = 1;
int32_t g_current_device = 0;

bool g_provider_agent_mode = false;
bool g_provider_use_fabric_mem = false;

bool AgentModeProvider() { return g_provider_agent_mode; }
bool FabricMemProvider() { return g_provider_use_fabric_mem; }

constexpr uintptr_t kContextBase = 0x10000;

aclrtContext ContextForDevice(int32_t device_id) {
    return reinterpret_cast<aclrtContext>(kContextBase +
                                          static_cast<uintptr_t>(device_id));
}

void ResetMocks() {
    g_malloc_host_calls = 0;
    g_free_host_calls = 0;
    g_set_device_calls = 0;
    g_set_current_context_calls = 0;
    g_device_count = 1;
    g_current_device = 0;
    g_provider_agent_mode = false;
    g_provider_use_fabric_mem = false;
    unsetenv("ASCEND_ENABLE_USE_FABRIC_MEM");
    unsetenv("ASCEND_USE_SHMEM");
}

}  // namespace

extern "C" {

aclError aclrtGetDeviceCount(uint32_t* count) {
    *count = g_device_count;
    return ACL_ERROR_NONE;
}

aclError aclrtGetDevice(int32_t* device_id) {
    *device_id = g_current_device;
    return ACL_ERROR_NONE;
}

aclError aclrtSetDevice(int32_t device_id) {
    if (device_id < 0 || static_cast<uint32_t>(device_id) >= g_device_count) {
        return ACL_ERROR_FAILURE;
    }
    g_current_device = device_id;
    ++g_set_device_calls;
    return ACL_ERROR_NONE;
}

aclError aclrtGetCurrentContext(aclrtContext* context) {
    *context = ContextForDevice(g_current_device);
    return ACL_ERROR_NONE;
}

aclError aclrtSetCurrentContext(aclrtContext context) {
    (void)context;
    ++g_set_current_context_calls;
    return ACL_ERROR_NONE;
}

aclError aclrtGetPhyDevIdByLogicDevId(int32_t logic_dev_id,
                                      int32_t* physical_dev_id) {
    *physical_dev_id = logic_dev_id;
    return ACL_ERROR_NONE;
}

const char* aclrtGetSocName() { return "Ascend910B"; }

const char* aclGetRecentErrMsg() { return "mock acl error message"; }

aclError aclrtMallocHost(void** host_ptr, size_t size) {
    ++g_malloc_host_calls;
    *host_ptr = malloc(size);
    return *host_ptr != nullptr ? ACL_ERROR_NONE : ACL_ERROR_FAILURE;
}

aclError aclrtFreeHost(void* host_ptr) {
    ++g_free_host_calls;
    free(host_ptr);
    return ACL_ERROR_NONE;
}

}  // extern "C"

namespace mooncake {
namespace {

class AscendAllocatorTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ResetMocks();
        ascend_allocator_set_config_provider(nullptr, nullptr);
        ContextManager::getInstance().finalize();
    }

    void TearDown() override {
        ascend_allocator_set_config_provider(nullptr, nullptr);
        ContextManager::getInstance().finalize();
        unsetenv("ASCEND_USE_SHMEM");
        unsetenv("ASCEND_ENABLE_USE_FABRIC_MEM");
    }
};

TEST_F(AscendAllocatorTest, RuntimeFlagsSelectHostAllocation) {
    ascend_allocator_set_runtime_flags(false, false);
    void* ptr = ascend_allocate_memory(4096, "ascend");
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(g_malloc_host_calls, 1);
    EXPECT_TRUE(ascend_is_store_memory(ptr, 4096));
    EXPECT_FALSE(ascend_is_direct_vmm_memory(ptr, 4096));
    std::string handle;
    EXPECT_FALSE(ascend_get_shared_host_handle(ptr, &handle));

    ascend_free_memory("ascend", ptr);
    EXPECT_EQ(g_free_host_calls, 1);
}

TEST_F(AscendAllocatorTest, ConfigProviderOverridesRuntimeFlags) {
    // Runtime flags ask for fabric memory, but the provider (as registered by a
    // process that links the transfer engine) says no: the provider wins.
    ascend_allocator_set_runtime_flags(true, true);
    ascend_allocator_set_config_provider(&AgentModeProvider,
                                         &FabricMemProvider);
    void* ptr = ascend_allocate_memory(4096, "ascend");
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(g_malloc_host_calls, 1);
    ascend_free_memory("ascend", ptr);
}

TEST_F(AscendAllocatorTest, ProviderAgentModeRotatesDeviceContexts) {
    g_device_count = 2;
    g_provider_agent_mode = true;
    g_provider_use_fabric_mem = false;
    ascend_allocator_set_runtime_flags(false, false);
    ascend_allocator_set_config_provider(&AgentModeProvider,
                                         &FabricMemProvider);
    ASSERT_TRUE(ContextManager::getInstance().initialize());

    void* first = ascend_allocate_memory(4096, "ascend");
    ASSERT_NE(first, nullptr);
    EXPECT_GE(g_set_device_calls, 2)
        << "ContextManager enumerates both devices";
    // One context switch selects the next agent device, and RunBoundAlloc
    // restores the caller's context around the allocation.
    EXPECT_EQ(g_set_current_context_calls, 2);

    void* second = ascend_allocate_memory(4096, "ascend");
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(g_set_current_context_calls, 4)
        << "agent allocations rotate across the device contexts";

    ascend_free_memory("ascend", first);
    ascend_free_memory("ascend", second);
}

TEST_F(AscendAllocatorTest, FabricAllocationRunsOnAffinityPool) {
    g_provider_agent_mode = false;
    g_provider_use_fabric_mem = true;
    ascend_allocator_set_config_provider(&AgentModeProvider,
                                         &FabricMemProvider);

    bool ran_on_pool = false;
    AllocationAffinityPools pools;
    pools.shared = [&ran_on_pool](std::function<void()> task) {
        ran_on_pool = true;
        task();
    };
    PublishAllocationAffinityPools(this, std::move(pools));

    // Whether fabric allocation is available in this build or not, the
    // request must be dispatched through the published affinity runner.
    (void)ascend_allocate_memory(4096, "ascend");
    EXPECT_TRUE(ran_on_pool);

    ClearAllocationAffinityPools(this);
}

TEST_F(AscendAllocatorTest, SharedHostMemoryPublishesImportableHandle) {
    ascend_allocator_set_runtime_flags(false, false);
    setenv("ASCEND_USE_SHMEM", "1", 1);
    void* ptr = ascend_allocate_memory(4096, "ascend");
    ASSERT_NE(ptr, nullptr);
    std::string handle;
    ASSERT_TRUE(ascend_get_shared_host_handle(ptr, &handle));
    EXPECT_NE(handle.rfind("/proc/", 0), std::string::npos);
    ascend_free_memory("ascend", ptr);
    unsetenv("ASCEND_USE_SHMEM");
}

}  // namespace
}  // namespace mooncake
