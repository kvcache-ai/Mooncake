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
//
// Integration test for #1845. Verifies the end-to-end wiring of the fix:
// WorkerPool::monitorWorker actually calls RdmaContext::reclaimEndpoints at
// ~1 Hz, causing quiescent entries in the endpoint store's waiting_list_ to
// drain without any further insertion traffic. The unit tests in
// endpoint_store_test.cpp verify the reclaim method itself; this file
// verifies that the scheduler invokes it.
//
// Requires an RDMA device. Passes on soft-RoCE (`rdma_rxe`) as well as real
// NICs. Self-skips (GTEST_SKIP) when no device is present, so it is safe to
// register with ctest on CI runners without RDMA.
//
// Environment override: set MC_TEST_DEVICE_NAME to force a specific device;
// otherwise the first device returned by ibv_get_device_list is used.

#include <gtest/gtest.h>
#include <infiniband/verbs.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>

#include "config.h"
#include "transport/rdma_transport/endpoint_store.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"
#include "transport/rdma_transport/rdma_transport.h"

#if defined(__has_feature)
#define MC_HAS_FEATURE(x) __has_feature(x)
#else
#define MC_HAS_FEATURE(x) 0
#endif
#if defined(__SANITIZE_ADDRESS__) || MC_HAS_FEATURE(address_sanitizer)
#include <sanitizer/lsan_interface.h>
#define MC_LSAN_IGNORE_OBJECT(p) __lsan_ignore_object(p)
#else
#define MC_LSAN_IGNORE_OBJECT(p) ((void)(p))
#endif

using namespace mooncake;

namespace {

std::string pickRdmaDevice() {
    const char *override_name = std::getenv("MC_TEST_DEVICE_NAME");
    if (override_name && *override_name) return override_name;
    int num_devices = 0;
    ibv_device **list = ibv_get_device_list(&num_devices);
    if (!list || num_devices == 0) return "";
    std::string name = ibv_get_device_name(list[0]);
    ibv_free_device_list(list);
    return name;
}

// Build an RdmaEndPoint with no QPs and active_=false. The store's reclaim
// path only inspects hasOutstandingSlice(), which for an endpoint with empty
// qp_list_ reduces to !active_. Safe to destruct because qp_list_ is empty.
std::shared_ptr<RdmaEndPoint> makeQuiescentEndpoint(RdmaContext &ctx) {
    auto ep = std::make_shared<RdmaEndPoint>(ctx);
    ep->set_active(false);
    return ep;
}

// Verifies the full fix wiring: after construct() spawns monitorWorker, a
// quiescent entry injected into the store's waiting_list_ is drained by the
// scheduler within ~1.5 s with no further insertion traffic.
TEST(EndpointStoreIntegration, MonitorWorkerTickDrainsWaitingList) {
    const std::string device = pickRdmaDevice();
    if (device.empty()) {
        GTEST_SKIP() << "no RDMA device available — integration test requires "
                        "rxe0, mlx5, or similar. Set MC_TEST_DEVICE_NAME to "
                        "override.";
    }

    // RdmaTransport's destructor dereferences metadata_ which is null until
    // init(); leak the engine to avoid touching that path. Marked ignored so
    // LSAN under ASAN builds doesn't flag this intentional leak.
    auto *transport = new RdmaTransport();
    MC_LSAN_IGNORE_OBJECT(transport);
    auto context = std::make_shared<RdmaContext>(*transport, device);
    auto &config = globalConfig();
    int rc = context->construct(config.num_cq_per_ctx,
                                config.num_comp_channels_per_ctx, config.port,
                                config.gid_index, config.max_cqe,
                                /*max_endpoints=*/4);
    if (rc != 0) {
        GTEST_SKIP() << "RdmaContext::construct failed on device " << device
                     << " (rc=" << rc << "); no usable RDMA device on this "
                     << "host (e.g., CI runners may enumerate a phantom "
                     << "mlx5_0 without a working port).";
    }

    context->testOnlyInsertWaiting(makeQuiescentEndpoint(*context));
    context->testOnlyInsertWaiting(makeQuiescentEndpoint(*context));
    context->testOnlyInsertWaiting(makeQuiescentEndpoint(*context));
    ASSERT_EQ(context->waitingListSize(), 3u);

    // monitorWorker's reclaim tick fires every ~1 s. Give it enough margin
    // for scheduling jitter but keep the test fast.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    EXPECT_EQ(context->waitingListSize(), 0u)
        << "monitorWorker must call reclaimEndpoints within ~1 s. If this "
           "fails, either the periodic tick in worker_pool.cpp was removed or "
           "reclaim is failing on quiescent entries.";
}

}  // namespace
