// Copyright 2024 KVCache.AI
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

// How to run:
// etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls
// http://10.0.0.1:2379
// ./rdma_transport_test --mode=target --metadata_server=127.0.0.1:2379
//   --local_server_name=127.0.0.2:12345 --device_name=erdma_0
//   --mem_backend=mlu --device_id=0
// ./rdma_transport_test --metadata_server=127.0.0.1:2379
//   --segment_id=127.0.0.2:12345 --local_server_name=127.0.0.3:12346
//   --device_name=erdma_1 --mem_backend=mlu --device_id=0
//   --expect_remote_location=mlu:0

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cstdlib>
#include <string>

#include "rdma_transport_test_common.h"

using mooncake::rdma_test::TestCtx;
using mooncake::rdma_test::TestOpts;

DEFINE_string(local_server_name, mooncake::getHostname(),
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "127.0.0.1:2379", "etcd server host address");
DEFINE_string(mode, "initiator", "Running mode: initiator or target");
DEFINE_string(device_name, "mlx5_0", "RDMA device name list");
DEFINE_string(nic_priority_matrix, "", "Path to RDMA NIC priority matrix");
DEFINE_string(segment_id, "127.0.0.1", "Segment ID to access data");
DEFINE_string(mem_backend, "auto", "Memory backend: auto|cpu|gpu|mlu");
DEFINE_int32(device_id, -1, "Backend device ID");
DEFINE_bool(use_wildcard_location, false,
            "Register memory with wildcard location");
DEFINE_string(expect_remote_location, "",
              "Expected remote buffer location");
DEFINE_uint64(buffer_size, 64ull << 20, "Registered buffer size");
DEFINE_uint64(data_length, 4ull << 20, "Transfer size");

// Legacy flags kept for backwards compatibility with the old GPU-only test.
DEFINE_bool(use_vram, true, "Legacy alias for mem_backend=gpu");
DEFINE_int32(gpu_id, 0, "Legacy alias for device_id");
DEFINE_string(protocol, "rdma", "Legacy transport selector");
DEFINE_string(operation, "read", "Legacy operation selector");

namespace {

std::string pickBackend() {
    if (FLAGS_mem_backend != "auto") {
        return FLAGS_mem_backend;
    }
#if defined(USE_MLU)
    return "mlu";
#elif defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
    return FLAGS_use_vram ? "gpu" : "cpu";
#else
    return "cpu";
#endif
}

int pickDevId(const std::string &backend) {
    if (FLAGS_device_id >= 0) {
        return FLAGS_device_id;
    }
    if (backend == "gpu") {
        return FLAGS_gpu_id;
    }
    return 0;
}

TestCtx mkCtx() {
    if (FLAGS_protocol != "rdma") {
        LOG(ERROR) << "Unsupported protocol: only rdma is supported";
        std::exit(EXIT_FAILURE);
    }
    (void)FLAGS_operation;

    TestCtx ctx;
    ctx.opts.local_server_name = FLAGS_local_server_name;
    ctx.opts.metadata_server = FLAGS_metadata_server;
    ctx.opts.mode = FLAGS_mode;
    ctx.opts.device_name = FLAGS_device_name;
    ctx.opts.nic_priority_matrix = FLAGS_nic_priority_matrix;
    ctx.opts.segment_id = FLAGS_segment_id;
    ctx.opts.expect_remote_location = FLAGS_expect_remote_location;
    ctx.opts.device_id = pickDevId(pickBackend());
    ctx.opts.use_wildcard_location = FLAGS_use_wildcard_location;
    ctx.opts.buffer_size = FLAGS_buffer_size;
    ctx.opts.data_length = FLAGS_data_length;

    auto backend = pickBackend();
    ctx.mem = mooncake::rdma_test::mkMem(backend);
    if (!ctx.mem) {
        LOG(ERROR) << "Unsupported mem_backend=" << backend;
        std::exit(EXIT_FAILURE);
    }
    return ctx;
}

}  // namespace

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    auto ctx = mkCtx();
    if (ctx.opts.mode == "initiator") {
        return mooncake::rdma_test::runInit(ctx);
    }
    if (ctx.opts.mode == "target") {
        return mooncake::rdma_test::runTgt(ctx);
    }

    LOG(ERROR) << "Unsupported mode: must be 'initiator' or 'target'";
    return EXIT_FAILURE;
}
