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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/time.h>

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>

#include "transfer_engine.h"
#include "transport/transport.h"
#include "common.h"

#define NR_SOCKETS (1)
#define BASE_ADDRESS_HINT (0x40000000000)

DEFINE_string(local_server_name, mooncake::getHostname(),
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "192.168.3.77:2379", "etcd server host address");
DEFINE_string(device_name, "mlx5_2", "Device name to use");
DEFINE_string(nic_priority_matrix, "",
              "Path to NIC priority matrix file (Advanced)");

using namespace mooncake;

static void *allocateMemoryPool(size_t size, int socket_id) {
    void *start_addr;
    start_addr = mmap((void *)BASE_ADDRESS_HINT, size, PROT_READ | PROT_WRITE,
                      MAP_ANON | MAP_PRIVATE, -1, 0);
    if (start_addr != (void *)BASE_ADDRESS_HINT) {
        PLOG(ERROR) << "Failed to allocate memory on specified address";
        exit(1);
    }
    return start_addr;
}

static void freeMemoryPool(void *addr, size_t size) { munmap(addr, size); }

std::string loadNicPriorityMatrix() {
    if (!FLAGS_nic_priority_matrix.empty()) {
        std::ifstream file(FLAGS_nic_priority_matrix);
        if (file.is_open()) {
            std::string content((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
            file.close();
            return content;
        }
    }
    return "{\"cpu:0\": [[\"" + FLAGS_device_name + "\"], []], " +
           "\"cpu:1\": [[\"" + FLAGS_device_name + "\"], []]}";
}

int target() {
    auto nic_priority_matrix = loadNicPriorityMatrix();

    const size_t dram_buffer_size = 1ull << 30;
    // disable topology auto discovery for testing.
    auto engine = std::make_unique<TransferEngine>(false);

    void **args = (void **)malloc(2 * sizeof(void *));
    args[0] = (void *)nic_priority_matrix.c_str();
    args[1] = nullptr;

    const std::string &connectable_name = FLAGS_local_server_name;
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                 connectable_name.c_str(), 12345);
    engine->installTransport("rdma", args);

    LOG_ASSERT(engine);

    void *addr[2] = {nullptr};
    for (int i = 0; i < NR_SOCKETS; ++i) {
        addr[i] = allocateMemoryPool(dram_buffer_size, i);
        int rc = engine->registerLocalMemory(addr[i], dram_buffer_size,
                                             "cpu:" + std::to_string(i));
        LOG_ASSERT(!rc);
    }

    while (true) sleep(1);

    for (int i = 0; i < NR_SOCKETS; ++i) {
        engine->unregisterLocalMemory(addr[i]);
        freeMemoryPool(addr[i], dram_buffer_size);
    }

    return 0;
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return target();
}
