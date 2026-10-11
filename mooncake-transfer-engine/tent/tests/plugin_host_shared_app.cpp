// Copyright 2025 KVCache.AI
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

// A program that uses TENT only through libtent_shared.so, as an external
// application would. Usage:
//   plugin_host_shared_app <plugin-dir> <probe-dir> [<plugins/dir entry>]
// The optional entry names <plugin-dir> in the configuration (for example
// relative to $ORIGIN); <plugin-dir> itself is the directory it resolves to.
// The engines are created with / as the working directory.
// Exits 0 if the loopback plugin loads once and carries a write and a read,
// and a plugin sees this process's TENT singletons.

#include <dlfcn.h>
#include <unistd.h>

#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/utils/random.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"
#include "tent/transfer_engine.h"
#include "tent/transport/shm/shm_transport.h"

using namespace mooncake::tent;

static int loadedCopies(const std::string& file_name) {
    std::ifstream maps("/proc/self/maps");
    std::string line;
    int copies = 0;
    while (std::getline(maps, line)) {
        if (line.find(file_name) == std::string::npos) continue;
        std::istringstream fields(line);
        std::string range, perms, offset;
        fields >> range >> perms >> offset;
        if (offset == "00000000") ++copies;
    }
    return copies;
}

#define REQUIRE(cond)                                                  \
    do {                                                               \
        if (!(cond)) {                                                 \
            std::cerr << "FAILED: " #cond " (line " << __LINE__ << ")" \
                      << std::endl;                                    \
            return 1;                                                  \
        }                                                              \
    } while (0)

int main(int argc, char** argv) {
    REQUIRE(argc == 3 || argc == 4);
    const std::string dir = argv[1];
    const std::string probe_dir = argv[2];
    const std::string entry = argc == 4 ? argv[3] : dir;
    auto make = [&] {
        auto config = std::make_shared<Config>();
        config->set("metadata_type", "p2p");
        config->set("rpc_server_hostname", "127.0.0.1");
        config->set("transports/rdma/enable", false);
        config->set("transports/tcp/enable", true);
        config->set("plugins/dir", entry + ":" + probe_dir);
        config->set("transports/loopback/enable", true);
        return config;
    };
    REQUIRE(chdir("/") == 0);
    TransferEngine target(make());
    TransferEngine initiator(make());
    REQUIRE(target.available() && initiator.available());
    REQUIRE(loadedCopies("libtent_xport_loopback.so") == 1);
    const int type = tent_transport_type_by_name("loopback");
    REQUIRE(type != TRANSPORT_UNSPEC);

    std::vector<char> src(65536, 's'), dst(65536, 0), back(65536, 0);
    REQUIRE(initiator.registerLocalMemory(src.data(), src.size()).ok());
    REQUIRE(initiator.registerLocalMemory(back.data(), back.size()).ok());
    REQUIRE(target.registerLocalMemory(dst.data(), dst.size()).ok());
    SegmentID handle = 0;
    REQUIRE(initiator.openSegment(handle, target.getSegmentName()).ok());
    // Moves dst <-> local over the loopback plugin.
    auto transfer = [&](Request::OpCode opcode, std::vector<char>& local) {
        auto batch = initiator.allocateBatch(1);
        Request request;
        request.opcode = opcode;
        request.source = local.data();
        request.target_id = handle;
        request.target_offset = reinterpret_cast<uint64_t>(dst.data());
        request.length = local.size();
        request.transport_hint = c_to_transport_hint(type);
        TransferStatus status;
        status.s = FAILED;
        if (initiator.submitTransfer(batch, {request}).ok()) {
            for (int i = 0; i < 10000; ++i) {
                if (!initiator.getTransferStatus(batch, status).ok()) break;
                if (status.s != PENDING && status.s != INITIAL) break;
                usleep(1000);
            }
        }
        initiator.freeBatch(batch);
        return status.s == COMPLETED;
    };
    REQUIRE(transfer(Request::WRITE, src));
    REQUIRE(std::memcmp(src.data(), dst.data(), src.size()) == 0);
    REQUIRE(transfer(Request::READ, back));
    REQUIRE(std::memcmp(src.data(), back.data(), src.size()) == 0);

    void* plugin = dlopen((dir + "/libtent_xport_loopback.so").c_str(),
                          RTLD_NOW | RTLD_NOLOAD);
    REQUIRE(plugin != nullptr);
    auto copied = reinterpret_cast<uint64_t (*)()>(
        dlsym(plugin, "tent_xport_loopback_bytes_copied"));
    REQUIRE(copied && copied() == src.size() + back.size());
    dlclose(plugin);

    void* probe = dlopen((probe_dir + "/libtent_xport_addr_probe.so").c_str(),
                         RTLD_NOW | RTLD_NOLOAD);
    REQUIRE(probe != nullptr);
    auto addresses = reinterpret_cast<void (*)(const void**)>(
        dlsym(probe, "tent_test_addr_probe_addresses"));
    REQUIRE(addresses != nullptr);
    const void* seen[3] = {};
    addresses(seen);
    REQUIRE(seen[0] == &Platform::getLoader());
    REQUIRE(seen[1] == &SimpleRandom::Get());
    REQUIRE(seen[2] == &Slab<ShmSubBatch>::Get());
    dlclose(probe);

    initiator.unregisterLocalMemory(src.data(), src.size());
    initiator.unregisterLocalMemory(back.data(), back.size());
    target.unregisterLocalMemory(dst.data(), dst.size());
    std::cout << "plugin_host_shared_app: OK" << std::endl;
    return 0;
}
