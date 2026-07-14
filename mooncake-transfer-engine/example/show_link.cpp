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

#include <iostream>
#include <memory>

#include "show_links.h"
#include "transfer_engine.h"

DEFINE_string(metadata_server, "etcd://127.0.0.1:2379",
              "Metadata server connection string");
DEFINE_string(local_server_name, "", "Local server name (ip:port)");
DEFINE_string(ip_or_host_name, "", "IP or hostname for RPC");
DEFINE_int32(rpc_port, 12345, "RPC port");
DEFINE_bool(json, false, "Output in JSON format");
DEFINE_bool(discover_only, false,
            "Only discover local topology without connecting");

using namespace mooncake;

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    if (FLAGS_discover_only) {
        auto engine = std::make_unique<TransferEngine>(/*auto_discover=*/false);
        auto topology = engine->getLocalTopology();
        if (topology) {
            topology->discover({});
        }
        std::cout << engine->showLinks(FLAGS_json) << std::endl;
        return 0;
    }

    if (FLAGS_local_server_name.empty()) {
        LOG(ERROR) << "Must specify --local_server_name or --discover_only";
        return 1;
    }

    auto engine = std::make_unique<TransferEngine>(/*auto_discover=*/true);
    int ret = engine->init(FLAGS_metadata_server, FLAGS_local_server_name,
                           FLAGS_ip_or_host_name, FLAGS_rpc_port);
    if (ret) {
        LOG(ERROR) << "Failed to initialize TransferEngine";
        return 1;
    }

    engine->installTransport("rdma", nullptr);

    std::cout << engine->showLinks(FLAGS_json) << std::endl;

    engine->freeEngine();
    return 0;
}
