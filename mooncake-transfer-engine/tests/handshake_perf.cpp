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

// Manual latency driver for the P2P handshake plugins. Not registered as a
// unit test. MC_HANDSHAKE_TRANSPORT selects socket (the default) or coro.
//
//   handshake_perf server [--block]
//   handshake_perf client <port> cold|warm|concurrent|mixed

#include "transfer_metadata_plugin.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <json/json.h>

namespace {

using clock_type = std::chrono::steady_clock;

std::atomic<bool> g_release{false};

Json::Value Object() { return Json::Value(Json::objectValue); }

int Ack(const Json::Value &, Json::Value &local) {
    local = Object();
    local["reply_msg"] = "";
    local["notify_msg"] = "success";
    return 0;
}

int BlockConnection(const Json::Value &, Json::Value &local) {
    const auto deadline = clock_type::now() + std::chrono::milliseconds(300);
    while (!g_release.load(std::memory_order_acquire) &&
           clock_type::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    local = Object();
    local["reply_msg"] = "";
    return 0;
}

int ProbeMaybeRelease(const Json::Value &peer, Json::Value &local) {
    if (peer.isMember("release")) {
        g_release.store(true, std::memory_order_release);
    }
    local = Object();
    return 0;
}

uint16_t Listen(int &fd) {
    fd = socket(AF_INET, SOCK_STREAM, 0);
    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) !=
        0) {
        return 0;
    }
    sockaddr_in bound{};
    socklen_t length = sizeof(bound);
    getsockname(fd, reinterpret_cast<sockaddr *>(&bound), &length);
    return ntohs(bound.sin_port);
}

double Percentile(std::vector<double> values, double p) {
    if (values.empty()) return 0;
    std::sort(values.begin(), values.end());
    size_t index = static_cast<size_t>(p * (values.size() - 1));
    return values[index];
}

void Report(const std::string &scenario, const std::string &op, int fails,
            const std::vector<double> &us, double seconds) {
    const double qps = seconds > 0 ? us.size() / seconds : 0;
    std::cout << scenario << "," << op << "," << us.size() << "," << fails
              << "," << Percentile(us, 0.50) << "," << Percentile(us, 0.99)
              << "," << qps << "\n";
}

using Call = int (*)(mooncake::HandShakePlugin &, uint16_t,
                     const Json::Value &);

int CallSend(mooncake::HandShakePlugin &plugin, uint16_t port,
             const Json::Value &request) {
    Json::Value response;
    return plugin.send("127.0.0.1", port, request, response);
}

int CallMeta(mooncake::HandShakePlugin &plugin, uint16_t port,
             const Json::Value &request) {
    Json::Value response;
    return plugin.exchangeMetadata("127.0.0.1", port, request, response);
}

int CallNotify(mooncake::HandShakePlugin &plugin, uint16_t port,
               const Json::Value &request) {
    Json::Value response;
    return plugin.sendNotify("127.0.0.1", port, request, response);
}

int CallProbe(mooncake::HandShakePlugin &plugin, uint16_t port,
              const Json::Value &request) {
    Json::Value response;
    return plugin.sendProbe("127.0.0.1", port, request, response);
}

struct Op {
    const char *name;
    Call call;
};

const Op kOps[] = {
    {"connection", CallSend},
    {"metadata", CallMeta},
    {"notify", CallNotify},
    {"probe", CallProbe},
};

void TimeLoop(const std::string &scenario, const char *op, Call call,
              mooncake::HandShakePlugin &plugin, uint16_t port, int n,
              bool fresh_client) {
    Json::Value request = Object();
    std::vector<double> us;
    us.reserve(n);
    int fails = 0;
    const auto started = clock_type::now();
    for (int i = 0; i < n; ++i) {
        std::shared_ptr<mooncake::HandShakePlugin> owned;
        mooncake::HandShakePlugin *used = &plugin;
        if (fresh_client) {
            owned = mooncake::HandShakePlugin::Create("P2PHANDSHAKE");
            used = owned.get();
        }
        const auto t0 = clock_type::now();
        if (call(*used, port, request) != 0) ++fails;
        const auto t1 = clock_type::now();
        us.push_back(
            std::chrono::duration<double, std::micro>(t1 - t0).count());
    }
    const double seconds =
        std::chrono::duration<double>(clock_type::now() - started).count();
    Report(scenario, op, fails, us, seconds);
}

void ConcurrentProbes(uint16_t port, int threads, int each) {
    auto plugin = mooncake::HandShakePlugin::Create("P2PHANDSHAKE");
    std::vector<double> us(static_cast<size_t>(threads * each));
    std::atomic<int> fails{0};
    const auto started = clock_type::now();
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            Json::Value request = Object();
            for (int i = 0; i < each; ++i) {
                const auto t0 = clock_type::now();
                Json::Value response;
                if (plugin->sendProbe("127.0.0.1", port, request, response) !=
                    0) {
                    fails.fetch_add(1);
                }
                const auto t1 = clock_type::now();
                us[static_cast<size_t>(t * each + i)] =
                    std::chrono::duration<double, std::micro>(t1 - t0).count();
            }
        });
    }
    for (auto &worker : workers) worker.join();
    const double seconds =
        std::chrono::duration<double>(clock_type::now() - started).count();
    Report("concurrent-" + std::to_string(threads), "probe", fails.load(), us,
           seconds);
}

void Mixed(uint16_t port) {
    auto plugin = mooncake::HandShakePlugin::Create("P2PHANDSHAKE");
    std::thread blocker([&] {
        Json::Value request = Object();
        Json::Value response;
        plugin->send("127.0.0.1", port, request, response);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    TimeLoop("blocked-connection", "probe", CallProbe, *plugin, port, 200,
             false);
    Json::Value release = Object();
    release["release"] = true;
    Json::Value response;
    plugin->sendProbe("127.0.0.1", port, release, response);
    blocker.join();
}

void Server(bool block) {
    int fd = -1;
    const uint16_t port = Listen(fd);
    auto plugin = mooncake::HandShakePlugin::Create("P2PHANDSHAKE");
    plugin->registerOnConnectionCallBack(block ? BlockConnection : Ack);
    plugin->registerOnMetadataCallBack(Ack);
    plugin->registerOnNotifyCallBack(Ack);
    plugin->registerOnProbeCallBack(ProbeMaybeRelease);
    if (plugin->startDaemon(port, fd) != 0) {
        std::cerr << "start failed\n";
        return;
    }
    std::cout << port << std::endl;
    for (;;) pause();
}

}  // namespace

int main(int argc, char **argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    if (argc < 2) return 2;
    const std::string mode = argv[1];
    if (mode == "server") {
        Server(argc > 2 && std::string(argv[2]) == "--block");
        return 0;
    }
    if (argc < 4) return 2;
    const auto port = static_cast<uint16_t>(std::stoi(argv[2]));
    const std::string scenario = argv[3];
    std::cout << "scenario,op,n,fail,p50_us,p99_us,qps\n";
    if (scenario == "cold" || scenario == "warm") {
        auto plugin = mooncake::HandShakePlugin::Create("P2PHANDSHAKE");
        const bool fresh = scenario == "cold";
        const int n = fresh ? 100 : 1000;
        for (const auto &op : kOps) {
            TimeLoop(scenario, op.name, op.call, *plugin, port, n, fresh);
        }
        return 0;
    }
    if (scenario == "concurrent") {
        for (int threads : {1, 8, 32}) ConcurrentProbes(port, threads, 200);
        return 0;
    }
    if (scenario == "mixed") {
        Mixed(port);
        return 0;
    }
    return 2;
}
