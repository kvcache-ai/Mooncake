// Copyright 2026 Huawei Technologies Co., Ltd
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

#include "transport/ascend_transport/ascend_direct_transport/utils.h"

#include <random>
#include <glog/logging.h>

#include "common.h"
#include "config.h"

namespace mooncake {
namespace {
constexpr int32_t kPortRange = 100;
constexpr int32_t kMaxGenPortAttempts = 500;
}  // namespace

// AscendThreadPool implementation
AscendThreadPool::AscendThreadPool(size_t num_threads) : running_(true) {
    for (size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back([this] {
            while (true) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock(queue_mutex_);
                    condition_.wait(
                        lock, [this] { return !running_ || !tasks_.empty(); });

                    if (!running_ && tasks_.empty()) return;

                    task = std::move(tasks_.front());
                    tasks_.pop();
                }
                task();
            }
        });
    }
}

AscendThreadPool::~AscendThreadPool() { stop(); }

void AscendThreadPool::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) {
        return;  // Already stopped
    }

    condition_.notify_all();
    for (std::thread& worker : workers_) {
        if (worker.joinable()) worker.join();
    }
}

bool AscendThreadPool::isRunning() const { return running_; }

std::string GenAdxlEngineName(const std::string& ip, const uint64_t port) {
    if (globalConfig().use_ipv6) {
        return ("[" + ip + "]") + ":" + std::to_string(port);
    }
    return ip + ":" + std::to_string(port);
}

uint16_t FindAdxlListenPort(int32_t base_port, int32_t device_id) {
    int32_t physical_dev_id;
    CHECK_ACL(aclrtGetPhyDevIdByLogicDevId(device_id, &physical_dev_id));
    static std::random_device rand_gen;
    std::uniform_int_distribution rand_dist;
    const int min_port = base_port + physical_dev_id * kPortRange;
    const int max_port = base_port + (physical_dev_id + 1) * kPortRange;
    LOG(INFO) << "Find available between " << min_port << " and " << max_port;
    bool use_ipv6 = globalConfig().use_ipv6;
    int sockfd;
    for (int attempt = 0; attempt < kMaxGenPortAttempts; ++attempt) {
        int port = min_port + rand_dist(rand_gen) % (max_port - min_port + 1);
        sockfd = socket(use_ipv6 ? AF_INET6 : AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1) {
            continue;
        }
        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;
        if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout))) {
            close(sockfd);
            sockfd = -1;
            continue;
        }
        sockaddr_storage bind_address_storage;
        memset(&bind_address_storage, 0, sizeof(bind_address_storage));
        auto* bind_addr = reinterpret_cast<sockaddr*>(&bind_address_storage);
        socklen_t addr_len;
        if (use_ipv6) {
            auto* addr6 = reinterpret_cast<sockaddr_in6*>(bind_addr);
            addr6->sin6_family = AF_INET6;
            addr6->sin6_port = htons(port);
            addr6->sin6_addr = IN6ADDR_ANY_INIT;
            addr_len = sizeof(*addr6);
        } else {
            auto* addr4 = reinterpret_cast<sockaddr_in*>(bind_addr);
            addr4->sin_family = AF_INET;
            addr4->sin_port = htons(port);
            addr4->sin_addr.s_addr = INADDR_ANY;
            addr_len = sizeof(*addr4);
        }
        if (bind(sockfd, bind_addr, addr_len) < 0) {
            close(sockfd);
            sockfd = -1;
            continue;
        }
        close(sockfd);
        return port;
    }
    return 0;
}

int SetDeviceAndGetContext(int32_t device_id, aclrtContext* out_context) {
    CHECK_ACL(aclrtSetDevice(device_id));
    CHECK_ACL(aclrtGetCurrentContext(out_context));
    return 0;
}
}  // namespace mooncake