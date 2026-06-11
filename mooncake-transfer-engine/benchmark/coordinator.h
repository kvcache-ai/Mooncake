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

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

namespace mooncake {
namespace bench {

//==============================================================================
// RPC Data Types
//==============================================================================

enum class ErrorCode : int32_t {
    OK = 0,
    INVALID_PARAMS = 1,
    TIMEOUT = 2,
    ALREADY_REGISTERED = 3,
    NOT_REGISTERED = 4,
    INTERNAL_ERROR = 5,
};

struct NodeInfo {
    int32_t node_rank;
    std::string rpc_address;
    std::string hostname;
};

struct RegisterRequest {
    std::string rpc_address;
    std::string hostname;
};

struct RegisterResponse {
    int32_t assigned_rank;
    int32_t total_nodes;
};

struct WaitRequest {
    int32_t my_rank;
};

struct WaitResponse {
    bool ready;
    std::string message;
    std::vector<NodeInfo> all_nodes;
};

struct ResultReport {
    int32_t node_rank;
    int32_t num_threads;
    int64_t block_size;
    int64_t batch_size;
    int64_t total_samples;
    double total_duration_avg;
    double transfer_duration_avg;
    double transfer_duration_min;
    double transfer_duration_max;
    double transfer_duration_p99;
    double transfer_duration_p999;
};

struct ResultAck {
    bool success;
};

struct PingRequest {
    int32_t node_rank;
};

struct PingResponse {
    bool alive;
};

//==============================================================================
// Client Interface (for tebench)
//==============================================================================

class CoordinatorClient {
   public:
    CoordinatorClient(const std::string& server_addr);
    ~CoordinatorClient();

    bool Connect();
    void Disconnect();
    tl::expected<RegisterResponse, ErrorCode> RegisterNode(
        const RegisterRequest& request);
    tl::expected<std::vector<NodeInfo>, ErrorCode> WaitForAll(
        int32_t node_rank, int timeout_sec = 120);
    tl::expected<ResultAck, ErrorCode> ReportResult(
        const ResultReport& request);
    tl::expected<PingResponse, ErrorCode> Ping(const PingRequest& request);

   private:
    class Impl;
    Impl* impl_;
};

}  // namespace bench
}  // namespace mooncake
