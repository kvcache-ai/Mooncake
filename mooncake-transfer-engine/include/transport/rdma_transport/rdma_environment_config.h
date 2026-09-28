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

#ifndef RDMA_ENVIRONMENT_CONFIG_H
#define RDMA_ENVIRONMENT_CONFIG_H

namespace mooncake {

class Environ;

// RDMA transport settings read from the environment.
struct RdmaEnvironmentConfig {
    // WITH_NVIDIA_PEERMEM: register GPU memory with ibv_reg_mr() through the
    // nvidia-peermem kernel module instead of exporting a dma_buf.
    bool with_nvidia_peermem{true};
    // MC_RDMA_DATA_DIRECT: register dma_buf MRs with mlx5 Data Direct.
    bool data_direct{false};

    static RdmaEnvironmentConfig FromEnvironment(const Environ& env);
    // Resolved from the process environment once, on first use.
    static const RdmaEnvironmentConfig& Process();
};

}  // namespace mooncake

#endif  // RDMA_ENVIRONMENT_CONFIG_H
