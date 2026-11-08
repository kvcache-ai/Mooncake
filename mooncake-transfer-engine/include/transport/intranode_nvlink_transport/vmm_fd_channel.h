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

#ifndef INTRANODE_NVLINK_VMM_FD_CHANNEL_H_
#define INTRANODE_NVLINK_VMM_FD_CHANNEL_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>
#include <vector>

// Helpers that let nvlink_intra share CUDA virtual-memory (cuMemCreate)
// buffers between unrelated processes on one host. They do not call CUDA.
//
// An exported POSIX file descriptor is a per-process kernel object, so it
// cannot travel in segment metadata. The exporter publishes a VmmBufferInfo
// (chunk layout plus the address of its fd server) in BufferDesc::shm_name,
// and importers fetch the fds over a Unix domain socket with SCM_RIGHTS.
namespace mooncake {
namespace intranode_vmm {

// One physical allocation (cuMemCreate handle) mapped inside the buffer.
struct VmmChunk {
    uint64_t offset;  // from the start of the buffer
    uint64_t size;
};

struct VmmBufferInfo {
    uint64_t buffer_id = 0;
    std::string endpoint;  // fd server address, see VmmFdServer::endpoint()
    std::vector<VmmChunk> chunks;
};

// The encoding starts with a magic and never has the size of a legacy
// cudaIpcMemHandle_t (64 bytes), so readers that only know cudaIpc handles
// reject it instead of passing it to cudaIpcOpenMemHandle.
std::vector<unsigned char> encodeVmmBufferInfo(const VmmBufferInfo &info);

bool decodeVmmBufferInfo(const std::vector<unsigned char> &data,
                         VmmBufferInfo &info);

// Serves fds for registered buffers to local peers. One per transport.
class VmmFdServer {
   public:
    // Fills `fds` with one newly exported fd per chunk of `buffer_id`, in
    // chunk order, and returns 0. The server closes them after sending.
    using ExportFn =
        std::function<int(uint64_t buffer_id, std::vector<int> &fds)>;

    VmmFdServer() = default;
    ~VmmFdServer();
    VmmFdServer(const VmmFdServer &) = delete;
    VmmFdServer &operator=(const VmmFdServer &) = delete;

    // Listens on an abstract socket, or on a socket file in `dir` when it is
    // not empty (for peers that share a volume but not a network namespace).
    int start(const std::string &dir, ExportFn export_fn);
    void stop();
    bool running() const { return listen_fd_ >= 0; }
    const std::string &endpoint() const { return endpoint_; }

   private:
    void serve();
    void handleClient(int fd);

    int listen_fd_ = -1;
    int wake_fds_[2] = {-1, -1};
    std::thread thread_;
    std::string endpoint_;
    std::string socket_path_;
    ExportFn export_fn_;
};

// Fetches the fds of `buffer_id` from the server at `endpoint`. On success
// `fds` holds `expected` fds owned by the caller.
int fetchVmmFds(const std::string &endpoint, uint64_t buffer_id,
                size_t expected, std::vector<int> &fds, int timeout_ms = 10000);

}  // namespace intranode_vmm
}  // namespace mooncake

#endif  // INTRANODE_NVLINK_VMM_FD_CHANNEL_H_
