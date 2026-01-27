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
#include <pybind11/pybind11.h>
#include <sys/time.h>

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <stack>
#include <vector>

#include "common/base/status.h"
#include "transfer_engine.h"
#include "transfer_engine_c.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/transport.h"

using namespace mooncake;

const static size_t kDefaultBufferCapacity = 2ull * 1024 * 1024 * 1024;
const static size_t kSlabSizeKBTabLen = 16;
const static size_t kMaxClassId = kSlabSizeKBTabLen - 1;
const static size_t kSlabSizeKB[] = {
    8,         16,        32,         64,        128,      256,
    512,       1024,      2 * 1024,   4 * 1024,  8 * 1024, 16 * 1024,
    32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024};

class TransferEnginePy {
   public:
    enum class TransferOpcode { READ = 0, WRITE = 1 };
    struct TransferNotify {
        std::string name;
        std::string msg;
    };

   public:
    using BatchDesc = Transport::BatchDesc;

   public:
    TransferEnginePy();

    ~TransferEnginePy();

    int initialize(const char *local_hostname, const char *metadata_server,
                   const char *protocol, const char *device_name);

    int initializeExt(const char *local_hostname, const char *metadata_server,
                      const char *protocol, const char *device_name,
                      const char *metadata_type);

    int getRpcPort();

    uintptr_t allocateManagedBuffer(size_t length);

    int freeManagedBuffer(uintptr_t user_tensor, size_t length);

    int transferSyncWrite(const char *target_hostname, uintptr_t buffer,
                          uintptr_t peer_buffer_address, size_t length);

    batch_id_t transferSubmitWrite(const char *target_hostname,
                                   uintptr_t buffer,
                                   uintptr_t peer_buffer_address,
                                   size_t length);

    int transferCheckStatus(batch_id_t batch_id);

    int transferSyncRead(const char *target_hostname, uintptr_t buffer,
                         uintptr_t peer_buffer_address, size_t length);

    int batchTransferSyncWrite(const char *target_hostname,
                               std::vector<uintptr_t> buffers,
                               std::vector<uintptr_t> peer_buffer_addresses,
                               std::vector<size_t> lengths);

    int batchTransferSyncRead(const char *target_hostname,
                              std::vector<uintptr_t> buffers,
                              std::vector<uintptr_t> peer_buffer_addresses,
                              std::vector<size_t> lengths);

    batch_id_t batchTransferAsyncWrite(
        const char *target_hostname, const std::vector<uintptr_t> &buffers,
        const std::vector<uintptr_t> &peer_buffer_addresses,
        const std::vector<size_t> &lengths);

    batch_id_t batchTransferAsyncRead(
        const char *target_hostname, const std::vector<uintptr_t> &buffers,
        const std::vector<uintptr_t> &peer_buffer_addresses,
        const std::vector<size_t> &lengths);

    int transferSync(const char *target_hostname, uintptr_t buffer,
                     uintptr_t peer_buffer_address, size_t length,
                     TransferOpcode opcode, TransferNotify *notify = nullptr);

    // Known issue: in a few inference engines and benchmarks, accuracy
    // may be affected when using the batchTransferSync API. We currently
    // found this issue only in multi-node NVLink transfers.
    int batchTransferSync(const char *target_hostname,
                          std::vector<uintptr_t> buffers,
                          std::vector<uintptr_t> peer_buffer_addresses,
                          std::vector<size_t> lengths, TransferOpcode opcode,
                          TransferNotify *notify = nullptr);

    batch_id_t batchTransferAsync(
        const char *target_hostname, const std::vector<uintptr_t> &buffers,
        const std::vector<uintptr_t> &peer_buffer_addresses,
        const std::vector<size_t> &lengths, TransferOpcode opcode);

    int getBatchTransferStatus(const std::vector<batch_id_t> &batch_ids);

#ifdef USE_CUDA
    void batchTransferOnCuda(
        const char *target_hostname, const std::vector<uintptr_t> &buffers,
        const std::vector<uintptr_t> &peer_buffer_addresses,
        const std::vector<size_t> &lengths, TransferOpcode opcode,
        uintptr_t stream_ptr = 0);

    void transferOnCudaWrite(const char *target_hostname, uintptr_t buffer,
                             uintptr_t peer_buffer_address, size_t length,
                             uintptr_t stream_ptr = 0);

    void transferOnCudaRead(const char *target_hostname, uintptr_t buffer,
                            uintptr_t peer_buffer_address, size_t length,
                            uintptr_t stream_ptr = 0);

    void batchTransferOnCudaWrite(
        const char *target_hostname, const std::vector<uintptr_t> &buffers,
        const std::vector<uintptr_t> &peer_buffer_addresses,
        const std::vector<size_t> &lengths, uintptr_t stream_ptr = 0);

    void batchTransferOnCudaRead(
        const char *target_hostname, const std::vector<uintptr_t> &buffers,
        const std::vector<uintptr_t> &peer_buffer_addresses,
        const std::vector<size_t> &lengths, uintptr_t stream_ptr = 0);
#endif

    uintptr_t getFirstBufferAddress(const std::string &segment_name);

    int writeBytesToBuffer(uintptr_t dest_address, char *src_ptr,
                           size_t length) {
        memcpy((void *)dest_address, (void *)src_ptr, length);
        return 0;
    }

    pybind11::bytes readBytesFromBuffer(uintptr_t source_address,
                                        size_t length) {
        return pybind11::bytes(
            static_cast<const char *>(reinterpret_cast<void *>(source_address)),
            length);
    }

    // FOR EXPERIMENT ONLY
    int registerMemory(uintptr_t buffer_addr, size_t capacity);

    // must be called before TransferEnginePy::~TransferEnginePy()
    int unregisterMemory(uintptr_t buffer_addr);

    int batchRegisterMemory(std::vector<uintptr_t> buffer_addresses,
                            std::vector<size_t> capacities);

    int batchUnregisterMemory(std::vector<uintptr_t> buffer_addresses);

    std::string getLocalTopology(const char *device_name);

    std::vector<TransferNotify> getNotifies();

    std::shared_ptr<TransferEngine> getEngine() const { return engine_; }

    uintptr_t getEnginePtr() const { return (uintptr_t)engine_.get(); }

   private:
    char *allocateRawBuffer(size_t capacity);

    int findClassId(size_t size);

    int doBuddyAllocate(int class_id);

   private:
    std::shared_ptr<TransferEngine> engine_;
    Transport *xport_;

    std::mutex mutex_;
    std::vector<std::stack<char *>> free_list_;
    std::vector<char *> buffer_list_;
    std::unordered_set<char *> large_buffer_list_;
    std::unordered_map<std::string, Transport::SegmentHandle> handle_map_;
    bool auto_discovery_;

    uint64_t transfer_timeout_nsec_;
};
