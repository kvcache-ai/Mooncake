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

#ifndef UB_CONTEXT_H
#define UB_CONTEXT_H

#include <atomic>
#include <fcntl.h>
#include <fstream>
#include <list>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <sys/epoll.h>

#include "config.h"
#include "ub_transport.h"

namespace mooncake {
class UbContext;

// define worker pool class
class UbWorkerPool {
   public:
    UbWorkerPool(UbContext& context, int numa_socket_id = 0);

    ~UbWorkerPool();

    // Add slices to queue, called by Transport
    int submitPostSend(const std::vector<Transport::Slice*>& slice_list);

   private:
    void performPostSend(int thread_id);

    void performPoll(int thread_id);

    void redispatch(std::vector<Transport::Slice*>& slice_list, int thread_id);

    void transferWorker(int thread_id);

    void monitorWorker();

    int doProcessContextEvents();

   private:
    UbContext& context_;
    const int numa_socket_id_;
    std::vector<std::thread> worker_thread_;
    std::atomic<bool> workers_running_;
    std::atomic<int> suspended_flag_;
    std::atomic<int> redispatch_counter_;
    std::mutex cond_mutex_;
    std::condition_variable cond_var_;
    using SliceList = std::vector<Transport::Slice*>;
    const static int kShardCount = 8;
    std::unordered_map<std::string, SliceList> slice_queue_[kShardCount];
    std::atomic<uint64_t> slice_queue_count_[kShardCount];
    TicketLock slice_queue_lock_[kShardCount];
    std::vector<std::unordered_map<std::string, SliceList>>
        collective_slice_queue_;
    std::atomic<uint64_t> submitted_slice_count_;
    std::atomic<uint64_t> processed_slice_count_;
    uint64_t success_nr_polls = 0, failed_nr_polls = 0;
};

class UbEndpointStore {
   public:
    virtual ~UbEndpointStore() = default;
    virtual std::shared_ptr<UbEndPoint> getEndpoint(
        const std::string& peer_nic_path) = 0;
    virtual std::shared_ptr<UbEndPoint> insertEndpoint(
        const std::string& peer_nic_path, UbContext* context) = 0;
    virtual int deleteEndpoint(const std::string& peer_nic_path) = 0;
    virtual void evictEndpoint() = 0;
    virtual void reclaimEndpoint() = 0;
    virtual size_t getSize() = 0;

    virtual int destroy() = 0;
    virtual int disconnect() = 0;
};

// NSDI 24, similar to clock with quick demotion
class UbSIEVEEndpointStore : public UbEndpointStore {
   public:
    UbSIEVEEndpointStore(size_t max_size)
        : waiting_list_len_(0), max_size_(max_size) {}

    std::shared_ptr<UbEndPoint> getEndpoint(
        const std::string& peer_nic_path) override;
    std::shared_ptr<UbEndPoint> insertEndpoint(const std::string& peer_nic_path,
                                               UbContext* context) override;
    int deleteEndpoint(const std::string& peer_nic_path) override;
    void evictEndpoint() override;
    void reclaimEndpoint() override;
    size_t getSize() override;

    int destroy() override;
    int disconnect() override;

   private:
    RWSpinlock endpoint_map_lock_;
    // The bool represents visited
    std::unordered_map<std::string,
                       std::pair<std::shared_ptr<UbEndPoint>, std::atomic_bool>>
        endpoint_map_;
    std::unordered_map<std::string, std::list<std::string>::iterator> fifo_map_;
    std::list<std::string> fifo_list_;

    std::optional<std::list<std::string>::iterator> hand_;

    std::unordered_set<std::shared_ptr<UbEndPoint>> waiting_list_;
    std::atomic<int> waiting_list_len_;

    size_t max_size_;
};

// UbContext class
class UbContext {
   public:
    UbContext(UbTransport& engine, std::string device_name, int max_endpoints)
        : device_name_(std::move(device_name)),
          engine_(engine),
          max_endpoints_(max_endpoints),
          worker_pool_(nullptr),
          active_(true),
          show_work_request_flushed_error_(false) {}

    virtual ~UbContext() = default;

    int doConstruct(GlobalConfig& config) {
        show_work_request_flushed_error_ = globalConfig().trace;
        if (construct(config)) {
            LOG(INFO) << "failed construct context " << toString();
            return 1;
        }
        LOG(INFO) << "finish construct context " << toString();
        endpoint_store_ =
            std::make_shared<UbSIEVEEndpointStore>(max_endpoints_);
        if (endpoint_store_ == nullptr) {
            LOG(INFO) << "failed create endpoint store.";
            return 1;
        }
        LOG(INFO) << "finish create endpoint store.";
        return 0;
    }

    virtual int buildLocalBufferDesc(uint64_t addr,
                                     UbTransport::BufferDesc& buffer_desc) = 0;

    virtual void* localSegWithIndex(unsigned value) = 0;

    virtual std::shared_ptr<UbEndPoint> makeEndpoint() = 0;

   private:
    virtual int construct(GlobalConfig& config) = 0;

    virtual int deconstruct() = 0;

   public:
    virtual int registerMemoryRegion(uint64_t va, size_t length) = 0;

    virtual int unregisterMemoryRegion(uint64_t va) = 0;

    virtual int doProcessContextEvents() = 0;

    virtual void* retrieveRemoteSeg(const std::string& value) = 0;

    virtual int openDevice(const std::string& device_name, uint8_t port,
                           int& eid_index) = 0;

    virtual int poll(int num_entries, Transport::Slice** cr,
                     int jfc_index = 0) = 0;

    virtual volatile int* outstandingCount(int jfc_index) = 0;

    virtual int jfcCount() = 0;

    virtual int submitPostSend(
        const std::vector<Transport::Slice*>& slice_list) = 0;

    virtual int getAsyncFd() = 0;

    virtual std::string getEid() = 0;

    virtual std::string toString() = 0;

    bool active() const { return active_; }

    void set_active(bool flag) { active_ = flag; }

    // EndPoint Management
    std::shared_ptr<UbEndPoint> endpoint() {
        return endpoint("LOCAL_SEGMENT_ID");
    }

    std::shared_ptr<UbEndPoint> endpoint(const std::string& peer_nic_path) {
        if (!active_) {
            LOG(ERROR) << "Context is not active: " << deviceName();
            return nullptr;
        }

        if (peer_nic_path.empty()) {
            LOG(ERROR) << "Invalid peer NIC path: " << deviceName();
            return nullptr;
        }
        auto endpoint = endpoint_store_->getEndpoint(peer_nic_path);
        if (endpoint) {
            return endpoint;
        }

        endpoint = endpoint_store_->insertEndpoint(peer_nic_path, this);
        endpoint_store_->reclaimEndpoint();
        return endpoint;
    }

    int deleteEndpoint(const std::string& peer_nic_path) {
        return endpoint_store_->deleteEndpoint(peer_nic_path);
    }

    int disconnectAllEndpoints() { return endpoint_store_->disconnect(); }

    // Device name, such as `mlx5_3`
    std::string deviceName() const { return device_name_; }

    // NIC Path, such as `192.168.3.76@mlx5_3`
    std::string nicPath() const {
        return MakeNicPath(engine_.local_server_name_, device_name_);
    }

    UbTransport& engine() const { return engine_; }

    uint8_t portNum() const { return port_; }

    int activeSpeed() const { return active_speed_; }

    int eventFd() const { return event_fd_; }

    int socketId() {
        std::string path =
            "/sys/class/infiniband/" + device_name_ + "/device/numa_node";
        std::ifstream file(path);
        if (file.is_open()) {
            int socket_id;
            file >> socket_id;
            file.close();
            return socket_id;
        } else {
            return 0;
        }
    }

    static int hexCharToValue(char c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'A' && c <= 'F') return 10 + c - 'A';
        if (c >= 'a' && c <= 'f') return 10 + c - 'a';
        throw std::invalid_argument("Invalid hexadecimal character");
    }

    static std::string serializeBinaryData(const void* data, size_t length) {
        if (!data) {
            throw std::invalid_argument("Data pointer cannot be null");
        }

        std::string hexString;
        hexString.reserve(length * 2);

        const unsigned char* byteData = static_cast<const unsigned char*>(data);
        for (size_t i = 0; i < length; ++i) {
            hexString.push_back("0123456789ABCDEF"[(byteData[i] >> 4) & 0x0F]);
            hexString.push_back("0123456789ABCDEF"[byteData[i] & 0x0F]);
        }

        return hexString;
    }

    static void deserializeBinaryData(const std::string& hexString,
                                      std::vector<unsigned char>& buffer) {
        if (hexString.length() % 2 != 0) {
            throw std::invalid_argument("Input string length must be even");
        }

        buffer.clear();
        buffer.reserve(hexString.length() / 2);

        for (size_t i = 0; i < hexString.length(); i += 2) {
            int high = hexCharToValue(hexString[i]);
            int low = hexCharToValue(hexString[i + 1]);
            buffer.push_back(static_cast<unsigned char>((high << 4) | low));
        }
    }

   protected:
    static int joinNonblockingPollList(int& event_fd, int data_fd) {
        event_fd = epoll_create1(0);
        if (event_fd < 0) {
            PLOG(ERROR) << "Failed to create epoll";
            return ERR_CONTEXT;
        }
        epoll_event event{};
        memset(&event, 0, sizeof(epoll_event));

        int flags = fcntl(data_fd, F_GETFL, 0);
        if (flags == -1) {
            PLOG(ERROR) << "Failed to get file descriptor flags";
            return ERR_CONTEXT;
        }
        if (fcntl(data_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
            PLOG(ERROR) << "Failed to set file descriptor nonblocking";
            return ERR_CONTEXT;
        }

        event.events = EPOLLIN | EPOLLET;
        event.data.fd = data_fd;
        if (epoll_ctl(event_fd, EPOLL_CTL_ADD, event.data.fd, &event)) {
            PLOG(ERROR) << "Failed to register file descriptor to epoll";
            return ERR_CONTEXT;
        }
        return 0;
    }

   protected:
    const std::string device_name_;
    UbTransport& engine_;
    int max_endpoints_;

    uint8_t port_ = 0;

    std::shared_ptr<UbEndpointStore> endpoint_store_;

    int active_speed_ = -1;

    int event_fd_ = -1;

    std::vector<std::thread> background_thread_;
    std::atomic<bool> threads_running_;

    std::shared_ptr<UbWorkerPool> worker_pool_;

    volatile bool active_;

    bool show_work_request_flushed_error_;
};
}  // namespace mooncake

#endif  // UB_CONTEXT_H
