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

#include "loopback_transport.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <unordered_map>

#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"

namespace {
std::atomic<uint64_t> g_bytes_copied{0};
}  // namespace

// Bytes moved by every loopback transport in this process. Lets tests check
// that a transfer really went through this plugin.
extern "C" __attribute__((visibility("default"))) uint64_t
tent_xport_loopback_bytes_copied(void) {
    return g_bytes_copied.load(std::memory_order_relaxed);
}

namespace mooncake {
namespace tent {

namespace {

struct LoopbackSubBatch : public Transport::SubBatch {
    std::vector<std::shared_ptr<LoopbackTransport::Task>> tasks;
    size_t max_size = 0;
    size_t size() const override { return tasks.size(); }
};

// Engines of this process with the loopback transport installed, by segment
// name. Deliveries run under the lock, so an engine cannot go away while a
// notification is being handed to it.
class Bus {
   public:
    static Bus& get() {
        static auto* bus = new Bus();
        return *bus;
    }

    void add(const std::string& name, LoopbackTransport* transport) {
        std::lock_guard<std::mutex> lock(mutex_);
        members_[name] = transport;
    }

    void remove(const std::string& name, LoopbackTransport* transport) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = members_.find(name);
        if (it != members_.end() && it->second == transport) members_.erase(it);
    }

    bool contains(const std::string& name) {
        std::lock_guard<std::mutex> lock(mutex_);
        return members_.count(name) != 0;
    }

    bool deliver(const std::string& name, const Notification& notify) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = members_.find(name);
        if (it == members_.end()) return false;
        it->second->deliver(notify);
        return true;
    }

    bool serve(const std::string& name, const Request& request,
               void* peer_addr) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = members_.find(name);
        return it != members_.end() && it->second->serve(request, peer_addr);
    }

   private:
    std::mutex mutex_;
    std::unordered_map<std::string, LoopbackTransport*> members_;
};

}  // namespace

LoopbackTransport::LoopbackTransport(const tent_transport_plugin_host_t* host)
    : type_(static_cast<TransportType>(host ? host->assigned_type : 0)) {}

LoopbackTransport::~LoopbackTransport() { uninstall(); }

Status LoopbackTransport::install(std::string& local_segment_name,
                                  std::shared_ptr<ControlService> metadata,
                                  std::shared_ptr<Topology> local_topology,
                                  std::shared_ptr<Config> conf) {
    if (installed_)
        return Status::InvalidArgument(
            "loopback transport already installed" LOC_MARK);
    if (type_ == UNSPEC || !metadata)
        return Status::InvalidArgument(
            "loopback transport needs a slot and a control service" LOC_MARK);
    metadata_ = metadata;
    segment_name_ = local_segment_name;
    if (conf) {
        async_ = conf->get("transports/loopback/async", false);
        delay_us_ = conf->get("transports/loopback/delay_us", delay_us_);
    }
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        stopping_ = false;
    }
    if (async_) worker_ = std::thread([this] { workerLoop(); });
    caps.dram_to_dram = true;
    Bus::get().add(segment_name_, this);
    installed_ = true;
    LOG(INFO) << "Loopback transport installed: slot " << type_
              << (async_ ? ", async" : ", sync");
    return Status::OK();
}

Status LoopbackTransport::uninstall() {
    if (!installed_) return Status::OK();
    Bus::get().remove(segment_name_, this);
    quiesce();
    metadata_.reset();
    installed_ = false;
    return Status::OK();
}

Status LoopbackTransport::quiesce() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        stopping_ = true;
    }
    queue_cv_.notify_all();
    // The worker drains the queue before it exits.
    if (worker_.joinable()) worker_.join();
    return Status::OK();
}

Status LoopbackTransport::allocateSubBatch(SubBatchRef& batch,
                                           size_t max_size) {
    auto* sub_batch = new (std::nothrow) LoopbackSubBatch();
    if (!sub_batch)
        return Status::InternalError("cannot allocate sub-batch" LOC_MARK);
    sub_batch->max_size = max_size;
    sub_batch->tasks.reserve(max_size);
    batch = sub_batch;
    return Status::OK();
}

Status LoopbackTransport::freeSubBatch(SubBatchRef& batch) {
    auto* sub_batch = dynamic_cast<LoopbackSubBatch*>(batch);
    if (!sub_batch)
        return Status::InvalidArgument("not a loopback sub-batch" LOC_MARK);
    delete sub_batch;
    batch = nullptr;
    return Status::OK();
}

Status LoopbackTransport::resolve(const Request& request, std::string& target,
                                  void*& peer_addr) {
    return metadata_->segmentManager().withCachedSegment(
        request.target_id, [&](SegmentDesc* segment) -> Status {
            if (segment->type != SegmentType::Memory)
                return Status::InvalidArgument(
                    "loopback only serves memory segments" LOC_MARK);
            if (!Bus::get().contains(segment->name))
                return Status::InvalidArgument(
                    "segment " + segment->name +
                    " has no loopback transport in this process" LOC_MARK);
            auto* buffer =
                segment->findBuffer(request.target_offset, request.length);
            if (!buffer)
                return Status::InvalidArgument(
                    "target range is not registered" LOC_MARK);
            if (std::find(buffer->transports.begin(), buffer->transports.end(),
                          type_) == buffer->transports.end())
                return Status::InvalidArgument(
                    "target buffer is not registered with loopback" LOC_MARK);
            target = segment->name;
            peer_addr = reinterpret_cast<void*>(request.target_offset);
            return Status::OK();
        });
}

Status LoopbackTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto* sub_batch = dynamic_cast<LoopbackSubBatch*>(batch);
    if (!sub_batch)
        return Status::InvalidArgument("not a loopback sub-batch" LOC_MARK);
    if (!installed_)
        return Status::InternalError(
            "loopback transport not installed" LOC_MARK);
    if (sub_batch->tasks.size() + request_list.size() > sub_batch->max_size)
        return Status::TooManyRequests("exceed batch capacity" LOC_MARK);

    // All-or-nothing: resolve every request before accepting any.
    std::vector<std::string> targets(request_list.size());
    std::vector<void*> peer_addrs(request_list.size(), nullptr);
    for (size_t i = 0; i < request_list.size(); ++i)
        CHECK_STATUS(resolve(request_list[i], targets[i], peer_addrs[i]));

    std::vector<std::shared_ptr<Task>> tasks;
    tasks.reserve(request_list.size());
    for (size_t i = 0; i < request_list.size(); ++i) {
        auto task = std::make_shared<Task>();
        task->request = request_list[i];
        task->target = targets[i];
        task->peer_addr = peer_addrs[i];
        task->progress_batch_id = batch->progress_batch_id;
        task->notify_progress = batch->notify_progress;
        tasks.push_back(std::move(task));
    }

    if (!async_) {
        sub_batch->tasks.insert(sub_batch->tasks.end(), tasks.begin(),
                                tasks.end());
        for (auto& task : tasks) run(task);
        return Status::OK();
    }
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (stopping_)
            return Status::InternalError(
                "loopback transport is shutting down" LOC_MARK);
        sub_batch->tasks.insert(sub_batch->tasks.end(), tasks.begin(),
                                tasks.end());
        queue_.insert(queue_.end(), tasks.begin(), tasks.end());
    }
    queue_cv_.notify_one();
    return Status::OK();
}

void LoopbackTransport::run(const std::shared_ptr<Task>& task) {
    auto notify_progress = std::move(task->notify_progress);
    if (task->cancel_requested.load(std::memory_order_acquire)) {
        task->status.store(CANCELED, std::memory_order_release);
    } else if (Bus::get().serve(task->target, task->request, task->peer_addr)) {
        g_bytes_copied.fetch_add(task->request.length,
                                 std::memory_order_relaxed);
        task->transferred.store(task->request.length,
                                std::memory_order_release);
        task->status.store(COMPLETED, std::memory_order_release);
    } else {
        // The peer went away or unregistered the memory after submit.
        task->status.store(FAILED, std::memory_order_release);
    }
    if (notify_progress) notify_progress(task->progress_batch_id);
}

void LoopbackTransport::workerLoop() {
    for (;;) {
        std::shared_ptr<Task> task;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
            if (queue_.empty()) return;
            task = std::move(queue_.front());
            queue_.pop_front();
        }
        if (delay_us_)
            std::this_thread::sleep_for(std::chrono::microseconds(delay_us_));
        run(task);
    }
}

Status LoopbackTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                            TransferStatus& status) {
    auto* sub_batch = dynamic_cast<LoopbackSubBatch*>(batch);
    if (!sub_batch || task_id < 0 ||
        task_id >= static_cast<int>(sub_batch->tasks.size()))
        return Status::InvalidArgument("invalid task id" LOC_MARK);
    const auto& task = sub_batch->tasks[task_id];
    status.s = task->status.load(std::memory_order_acquire);
    status.transferred_bytes =
        task->transferred.load(std::memory_order_acquire);
    return Status::OK();
}

Status LoopbackTransport::cancelTransferTask(SubBatchRef batch, int task_id) {
    auto* sub_batch = dynamic_cast<LoopbackSubBatch*>(batch);
    if (!sub_batch || task_id < 0 ||
        task_id >= static_cast<int>(sub_batch->tasks.size()))
        return Status::InvalidArgument("invalid task id" LOC_MARK);
    sub_batch->tasks[task_id]->cancel_requested.store(
        true, std::memory_order_release);
    return Status::OK();
}

bool LoopbackTransport::serve(const Request& request, void* peer_addr) {
    std::shared_lock<std::shared_mutex> lock(ranges_mutex_);
    const auto addr = reinterpret_cast<uint64_t>(peer_addr);
    const auto end = ranges_.upper_bound(addr);
    const bool covered =
        std::any_of(ranges_.begin(), end, [&](const auto& range) {
            return addr + request.length <= range.first + range.second;
        });
    if (!covered) return false;
    if (request.opcode == Request::WRITE)
        std::memcpy(peer_addr, request.source, request.length);
    else
        std::memcpy(request.source, peer_addr, request.length);
    return true;
}

Status LoopbackTransport::addMemoryBuffer(BufferDesc& desc,
                                          const MemoryOptions& options) {
    {
        std::unique_lock<std::shared_mutex> lock(ranges_mutex_);
        ranges_.emplace(desc.addr, desc.length);
    }
    desc.transports.push_back(type_);
    return Status::OK();
}

Status LoopbackTransport::removeMemoryBuffer(BufferDesc& desc) {
    std::unique_lock<std::shared_mutex> lock(ranges_mutex_);
    auto [first, last] = ranges_.equal_range(desc.addr);
    for (auto it = first; it != last; ++it) {
        if (it->second == desc.length) {
            ranges_.erase(it);
            break;
        }
    }
    return Status::OK();
}

Status LoopbackTransport::sendNotification(SegmentID target_id,
                                           const Notification& notify) {
    if (!installed_)
        return Status::InternalError(
            "loopback transport not installed" LOC_MARK);
    std::string target;
    CHECK_STATUS(metadata_->segmentManager().withCachedSegment(
        target_id, [&](SegmentDesc* segment) -> Status {
            target = segment->name;
            return Status::OK();
        }));
    // Not a loopback peer: let the engine try its next notification path.
    if (!Bus::get().deliver(target, notify))
        return Status::DeviceNotFound("segment " + target +
                                      " has no loopback transport" LOC_MARK);
    return Status::OK();
}

void LoopbackTransport::deliver(const Notification& notify) {
    std::lock_guard<std::mutex> lock(notify_mutex_);
    notifications_.push_back(notify);
}

Status LoopbackTransport::receiveNotification(
    std::vector<Notification>& notify_list) {
    std::lock_guard<std::mutex> lock(notify_mutex_);
    notify_list.clear();
    notify_list.swap(notifications_);
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
