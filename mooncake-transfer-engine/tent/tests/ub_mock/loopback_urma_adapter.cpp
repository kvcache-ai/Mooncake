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

#include "loopback_urma_adapter.h"

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace mooncake {
namespace tent {
namespace ub {
namespace mock {
namespace {

constexpr uint32_t kApiVersion = 1;
constexpr uint32_t kDescriptorSize = 16;

std::atomic<uint64_t> g_read_requests{0};
std::atomic<uint64_t> g_write_requests{0};
std::atomic<uint64_t> g_bytes{0};

DeviceInfo loopbackDevice() {
    DeviceInfo info;
    info.topology_name = "ub:mock0:eid0";
    info.native_device_name = "mock0";
    // Left empty so topology discovery does not look for a sysfs node.
    info.native_device_path = "";
    info.eid_index = 0;
    info.eid = "00ff:0000:0000:0000:0000:0000:0000:0001";
    info.active = true;
    // Zero capabilities mean "no limit reported".
    return info;
}

// The descriptor carries "address:length" so any engine in the process can
// import a segment registered by another engine.
SegmentDescriptor makeDescriptor(uint64_t address, uint64_t length) {
    return SegmentDescriptor{
        SegmentDescriptor::kSchemaVersion, kApiVersion, kDescriptorSize,
        std::to_string(address) + ":" + std::to_string(length)};
}

bool parseDescriptor(const SegmentDescriptor& descriptor, uint64_t& address,
                     uint64_t& length) {
    const auto colon = descriptor.hex.find(':');
    if (descriptor.schema_version != SegmentDescriptor::kSchemaVersion ||
        descriptor.urma_api_version != kApiVersion ||
        descriptor.urma_abi_size != kDescriptorSize ||
        colon == std::string::npos) {
        return false;
    }
    try {
        address = std::stoull(descriptor.hex.substr(0, colon));
        length = std::stoull(descriptor.hex.substr(colon + 1));
        return length != 0;
    } catch (...) {
        return false;
    }
}

bool contains(uint64_t base, uint64_t length, uint64_t address, size_t size) {
    return address >= base && size <= length && address - base <= length - size;
}

class LoopbackContext final : public Context {
   public:
    explicit LoopbackContext(DeviceInfo info) : info_(std::move(info)) {}
    bool valid() const noexcept override {
        return valid_.load(std::memory_order_acquire);
    }
    const DeviceInfo& deviceInfo() const noexcept override { return info_; }
    int asyncFd() const noexcept override { return -1; }
    void close() { valid_.store(false, std::memory_order_release); }

   private:
    DeviceInfo info_;
    std::atomic<bool> valid_{true};
};

class LoopbackJfc final : public Jfc {
   public:
    bool valid() const noexcept override {
        return valid_.load(std::memory_order_acquire);
    }
    int eventFd() const noexcept override { return -1; }
    void close() { valid_.store(false, std::memory_order_release); }

    void push(const Completion& completion) {
        std::lock_guard<std::mutex> lock(mutex_);
        completions_.push_back(completion);
    }
    void poll(size_t maximum, std::vector<Completion>& output) {
        std::lock_guard<std::mutex> lock(mutex_);
        while (!completions_.empty() && output.size() < maximum) {
            output.push_back(completions_.front());
            completions_.pop_front();
        }
    }

   private:
    std::mutex mutex_;
    std::deque<Completion> completions_;
    std::atomic<bool> valid_{true};
};

template <typename Base>
class LoopbackSegment final : public Base {
   public:
    LoopbackSegment(uint64_t address, uint64_t length,
                    SegmentDescriptor descriptor)
        : address_(address),
          length_(length),
          descriptor_(std::move(descriptor)) {}
    bool valid() const noexcept override {
        return valid_.load(std::memory_order_acquire);
    }
    uint64_t address() const noexcept override { return address_; }
    uint64_t length() const noexcept override { return length_; }
    const SegmentDescriptor& descriptor() const noexcept override {
        return descriptor_;
    }
    void close() { valid_.store(false, std::memory_order_release); }

   private:
    uint64_t address_;
    uint64_t length_;
    SegmentDescriptor descriptor_;
    std::atomic<bool> valid_{true};
};

using LoopbackLocalSegment = LoopbackSegment<LocalSegment>;
using LoopbackRemoteSegment = LoopbackSegment<RemoteSegment>;

class LoopbackJetty final : public Jetty {
   public:
    LoopbackJetty(uint32_t id, std::shared_ptr<LoopbackJfc> jfc)
        : id_(id), jfc_(std::move(jfc)) {}
    bool valid() const noexcept override {
        return valid_.load(std::memory_order_acquire);
    }
    uint32_t id() const noexcept override { return id_; }
    uint32_t uasid() const noexcept override { return 0; }
    const std::shared_ptr<LoopbackJfc>& jfc() const { return jfc_; }
    bool bound() const { return bound_.load(std::memory_order_acquire); }
    void setBound(bool bound) {
        bound_.store(bound, std::memory_order_release);
    }
    void close() {
        valid_.store(false, std::memory_order_release);
        setBound(false);
    }

   private:
    uint32_t id_;
    std::shared_ptr<LoopbackJfc> jfc_;
    std::atomic<bool> valid_{true};
    std::atomic<bool> bound_{false};
};

class LoopbackUrmaAdapter final : public UrmaAdapter {
   public:
    bool available() const noexcept override { return true; }
    uint32_t nativeApiVersion() const noexcept override { return kApiVersion; }
    size_t nativeSegmentDescriptorSize() const noexcept override {
        return kDescriptorSize;
    }

    Status initialize() override {
        initialized_.store(true, std::memory_order_release);
        return Status::OK();
    }
    Status shutdown() override {
        initialized_.store(false, std::memory_order_release);
        return Status::OK();
    }
    Status discoverDevices(std::vector<DeviceInfo>& devices) override {
        if (!initialized_.load(std::memory_order_acquire)) {
            return Status::InvalidArgument("loopback adapter not initialized");
        }
        devices = {device_};
        return Status::OK();
    }

    Status openContext(const DeviceInfo& device, ContextPtr& context) override {
        if (!initialized_.load(std::memory_order_acquire) ||
            device.topology_name != device_.topology_name) {
            return Status::DeviceNotFound("loopback device missing");
        }
        context = std::make_shared<LoopbackContext>(device);
        return Status::OK();
    }
    Status closeContext(ContextPtr& context) override {
        if (auto loopback = std::dynamic_pointer_cast<LoopbackContext>(context))
            loopback->close();
        context.reset();
        return Status::OK();
    }

    Status createJfc(const ContextPtr&, const JfcOptions&,
                     JfcPtr& jfc) override {
        jfc = std::make_shared<LoopbackJfc>();
        return Status::OK();
    }
    Status deleteJfc(JfcPtr& jfc) override {
        if (auto loopback = std::dynamic_pointer_cast<LoopbackJfc>(jfc))
            loopback->close();
        jfc.reset();
        return Status::OK();
    }

    Status registerLocalSegment(const ContextPtr&, uint64_t address,
                                size_t length, const SegmentOptions&,
                                LocalSegmentPtr& segment) override {
        if (address == 0 || length == 0) {
            return Status::InvalidArgument("empty loopback segment");
        }
        segment = std::make_shared<LoopbackLocalSegment>(
            address, length, makeDescriptor(address, length));
        return Status::OK();
    }
    Status unregisterLocalSegment(LocalSegmentPtr& segment) override {
        if (auto loopback =
                std::dynamic_pointer_cast<LoopbackLocalSegment>(segment))
            loopback->close();
        segment.reset();
        return Status::OK();
    }

    Status importRemoteSegment(const ContextPtr&,
                               const SegmentDescriptor& descriptor,
                               const SegmentOptions&,
                               RemoteSegmentPtr& segment) override {
        uint64_t address = 0;
        uint64_t length = 0;
        if (!parseDescriptor(descriptor, address, length)) {
            return Status::InvalidArgument("bad loopback segment descriptor");
        }
        segment = std::make_shared<LoopbackRemoteSegment>(address, length,
                                                          descriptor);
        return Status::OK();
    }
    Status unimportRemoteSegment(RemoteSegmentPtr& segment) override {
        if (auto loopback =
                std::dynamic_pointer_cast<LoopbackRemoteSegment>(segment))
            loopback->close();
        segment.reset();
        return Status::OK();
    }

    Status createJetty(const ContextPtr&, const JfcPtr& jfc,
                       const JettyOptions&, JettyPtr& jetty) override {
        auto loopback_jfc = std::dynamic_pointer_cast<LoopbackJfc>(jfc);
        if (!loopback_jfc) return Status::InvalidArgument("bad loopback JFC");
        jetty = std::make_shared<LoopbackJetty>(
            next_jetty_id_.fetch_add(1, std::memory_order_relaxed),
            std::move(loopback_jfc));
        return Status::OK();
    }
    Status deleteJetty(JettyPtr& jetty) override {
        if (auto loopback = std::dynamic_pointer_cast<LoopbackJetty>(jetty))
            loopback->close();
        jetty.reset();
        return Status::OK();
    }
    Status bindJetty(const JettyPtr& jetty, const RemoteJettyInfo&) override {
        auto loopback = std::dynamic_pointer_cast<LoopbackJetty>(jetty);
        if (!loopback || !loopback->valid())
            return Status::InvalidArgument("bad loopback Jetty");
        loopback->setBound(true);
        return Status::OK();
    }
    Status unbindJetty(const JettyPtr& jetty) override {
        if (auto loopback = std::dynamic_pointer_cast<LoopbackJetty>(jetty))
            loopback->setBound(false);
        return Status::OK();
    }
    Status resetJetty(const JettyPtr&) override { return Status::OK(); }

    // Every posted WR completes inside post(), so nothing is ever in flight.
    Status quiesceJetty(const JettyPtr& jetty, uint32_t timeout_ms,
                        std::vector<Completion>& completions) override {
        completions.clear();
        auto loopback = std::dynamic_pointer_cast<LoopbackJetty>(jetty);
        if (!loopback || !loopback->valid() || timeout_ms == 0)
            return Status::InvalidArgument("bad loopback Jetty quiesce");
        return Status::OK();
    }

    Status post(const JettyPtr& jetty, const std::vector<WorkRequest>& requests,
                size_t& posted_count) override {
        posted_count = 0;
        auto loopback = std::dynamic_pointer_cast<LoopbackJetty>(jetty);
        if (!loopback || !loopback->valid() || !loopback->bound())
            return Status::InvalidArgument("unbound loopback Jetty");
        for (const auto& request : requests) {
            if (!request.local_segment || !request.remote_segment ||
                request.token == 0 || request.length == 0) {
                return Status::InvalidArgument("bad loopback WR");
            }
            const auto& local = *request.local_segment;
            const auto& remote = *request.remote_segment;
            CompletionCategory category = CompletionCategory::SUCCESS;
            if (!local.valid() || !remote.valid() ||
                !contains(local.address(), local.length(),
                          request.local_address, request.length) ||
                !contains(remote.address(), remote.length(),
                          request.remote_address, request.length)) {
                category = CompletionCategory::MEMORY_ERROR;
            } else if (request.operation == Operation::WRITE) {
                std::memmove(
                    reinterpret_cast<void*>(request.remote_address),
                    reinterpret_cast<const void*>(request.local_address),
                    request.length);
                g_write_requests.fetch_add(1, std::memory_order_relaxed);
            } else {
                std::memmove(
                    reinterpret_cast<void*>(request.local_address),
                    reinterpret_cast<const void*>(request.remote_address),
                    request.length);
                g_read_requests.fetch_add(1, std::memory_order_relaxed);
            }
            const bool ok = category == CompletionCategory::SUCCESS;
            if (ok)
                g_bytes.fetch_add(request.length, std::memory_order_relaxed);
            loopback->jfc()->push(
                Completion{category, ok ? 0 : -1, request.token,
                           ok ? static_cast<uint32_t>(request.length) : 0,
                           loopback->id()});
            ++posted_count;
        }
        return Status::OK();
    }

    Status poll(const JfcPtr& jfc, size_t maximum,
                std::vector<Completion>& completions) override {
        completions.clear();
        auto loopback = std::dynamic_pointer_cast<LoopbackJfc>(jfc);
        if (!loopback || !loopback->valid())
            return Status::InvalidArgument("bad loopback JFC");
        loopback->poll(maximum, completions);
        return Status::OK();
    }

   private:
    const DeviceInfo device_ = loopbackDevice();
    std::atomic<bool> initialized_{false};
    std::atomic<uint32_t> next_jetty_id_{1};
};

}  // namespace

std::shared_ptr<UrmaAdapter> createLoopbackUrmaAdapter() {
    return std::make_shared<LoopbackUrmaAdapter>();
}

bool loopbackRequested() {
    const char* value = std::getenv(kLoopbackEnv);
    return value && value[0] != '\0' && std::strcmp(value, "0") != 0;
}

LoopbackStats loopbackStats() {
    return LoopbackStats{g_read_requests.load(std::memory_order_relaxed),
                         g_write_requests.load(std::memory_order_relaxed),
                         g_bytes.load(std::memory_order_relaxed)};
}

void resetLoopbackStats() {
    g_read_requests.store(0, std::memory_order_relaxed);
    g_write_requests.store(0, std::memory_order_relaxed);
    g_bytes.store(0, std::memory_order_relaxed);
}

}  // namespace mock
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
