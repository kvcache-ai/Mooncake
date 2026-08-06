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

#include "tent/transport/ub/urma_adapter.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstring>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#if defined(TENT_HAS_REAL_URMA) && TENT_HAS_REAL_URMA
#include <urma_api.h>
#endif

namespace mooncake {
namespace tent {
namespace ub {
namespace {

constexpr char kUnavailableMessage[] =
    "native TENT UB is unavailable because this build has no liburma";

class UnavailableUrmaAdapter final : public UrmaAdapter {
   public:
    bool available() const noexcept override { return false; }
    uint32_t nativeApiVersion() const noexcept override { return 0; }
    size_t nativeSegmentDescriptorSize() const noexcept override { return 0; }

    Status initialize() override { return unavailable(); }
    Status shutdown() override { return Status::OK(); }

    Status discoverDevices(std::vector<DeviceInfo>& devices) override {
        devices.clear();
        return unavailable();
    }

    Status openContext(const DeviceInfo&, ContextPtr& context) override {
        context.reset();
        return unavailable();
    }

    Status closeContext(ContextPtr& context) override {
        context.reset();
        return Status::OK();
    }

    Status createJfc(const ContextPtr&, const JfcOptions&,
                     JfcPtr& jfc) override {
        jfc.reset();
        return unavailable();
    }

    Status deleteJfc(JfcPtr& jfc) override {
        jfc.reset();
        return Status::OK();
    }

    Status registerLocalSegment(const ContextPtr&, uint64_t, size_t,
                                const SegmentOptions&,
                                LocalSegmentPtr& segment) override {
        segment.reset();
        return unavailable();
    }

    Status unregisterLocalSegment(LocalSegmentPtr& segment) override {
        segment.reset();
        return Status::OK();
    }

    Status importRemoteSegment(const ContextPtr&, const SegmentDescriptor&,
                               const SegmentOptions&,
                               RemoteSegmentPtr& segment) override {
        segment.reset();
        return unavailable();
    }

    Status unimportRemoteSegment(RemoteSegmentPtr& segment) override {
        segment.reset();
        return Status::OK();
    }

    Status createJetty(const ContextPtr&, const JfcPtr&, const JettyOptions&,
                       JettyPtr& jetty) override {
        jetty.reset();
        return unavailable();
    }

    Status deleteJetty(JettyPtr& jetty) override {
        jetty.reset();
        return Status::OK();
    }

    Status bindJetty(const JettyPtr&, const RemoteJettyInfo&) override {
        return unavailable();
    }

    Status unbindJetty(const JettyPtr& jetty) override {
        return jetty ? unavailable() : Status::OK();
    }

    Status resetJetty(const JettyPtr& jetty) override {
        return jetty ? unavailable() : Status::OK();
    }

    Status quiesceJetty(const JettyPtr& jetty, uint32_t,
                        std::vector<Completion>& completions) override {
        completions.clear();
        return jetty ? unavailable() : Status::OK();
    }

    Status post(const JettyPtr&, const std::vector<WorkRequest>&,
                size_t& posted_count) override {
        posted_count = 0;
        return unavailable();
    }

    Status poll(const JfcPtr&, size_t,
                std::vector<Completion>& completions) override {
        completions.clear();
        return unavailable();
    }

   private:
    static Status unavailable() {
        return Status::NotImplemented(kUnavailableMessage);
    }
};

#if defined(TENT_HAS_REAL_URMA) && TENT_HAS_REAL_URMA

Status nativeError(const char* operation, int native_status) {
    return Status::InternalError(std::string(operation) +
                                 " failed with URMA status " +
                                 std::to_string(native_status));
}

Status nativePointerError(const char* operation) {
    const int native_status = errno == 0 ? URMA_FAIL : errno;
    return nativeError(operation, native_status);
}

Status invalidHandle(const char* handle_name) {
    return Status::InvalidArgument(std::string("invalid ") + handle_name +
                                   " handle");
}

bool checkedRangeContains(uint64_t base, uint64_t range_length,
                          uint64_t address, uint64_t requested_length) {
    if (requested_length == 0 || address < base) return false;
    const uint64_t offset = address - base;
    return offset <= range_length && requested_length <= range_length - offset;
}

bool isAllZero(const urma_eid_t& eid) {
    for (uint8_t byte : eid.raw) {
        if (byte != 0) return false;
    }
    return true;
}

std::string formatEid(const urma_eid_t& eid) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string result;
    result.reserve(39);
    for (size_t i = 0; i < URMA_EID_SIZE; ++i) {
        if (i != 0 && i % 2 == 0) result.push_back(':');
        result.push_back(kHex[(eid.raw[i] >> 4) & 0x0f]);
        result.push_back(kHex[eid.raw[i] & 0x0f]);
    }
    return result;
}

int decodeHexDigit(char value) {
    if (value >= '0' && value <= '9') return value - '0';
    if (value >= 'a' && value <= 'f') return value - 'a' + 10;
    if (value >= 'A' && value <= 'F') return value - 'A' + 10;
    return -1;
}

bool parseEid(std::string_view encoded, urma_eid_t& eid) {
    // Canonical URMA EID form is eight groups of four hex digits.
    if (encoded.size() != 39) return false;

    urma_eid_t parsed{};
    size_t cursor = 0;
    for (size_t byte_index = 0; byte_index < URMA_EID_SIZE; ++byte_index) {
        if (byte_index != 0 && byte_index % 2 == 0) {
            if (cursor >= encoded.size() || encoded[cursor] != ':') {
                return false;
            }
            ++cursor;
        }
        if (cursor + 2 > encoded.size()) return false;
        const int high = decodeHexDigit(encoded[cursor]);
        const int low = decodeHexDigit(encoded[cursor + 1]);
        if (high < 0 || low < 0) return false;
        parsed.raw[byte_index] = static_cast<uint8_t>((high << 4) | low);
        cursor += 2;
    }
    if (cursor != encoded.size()) return false;
    eid = parsed;
    return true;
}

std::string encodeHex(const void* data, size_t length) {
    static constexpr char kHex[] = "0123456789ABCDEF";
    const auto* bytes = static_cast<const uint8_t*>(data);
    std::string result;
    result.resize(length * 2);
    for (size_t i = 0; i < length; ++i) {
        result[i * 2] = kHex[(bytes[i] >> 4) & 0x0f];
        result[i * 2 + 1] = kHex[bytes[i] & 0x0f];
    }
    return result;
}

bool decodeHex(std::string_view encoded, void* output, size_t output_size) {
    if (output == nullptr || encoded.size() != output_size * 2) return false;
    auto* bytes = static_cast<uint8_t*>(output);
    for (size_t i = 0; i < output_size; ++i) {
        const int high = decodeHexDigit(encoded[i * 2]);
        const int low = decodeHexDigit(encoded[i * 2 + 1]);
        if (high < 0 || low < 0) return false;
        bytes[i] = static_cast<uint8_t>((high << 4) | low);
    }
    return true;
}

std::string boundedString(const char* value, size_t capacity) {
    return std::string(value, strnlen(value, capacity));
}

DeviceCapabilities convertCapabilities(const urma_device_attr_t& attr) {
    DeviceCapabilities result;
    result.max_jfc = attr.dev_cap.max_jfc;
    result.max_jfc_depth = attr.dev_cap.max_jfc_depth;
    result.max_jfr_depth = attr.dev_cap.max_jfr_depth;
    result.max_jetty = attr.dev_cap.max_jetty;
    result.max_jetty_depth = attr.dev_cap.max_jfs_depth;
    result.max_send_sge = attr.dev_cap.max_jfs_sge;
    result.max_remote_sge = attr.dev_cap.max_jfs_rsge;
    result.max_message_size = attr.dev_cap.max_msg_size;
    result.max_read_size = attr.dev_cap.max_read_size;
    result.max_write_size = attr.dev_cap.max_write_size;
    result.feature_flags = attr.dev_cap.feature.value;
    result.transport_modes = attr.dev_cap.trans_mode;
    return result;
}

bool deviceIsActive(const urma_device_attr_t& attr) {
    if (attr.port_cnt == 0) return true;
    const size_t port_count = std::min<size_t>(attr.port_cnt, MAX_PORT_CNT);
    for (size_t i = 0; i < port_count; ++i) {
        if (attr.port_attr[i].state == URMA_PORT_ACTIVE ||
            attr.port_attr[i].state == URMA_PORT_ACTIVE_DEFER) {
            return true;
        }
    }
    return false;
}

struct DeviceListDeleter {
    void operator()(urma_device_t** devices) const {
        if (devices != nullptr) urma_free_device_list(devices);
    }
};
using DeviceList = std::unique_ptr<urma_device_t*, DeviceListDeleter>;

struct EidListDeleter {
    void operator()(urma_eid_info_t* eids) const {
        if (eids != nullptr) urma_free_eid_list(eids);
    }
};
using EidList = std::unique_ptr<urma_eid_info_t, EidListDeleter>;

std::mutex g_runtime_mutex;
size_t g_runtime_reference_count = 0;
bool g_runtime_owned = false;

// One lease corresponds to one initialized adapter. Contexts retain their
// adapter's lease, so shutdown cannot call urma_uninit before child handles
// have released all native resources.
class RuntimeLease {
   public:
    static Status Acquire(std::shared_ptr<RuntimeLease>& output) {
        auto lease = std::shared_ptr<RuntimeLease>(new RuntimeLease());
        std::lock_guard<std::mutex> lock(g_runtime_mutex);
        if (g_runtime_reference_count == 0) {
            urma_init_attr_t attributes{};
            const int rc = urma_init(&attributes);
            if (rc != URMA_SUCCESS && rc != URMA_EEXIST) {
                return nativeError("urma_init", rc);
            }
            // EEXIST means another component owns the process-wide runtime.
            // In that case this adapter must not uninitialize it.
            g_runtime_owned = rc == URMA_SUCCESS;
        }
        ++g_runtime_reference_count;
        lease->acquired_ = true;
        output = std::move(lease);
        return Status::OK();
    }

    ~RuntimeLease() { (void)release(); }

    Status release() {
        if (!acquired_) return Status::OK();
        std::lock_guard<std::mutex> lock(g_runtime_mutex);
        if (g_runtime_reference_count == 0) {
            return Status::InternalError(
                "URMA runtime reference count underflow");
        }
        if (g_runtime_reference_count == 1 && g_runtime_owned) {
            const int rc = urma_uninit();
            if (rc != URMA_SUCCESS) return nativeError("urma_uninit", rc);
        }
        --g_runtime_reference_count;
        acquired_ = false;
        if (g_runtime_reference_count == 0) {
            g_runtime_owned = false;
        }
        return Status::OK();
    }

   private:
    RuntimeLease() = default;
    bool acquired_ = false;
};

class RealContext final : public Context {
   public:
    RealContext(std::shared_ptr<RuntimeLease> runtime, DeviceInfo info,
                urma_context_t* native)
        : runtime_(std::move(runtime)),
          info_(std::move(info)),
          native_(native) {}

    ~RealContext() override { (void)close(); }

    bool valid() const noexcept override { return native_ != nullptr; }
    const DeviceInfo& deviceInfo() const noexcept override { return info_; }
    int asyncFd() const noexcept override {
        return native_ == nullptr ? -1 : native_->async_fd;
    }

    urma_context_t* native() const noexcept { return native_; }

    Status close() {
        if (native_ == nullptr) return Status::OK();
        const int rc = urma_delete_context(native_);
        if (rc != URMA_SUCCESS) return nativeError("urma_delete_context", rc);
        native_ = nullptr;
        return Status::OK();
    }

   private:
    std::shared_ptr<RuntimeLease> runtime_;
    DeviceInfo info_;
    urma_context_t* native_ = nullptr;
};

class RealJfc final : public Jfc {
   public:
    explicit RealJfc(std::shared_ptr<RealContext> context)
        : context_(std::move(context)) {}

    ~RealJfc() override { (void)close(); }

    Status initialize(const JfcOptions& options) {
        if (options.enable_completion_events) {
            jfce_ = urma_create_jfce(context_->native());
            if (jfce_ == nullptr) return nativePointerError("urma_create_jfce");
        }

        urma_jfc_cfg_t send_cfg{};
        send_cfg.depth = options.depth;
        send_cfg.jfce = jfce_;
        send_jfc_ = urma_create_jfc(context_->native(), &send_cfg);
        if (send_jfc_ == nullptr)
            return nativePointerError("urma_create_jfc(send)");

        urma_jfc_cfg_t receive_cfg{};
        receive_cfg.depth = options.receiver_depth;
        receive_jfc_ = urma_create_jfc(context_->native(), &receive_cfg);
        if (receive_jfc_ == nullptr) {
            return nativePointerError("urma_create_jfc(receive)");
        }

        urma_jfr_cfg_t receiver_cfg{};
        receiver_cfg.depth = options.receiver_depth;
        receiver_cfg.flag.bs.tag_matching = 0;
        receiver_cfg.trans_mode = URMA_TM_RC;
        receiver_cfg.max_sge = 1;
        receiver_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
        receiver_cfg.jfc = receive_jfc_;
        receiver_cfg.token_value.token = options.token;
        receiver_jfr_ = urma_create_jfr(context_->native(), &receiver_cfg);
        if (receiver_jfr_ == nullptr)
            return nativePointerError("urma_create_jfr");
        return Status::OK();
    }

    bool valid() const noexcept override {
        return send_jfc_ != nullptr && receive_jfc_ != nullptr &&
               receiver_jfr_ != nullptr;
    }

    int eventFd() const noexcept override {
        return jfce_ == nullptr ? -1 : jfce_->fd;
    }

    urma_jfc_t* nativeSendJfc() const noexcept { return send_jfc_; }
    urma_jfc_t* nativeReceiveJfc() const noexcept { return receive_jfc_; }
    urma_jfr_t* nativeReceiver() const noexcept { return receiver_jfr_; }
    const std::shared_ptr<RealContext>& context() const noexcept {
        return context_;
    }
    std::mutex& pollMutex() noexcept { return poll_mutex_; }

    // Callers hold pollMutex(). Markers must survive a failed quiesce call:
    // another normal poller may consume FLUSH_ERR_DONE before shutdown retries
    // the same Jetty.
    void rememberFlushDone(uint32_t jetty_id) {
        flush_done_jetty_ids_.insert(jetty_id);
    }
    bool hasFlushDone(uint32_t jetty_id) const {
        return flush_done_jetty_ids_.count(jetty_id) != 0;
    }
    void clearFlushDone(uint32_t jetty_id) {
        flush_done_jetty_ids_.erase(jetty_id);
    }

    Status retainSegments(uint32_t jetty_id,
                          const std::vector<WorkRequest>& requests) {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        std::unordered_set<uint64_t> new_tokens;
        new_tokens.reserve(requests.size());
        for (const WorkRequest& request : requests) {
            if (!new_tokens.insert(request.token).second ||
                inflight_segments_.find(request.token) !=
                    inflight_segments_.end()) {
                return Status::InvalidArgument(
                    "work request token is already in flight on this JFC");
            }
        }
        for (const WorkRequest& request : requests) {
            inflight_segments_.emplace(
                request.token,
                InflightSegments{request.local_segment, request.remote_segment,
                                 jetty_id});
        }
        return Status::OK();
    }

    void releaseSegment(uint64_t token) {
        if (token == 0) return;
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        inflight_segments_.erase(token);
    }

    // A successful Jetty flush fence proves that no WR posted through that
    // Jetty can touch its segments again. Providers are allowed to omit an
    // individual completion after the fence, so drop any remaining retention
    // entries by Jetty instead of waiting forever for a lost token.
    void releaseSegmentsForJetty(uint32_t jetty_id) {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        for (auto it = inflight_segments_.begin();
             it != inflight_segments_.end();) {
            if (it->second.jetty_id == jetty_id) {
                it = inflight_segments_.erase(it);
            } else {
                ++it;
            }
        }
    }

    Status close() {
        std::lock_guard<std::mutex> lock(poll_mutex_);
        {
            std::lock_guard<std::mutex> inflight_lock(inflight_mutex_);
            if (!inflight_segments_.empty()) {
                return Status::TooManyRequests(
                    "Jfc still retains in-flight segment handles");
            }
        }
        if (receiver_jfr_ != nullptr) {
            const int rc = urma_delete_jfr(receiver_jfr_);
            if (rc != URMA_SUCCESS) return nativeError("urma_delete_jfr", rc);
            receiver_jfr_ = nullptr;
        }
        if (receive_jfc_ != nullptr) {
            const int rc = urma_delete_jfc(receive_jfc_);
            if (rc != URMA_SUCCESS) {
                return nativeError("urma_delete_jfc(receive)", rc);
            }
            receive_jfc_ = nullptr;
        }
        if (send_jfc_ != nullptr) {
            const int rc = urma_delete_jfc(send_jfc_);
            if (rc != URMA_SUCCESS) {
                return nativeError("urma_delete_jfc(send)", rc);
            }
            send_jfc_ = nullptr;
        }
        if (jfce_ != nullptr) {
            const int rc = urma_delete_jfce(jfce_);
            if (rc != URMA_SUCCESS) return nativeError("urma_delete_jfce", rc);
            jfce_ = nullptr;
        }
        return Status::OK();
    }

   private:
    std::shared_ptr<RealContext> context_;
    urma_jfce_t* jfce_ = nullptr;
    urma_jfc_t* send_jfc_ = nullptr;
    urma_jfc_t* receive_jfc_ = nullptr;
    urma_jfr_t* receiver_jfr_ = nullptr;
    std::mutex poll_mutex_;
    std::unordered_set<uint32_t> flush_done_jetty_ids_;

    struct InflightSegments {
        LocalSegmentPtr local;
        RemoteSegmentPtr remote;
        uint32_t jetty_id{0};
    };
    std::mutex inflight_mutex_;
    std::unordered_map<uint64_t, InflightSegments> inflight_segments_;
};

class RealLocalSegment final : public LocalSegment {
   public:
    RealLocalSegment(std::shared_ptr<RealContext> context,
                     urma_target_seg_t* native, uint64_t address,
                     uint64_t length, SegmentDescriptor descriptor)
        : context_(std::move(context)),
          native_(native),
          address_(address),
          length_(length),
          descriptor_(std::move(descriptor)) {}

    ~RealLocalSegment() override { (void)close(); }

    bool valid() const noexcept override { return native_ != nullptr; }
    uint64_t address() const noexcept override { return address_; }
    uint64_t length() const noexcept override { return length_; }
    const SegmentDescriptor& descriptor() const noexcept override {
        return descriptor_;
    }

    urma_target_seg_t* native() const noexcept { return native_; }
    const std::shared_ptr<RealContext>& context() const noexcept {
        return context_;
    }

    Status close() {
        if (native_ == nullptr) return Status::OK();
        const int rc = urma_unregister_seg(native_);
        if (rc != URMA_SUCCESS) return nativeError("urma_unregister_seg", rc);
        native_ = nullptr;
        return Status::OK();
    }

   private:
    std::shared_ptr<RealContext> context_;
    urma_target_seg_t* native_ = nullptr;
    uint64_t address_ = 0;
    uint64_t length_ = 0;
    SegmentDescriptor descriptor_;
};

class RealRemoteSegment final : public RemoteSegment {
   public:
    RealRemoteSegment(std::shared_ptr<RealContext> context,
                      urma_target_seg_t* native, uint64_t address,
                      uint64_t length, SegmentDescriptor descriptor)
        : context_(std::move(context)),
          native_(native),
          address_(address),
          length_(length),
          descriptor_(std::move(descriptor)) {}

    ~RealRemoteSegment() override { (void)close(); }

    bool valid() const noexcept override { return native_ != nullptr; }
    uint64_t address() const noexcept override { return address_; }
    uint64_t length() const noexcept override { return length_; }
    const SegmentDescriptor& descriptor() const noexcept override {
        return descriptor_;
    }

    urma_target_seg_t* native() const noexcept { return native_; }
    const std::shared_ptr<RealContext>& context() const noexcept {
        return context_;
    }

    Status close() {
        if (native_ == nullptr) return Status::OK();
        const int rc = urma_unimport_seg(native_);
        if (rc != URMA_SUCCESS) return nativeError("urma_unimport_seg", rc);
        native_ = nullptr;
        return Status::OK();
    }

   private:
    std::shared_ptr<RealContext> context_;
    urma_target_seg_t* native_ = nullptr;
    uint64_t address_ = 0;
    uint64_t length_ = 0;
    SegmentDescriptor descriptor_;
};

class RealJetty final : public Jetty {
   public:
    RealJetty(std::shared_ptr<RealContext> context,
              std::shared_ptr<RealJfc> jfc)
        : context_(std::move(context)), jfc_(std::move(jfc)) {}

    ~RealJetty() override { cleanup(); }

    Status initialize(const JettyOptions& options) {
        urma_jetty_cfg_t config{};
        config.flag.bs.share_jfr = 1;
        config.jfs_cfg.depth = options.depth;
        config.jfs_cfg.trans_mode = URMA_TM_RC;
        config.jfs_cfg.priority = options.priority;
        config.jfs_cfg.max_sge = options.max_sge;
        config.jfs_cfg.max_rsge = options.max_sge;
        config.jfs_cfg.rnr_retry = options.rnr_retry;
        config.jfs_cfg.err_timeout = options.error_timeout;
        config.jfs_cfg.jfc = jfc_->nativeSendJfc();
        config.shared.jfr = jfc_->nativeReceiver();
        config.shared.jfc = nullptr;

        native_ = urma_create_jetty(context_->native(), &config);
        if (native_ == nullptr) return nativePointerError("urma_create_jetty");
        depth_ = options.depth;
        return Status::OK();
    }

    bool valid() const noexcept override { return native_ != nullptr; }
    uint32_t id() const noexcept override {
        return native_ == nullptr ? 0 : native_->jetty_id.id;
    }
    uint32_t uasid() const noexcept override {
        return native_ == nullptr ? 0 : native_->jetty_id.uasid;
    }

    const std::shared_ptr<RealContext>& context() const noexcept {
        return context_;
    }
    const std::shared_ptr<RealJfc>& jfc() const noexcept { return jfc_; }
    std::mutex& mutex() noexcept { return mutex_; }
    urma_jetty_t* native() const noexcept { return native_; }
    urma_target_jetty_t* remote() const noexcept { return remote_; }
    bool postable() const noexcept { return state_ == State::BOUND; }
    uint32_t depth() const noexcept { return depth_; }

    Status beginError() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (native_ == nullptr) return invalidHandle("Jetty");
        if (state_ == State::RESET) {
            return Status::InvalidArgument("cannot quiesce a reset Jetty");
        }
        if (state_ == State::ERROR) return Status::OK();

        urma_jetty_attr_t attributes{};
        attributes.mask = JETTY_STATE;
        attributes.state = URMA_JETTY_STATE_ERROR;
        const int rc = urma_modify_jetty(native_, &attributes);
        if (rc != URMA_SUCCESS) return nativeError("urma_modify_jetty", rc);
        state_ = State::ERROR;
        flush_fenced_ = false;
        return Status::OK();
    }

    void markFlushFenced() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        flush_fenced_ = true;
    }

    bool flushFenced() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return flush_fenced_;
    }

    bool errorStarted() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return state_ == State::ERROR;
    }

    Status bind(const RemoteJettyInfo& remote_info,
                const urma_eid_t& remote_eid) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (native_ == nullptr) return invalidHandle("Jetty");
        if (state_ == State::RESET) {
            return Status::InvalidArgument("cannot bind a reset Jetty");
        }
        if (remote_ != nullptr) {
            if (!needs_native_unbind_ || state_ != State::BOUND) {
                return Status::TooManyRequests(
                    "Jetty has an imported peer pending cleanup");
            }
            const bool same = remote_info.id == remote_id_ &&
                              remote_info.uasid == remote_uasid_ &&
                              std::memcmp(remote_eid.raw, remote_eid_.raw,
                                          URMA_EID_SIZE) == 0;
            return same ? Status::OK()
                        : Status::InvalidArgument(
                              "Jetty is already bound to another peer");
        }

        urma_rjetty_t descriptor{};
        descriptor.jetty_id.eid = remote_eid;
        descriptor.jetty_id.uasid = remote_info.uasid;
        descriptor.jetty_id.id = remote_info.id;
        descriptor.trans_mode = URMA_TM_RC;
        descriptor.type = URMA_JETTY;
        descriptor.tp_type = URMA_CTP;

        urma_token_t token{.token = remote_info.token};
        urma_target_jetty_t* imported =
            urma_import_jetty(context_->native(), &descriptor, &token);
        if (imported == nullptr) {
            return nativePointerError("urma_import_jetty");
        }

        const int rc = urma_bind_jetty(native_, imported);
        if (rc != URMA_SUCCESS && rc != URMA_EEXIST) {
            const int rollback_rc = urma_unimport_jetty(imported);
            if (rollback_rc != URMA_SUCCESS) {
                // The target Jetty was imported but never bound. Preserve the
                // raw handle as an explicit cleanup-only phase so endpoint
                // failure teardown can retry unimport without issuing an
                // invalid native unbind.
                remote_ = imported;
                remote_eid_ = remote_eid;
                remote_id_ = remote_info.id;
                remote_uasid_ = remote_info.uasid;
                needs_native_unbind_ = false;
                return Status::InternalError(
                    "urma_bind_jetty failed with URMA status " +
                    std::to_string(rc) +
                    "; rollback urma_unimport_jetty failed with URMA status " +
                    std::to_string(rollback_rc) +
                    "; imported target retained for retry");
            }
            return nativeError("urma_bind_jetty", rc);
        }

        remote_ = imported;
        remote_eid_ = remote_eid;
        remote_id_ = remote_info.id;
        remote_uasid_ = remote_info.uasid;
        needs_native_unbind_ = true;
        state_ = State::BOUND;
        return Status::OK();
    }

    Status reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (native_ == nullptr) return invalidHandle("Jetty");
        if (state_ == State::RESET) return Status::OK();
        if (state_ == State::ERROR && !flush_fenced_) {
            return Status::InvalidArgument(
                "cannot reset an ERROR Jetty before its flush fence");
        }

        urma_jetty_attr_t attributes{};
        attributes.mask = JETTY_STATE;
        attributes.state = URMA_JETTY_STATE_RESET;
        const int rc = urma_modify_jetty(native_, &attributes);
        if (rc != URMA_SUCCESS) return nativeError("urma_modify_jetty", rc);
        state_ = State::RESET;
        return Status::OK();
    }

    Status unbind() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (native_ == nullptr) return invalidHandle("Jetty");
        if (remote_ == nullptr) return Status::OK();

        if (needs_native_unbind_) {
            const int unbind_rc = urma_unbind_jetty(native_);
            if (unbind_rc != URMA_SUCCESS) {
                return nativeError("urma_unbind_jetty", unbind_rc);
            }
            // UMDK clears native_->remote_jetty on success. Preserve this
            // phase across an unimport failure so a retry does not issue an
            // invalid second unbind and can proceed directly to unimport.
            needs_native_unbind_ = false;
        }
        const int unimport_rc = urma_unimport_jetty(remote_);
        if (unimport_rc != URMA_SUCCESS) {
            return nativeError("urma_unimport_jetty", unimport_rc);
        }
        remote_ = nullptr;
        needs_native_unbind_ = false;
        if (state_ != State::RESET) state_ = State::CREATED;
        return Status::OK();
    }

    Status close() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (native_ == nullptr) return Status::OK();
        if (remote_ != nullptr || state_ != State::RESET) {
            return Status::InvalidArgument(
                "Jetty must be reset and unbound before deletion");
        }
        const int rc = urma_delete_jetty(native_);
        if (rc != URMA_SUCCESS) return nativeError("urma_delete_jetty", rc);
        native_ = nullptr;
        return Status::OK();
    }

   private:
    enum class State : uint8_t { CREATED, BOUND, ERROR, RESET };

    void cleanup() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (native_ == nullptr) return;

        if (state_ != State::RESET) {
            urma_jetty_attr_t attributes{};
            attributes.mask = JETTY_STATE;
            attributes.state = URMA_JETTY_STATE_RESET;
            (void)urma_modify_jetty(native_, &attributes);
        }

        if (remote_ != nullptr) {
            if (!needs_native_unbind_ ||
                urma_unbind_jetty(native_) == URMA_SUCCESS) {
                needs_native_unbind_ = false;
                (void)urma_unimport_jetty(remote_);
                remote_ = nullptr;
            }
        }
        (void)urma_delete_jetty(native_);
        native_ = nullptr;

        // If unbind failed, deleting the local Jetty has severed the binding;
        // make a final best-effort attempt to release the imported peer.
        if (remote_ != nullptr) {
            (void)urma_unimport_jetty(remote_);
            remote_ = nullptr;
        }
        state_ = State::RESET;
    }

    std::shared_ptr<RealContext> context_;
    std::shared_ptr<RealJfc> jfc_;
    urma_jetty_t* native_ = nullptr;
    urma_target_jetty_t* remote_ = nullptr;
    urma_eid_t remote_eid_{};
    uint32_t remote_id_ = 0;
    uint32_t remote_uasid_ = 0;
    // True only while remote_ is natively bound. A non-null remote_ with this
    // flag clear is an imported-only cleanup phase retained after rollback or
    // after a successful unbind followed by a failed unimport.
    bool needs_native_unbind_{false};
    uint32_t depth_ = 0;
    State state_ = State::CREATED;
    bool flush_fenced_{false};
    mutable std::mutex mutex_;
};

uint32_t nativeAccess(uint32_t access) {
    uint32_t result = 0;
    if ((access & SEGMENT_ACCESS_LOCAL_ONLY) != 0) {
        result |= URMA_ACCESS_LOCAL_ONLY;
    }
    if ((access & SEGMENT_ACCESS_READ) != 0) result |= URMA_ACCESS_READ;
    if ((access & SEGMENT_ACCESS_WRITE) != 0) result |= URMA_ACCESS_WRITE;
    if ((access & SEGMENT_ACCESS_ATOMIC) != 0) result |= URMA_ACCESS_ATOMIC;
    return result;
}

Status validateSegmentOptions(const SegmentOptions& options) {
    constexpr uint32_t kAllAccess = SEGMENT_ACCESS_READ | SEGMENT_ACCESS_WRITE |
                                    SEGMENT_ACCESS_ATOMIC |
                                    SEGMENT_ACCESS_LOCAL_ONLY;
    if (options.access == 0 || (options.access & ~kAllAccess) != 0) {
        return Status::InvalidArgument("invalid segment access mask");
    }
    if ((options.access & SEGMENT_ACCESS_LOCAL_ONLY) != 0 &&
        options.access != SEGMENT_ACCESS_LOCAL_ONLY) {
        return Status::InvalidArgument(
            "local-only segment access cannot include remote permissions");
    }
    return Status::OK();
}

CompletionCategory classifyCompletion(int status) {
    switch (status) {
        case URMA_CR_SUCCESS:
            return CompletionCategory::SUCCESS;
        case URMA_CR_LOC_OPERATION_ERR:
            return CompletionCategory::LOCAL_DEVICE_ERROR;
        case URMA_CR_LOC_LEN_ERR:
        case URMA_CR_LOC_ACCESS_ERR:
        case URMA_CR_LOC_DATA_POISON:
            return CompletionCategory::MEMORY_ERROR;
        case URMA_CR_REM_RESP_LEN_ERR:
        case URMA_CR_REM_UNSUPPORTED_REQ_ERR:
        case URMA_CR_REM_OPERATION_ERR:
        case URMA_CR_REM_ACCESS_ABORT_ERR:
        case URMA_CR_RNR_RETRY_CNT_EXC_ERR:
        case URMA_CR_REM_DATA_POISON:
            return CompletionCategory::REMOTE_PATH_ERROR;
        case URMA_CR_ACK_TIMEOUT_ERR:
            return CompletionCategory::TIMEOUT;
        case URMA_CR_UNSUPPORTED_OPCODE_ERR:
        case URMA_CR_WR_FLUSH_ERR:
        case URMA_CR_WR_SUSPEND_DONE:
        case URMA_CR_WR_FLUSH_ERR_DONE:
        case URMA_CR_WR_UNHANDLED:
            return CompletionCategory::ENDPOINT_ERROR;
        default:
            return CompletionCategory::UNKNOWN_ERROR;
    }
}

bool isEntityMarker(const urma_cr_t& completion) {
    return completion.status == URMA_CR_WR_SUSPEND_DONE ||
           completion.status == URMA_CR_WR_FLUSH_ERR_DONE;
}

Completion convertCompletion(const urma_cr_t& native) {
    Completion completion;
    completion.category = classifyCompletion(native.status);
    completion.native_status = native.status;
    completion.completed_bytes = native.completion_len;
    completion.local_jetty_id = native.local_id;
    // UMDK v25.12 identifies entity markers by status. Their user_ctx is
    // invalid; every other CR (including WR_UNHANDLED returned by flush) is a
    // real WR completion and carries the original token.
    completion.token = isEntityMarker(native) ? 0 : native.user_ctx;
    return completion;
}

template <typename Native, typename Base>
Status releaseTypedHandle(std::shared_ptr<Base>& handle, const char* name) {
    if (!handle) return Status::OK();
    if (handle.use_count() != 1) {
        return Status::TooManyRequests(std::string(name) +
                                       " handle is still retained");
    }
    auto native = std::dynamic_pointer_cast<Native>(handle);
    if (!native) return invalidHandle(name);
    CHECK_STATUS(native->close());
    handle.reset();
    return Status::OK();
}

class RealUrmaAdapter final : public UrmaAdapter {
   public:
    bool available() const noexcept override { return true; }
    uint32_t nativeApiVersion() const noexcept override {
        return URMA_API_VERSION;
    }
    size_t nativeSegmentDescriptorSize() const noexcept override {
        return sizeof(urma_seg_t);
    }

    Status initialize() override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (runtime_) return Status::OK();
        return RuntimeLease::Acquire(runtime_);
    }

    Status shutdown() override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!runtime_) return Status::OK();
        if (runtime_.use_count() != 1) {
            return Status::TooManyRequests(
                "URMA runtime is still retained by native handles");
        }
        CHECK_STATUS(runtime_->release());
        runtime_.reset();
        return Status::OK();
    }

    Status discoverDevices(std::vector<DeviceInfo>& devices) override {
        devices.clear();
        std::shared_ptr<RuntimeLease> runtime;
        CHECK_STATUS(getRuntime(runtime));

        int device_count = 0;
        DeviceList native_devices(urma_get_device_list(&device_count));
        if (!native_devices || device_count < 0) {
            return nativePointerError("urma_get_device_list");
        }

        for (int i = 0; i < device_count; ++i) {
            urma_device_t* native_device = native_devices.get()[i];
            if (native_device == nullptr) continue;

            urma_device_attr_t attributes{};
            const int query_rc = urma_query_device(native_device, &attributes);
            if (query_rc != URMA_SUCCESS) {
                return nativeError("urma_query_device", query_rc);
            }

            uint32_t eid_count = 0;
            EidList eids(urma_get_eid_list(native_device, &eid_count));
            if (!eids && eid_count != 0) {
                return nativePointerError("urma_get_eid_list");
            }

            const std::string native_name =
                boundedString(native_device->name, URMA_MAX_NAME);
            const std::string native_path =
                boundedString(native_device->path, URMA_MAX_PATH);
            for (uint32_t eid_position = 0; eid_position < eid_count;
                 ++eid_position) {
                const urma_eid_info_t& eid = eids.get()[eid_position];
                DeviceInfo info;
                info.native_device_name = native_name;
                info.native_device_path = native_path;
                info.eid_index = eid.eid_index;
                info.eid = formatEid(eid.eid);
                info.topology_name = "ub:" + native_name + ":eid" +
                                     std::to_string(eid.eid_index);
                info.active = deviceIsActive(attributes) && !isAllZero(eid.eid);
                info.capabilities = convertCapabilities(attributes);
                devices.push_back(std::move(info));
            }
        }
        return Status::OK();
    }

    Status openContext(const DeviceInfo& requested,
                       ContextPtr& output) override {
        std::shared_ptr<RuntimeLease> runtime;
        CHECK_STATUS(getRuntime(runtime));
        if (requested.native_device_name.empty()) {
            return Status::InvalidArgument("URMA native device name is empty");
        }
        urma_eid_t requested_eid{};
        if (!parseEid(requested.eid, requested_eid) ||
            isAllZero(requested_eid)) {
            return Status::InvalidArgument("invalid or null URMA EID");
        }

        int device_count = 0;
        DeviceList devices(urma_get_device_list(&device_count));
        if (!devices || device_count < 0) {
            return nativePointerError("urma_get_device_list");
        }

        for (int i = 0; i < device_count; ++i) {
            urma_device_t* native_device = devices.get()[i];
            if (native_device == nullptr ||
                boundedString(native_device->name, URMA_MAX_NAME) !=
                    requested.native_device_name) {
                continue;
            }

            uint32_t eid_count = 0;
            EidList eids(urma_get_eid_list(native_device, &eid_count));
            if (!eids && eid_count != 0) {
                return nativePointerError("urma_get_eid_list");
            }
            bool eid_found = false;
            for (uint32_t j = 0; j < eid_count; ++j) {
                if (eids.get()[j].eid_index == requested.eid_index &&
                    std::memcmp(eids.get()[j].eid.raw, requested_eid.raw,
                                URMA_EID_SIZE) == 0) {
                    eid_found = true;
                    break;
                }
            }
            if (!eid_found) {
                return Status::DeviceNotFound(
                    "requested EID is no longer present on URMA device " +
                    requested.native_device_name);
            }

            urma_device_attr_t attributes{};
            const int query_rc = urma_query_device(native_device, &attributes);
            if (query_rc != URMA_SUCCESS) {
                return nativeError("urma_query_device", query_rc);
            }
            urma_context_t* native_context =
                urma_create_context(native_device, requested.eid_index);
            if (native_context == nullptr) {
                return nativePointerError("urma_create_context");
            }

            DeviceInfo current = requested;
            current.native_device_path =
                boundedString(native_device->path, URMA_MAX_PATH);
            current.active = deviceIsActive(attributes);
            current.capabilities = convertCapabilities(attributes);
            if (current.topology_name.empty()) {
                current.topology_name = "ub:" + current.native_device_name +
                                        ":eid" +
                                        std::to_string(current.eid_index);
            }
            output = std::make_shared<RealContext>(
                std::move(runtime), std::move(current), native_context);
            return Status::OK();
        }
        return Status::DeviceNotFound("URMA device not found: " +
                                      requested.native_device_name);
    }

    Status closeContext(ContextPtr& context) override {
        return releaseTypedHandle<RealContext>(context, "Context");
    }

    Status createJfc(const ContextPtr& context, const JfcOptions& options,
                     JfcPtr& output) override {
        std::shared_ptr<RuntimeLease> runtime;
        CHECK_STATUS(getRuntime(runtime));
        auto real_context = std::dynamic_pointer_cast<RealContext>(context);
        if (!real_context || !real_context->valid()) {
            return invalidHandle("Context");
        }
        if (options.depth == 0 || options.receiver_depth == 0) {
            return Status::InvalidArgument("JFC depths must be positive");
        }
        const auto& caps = real_context->deviceInfo().capabilities;
        if (caps.max_jfc != 0 && caps.max_jfc < 2) {
            return Status::InvalidArgument(
                "URMA device cannot provide send and receive JFCs");
        }
        if (caps.max_jfc_depth != 0 &&
            (options.depth > caps.max_jfc_depth ||
             options.receiver_depth > caps.max_jfc_depth)) {
            return Status::InvalidArgument(
                "requested JFC depth exceeds device capability");
        }
        if (caps.max_jfr_depth != 0 &&
            options.receiver_depth > caps.max_jfr_depth) {
            return Status::InvalidArgument(
                "requested JFR depth exceeds device capability");
        }

        auto jfc = std::make_shared<RealJfc>(std::move(real_context));
        auto status = jfc->initialize(options);
        if (!status.ok()) {
            // initialize may already own a JFCE/JFC/JFR prefix. Return the
            // wrapper alongside the error so the caller can retain it and
            // drive retryable delete instead of relying on its destructor.
            output = std::move(jfc);
            return status;
        }
        output = std::move(jfc);
        return Status::OK();
    }

    Status deleteJfc(JfcPtr& jfc) override {
        return releaseTypedHandle<RealJfc>(jfc, "Jfc");
    }

    Status registerLocalSegment(const ContextPtr& context, uint64_t address,
                                size_t length, const SegmentOptions& options,
                                LocalSegmentPtr& output) override {
        std::shared_ptr<RuntimeLease> runtime;
        CHECK_STATUS(getRuntime(runtime));
        auto real_context = std::dynamic_pointer_cast<RealContext>(context);
        if (!real_context || !real_context->valid()) {
            return invalidHandle("Context");
        }
        CHECK_STATUS(validateSegmentOptions(options));
        if (address == 0 || length == 0 ||
            length > std::numeric_limits<uint64_t>::max() - address) {
            return Status::InvalidArgument("invalid local segment range");
        }

        urma_reg_seg_flag_t flags{};
        flags.bs.token_policy = URMA_TOKEN_NONE;
        flags.bs.cacheable =
            options.cacheable ? URMA_CACHEABLE : URMA_NON_CACHEABLE;
        flags.bs.access = nativeAccess(options.access);

        urma_seg_cfg_t config{};
        config.va = address;
        config.len = length;
        config.token_value.token = options.token;
        config.flag = flags;
        urma_target_seg_t* native_segment =
            urma_register_seg(real_context->native(), &config);
        if (native_segment == nullptr) {
            return nativePointerError("urma_register_seg");
        }

        urma_seg_t wire_descriptor{};
        wire_descriptor.ubva = native_segment->seg.ubva;
        wire_descriptor.len = native_segment->seg.len;
        wire_descriptor.attr = native_segment->seg.attr;
        wire_descriptor.token_id = native_segment->seg.token_id;

        SegmentDescriptor descriptor;
        descriptor.urma_api_version = URMA_API_VERSION;
        descriptor.urma_abi_size = sizeof(urma_seg_t);
        descriptor.hex = encodeHex(&wire_descriptor, sizeof(wire_descriptor));
        output = std::make_shared<RealLocalSegment>(
            std::move(real_context), native_segment, address, length,
            std::move(descriptor));
        return Status::OK();
    }

    Status unregisterLocalSegment(LocalSegmentPtr& segment) override {
        return releaseTypedHandle<RealLocalSegment>(segment, "LocalSegment");
    }

    Status importRemoteSegment(const ContextPtr& context,
                               const SegmentDescriptor& descriptor,
                               const SegmentOptions& options,
                               RemoteSegmentPtr& output) override {
        std::shared_ptr<RuntimeLease> runtime;
        CHECK_STATUS(getRuntime(runtime));
        auto real_context = std::dynamic_pointer_cast<RealContext>(context);
        if (!real_context || !real_context->valid()) {
            return invalidHandle("Context");
        }
        CHECK_STATUS(validateSegmentOptions(options));
        if (descriptor.schema_version != SegmentDescriptor::kSchemaVersion) {
            return Status::InvalidArgument(
                "unsupported URMA segment descriptor schema");
        }
        if (descriptor.urma_api_version != URMA_API_VERSION) {
            return Status::InvalidArgument(
                "URMA segment descriptor API version mismatch");
        }
        if (descriptor.urma_abi_size != sizeof(urma_seg_t)) {
            return Status::InvalidArgument(
                "URMA segment descriptor ABI size mismatch");
        }

        urma_seg_t native_descriptor{};
        if (!decodeHex(descriptor.hex, &native_descriptor,
                       sizeof(native_descriptor))) {
            return Status::InvalidArgument(
                "malformed URMA segment descriptor hex");
        }
        if (native_descriptor.len == 0 ||
            native_descriptor.len > std::numeric_limits<uint64_t>::max() -
                                        native_descriptor.ubva.va ||
            native_descriptor.attr.bs.reserved != 0) {
            return Status::InvalidArgument(
                "invalid URMA segment descriptor contents");
        }

        urma_import_seg_flag_t flags{};
        flags.bs.cacheable =
            options.cacheable ? URMA_CACHEABLE : URMA_NON_CACHEABLE;
        flags.bs.access = nativeAccess(options.access);
        flags.bs.mapping = URMA_SEG_NOMAP;
        urma_token_t token{.token = options.token};
        urma_target_seg_t* imported = urma_import_seg(
            real_context->native(), &native_descriptor, &token, 0, flags);
        if (imported == nullptr) return nativePointerError("urma_import_seg");

        // urma_ubva_t is packed; copy its fields before passing them through
        // forwarding references used by make_shared.
        const uint64_t remote_address = native_descriptor.ubva.va;
        const uint64_t remote_length = native_descriptor.len;
        output = std::make_shared<RealRemoteSegment>(std::move(real_context),
                                                     imported, remote_address,
                                                     remote_length, descriptor);
        return Status::OK();
    }

    Status unimportRemoteSegment(RemoteSegmentPtr& segment) override {
        return releaseTypedHandle<RealRemoteSegment>(segment, "RemoteSegment");
    }

    Status createJetty(const ContextPtr& context, const JfcPtr& jfc,
                       const JettyOptions& options, JettyPtr& output) override {
        std::shared_ptr<RuntimeLease> runtime;
        CHECK_STATUS(getRuntime(runtime));
        auto real_context = std::dynamic_pointer_cast<RealContext>(context);
        auto real_jfc = std::dynamic_pointer_cast<RealJfc>(jfc);
        if (!real_context || !real_context->valid()) {
            return invalidHandle("Context");
        }
        if (!real_jfc || !real_jfc->valid()) return invalidHandle("Jfc");
        if (real_jfc->context().get() != real_context.get()) {
            return Status::InvalidArgument(
                "Jfc belongs to a different Context");
        }
        if (options.depth == 0 || options.max_sge == 0 ||
            options.priority > URMA_MAX_PRIORITY || options.rnr_retry > 7 ||
            options.error_timeout > 31) {
            return Status::InvalidArgument("invalid Jetty options");
        }
        const auto& caps = real_context->deviceInfo().capabilities;
        if (caps.max_jetty_depth != 0 && options.depth > caps.max_jetty_depth) {
            return Status::InvalidArgument(
                "requested Jetty depth exceeds device capability");
        }
        if ((caps.max_send_sge != 0 && options.max_sge > caps.max_send_sge) ||
            (caps.max_remote_sge != 0 &&
             options.max_sge > caps.max_remote_sge)) {
            return Status::InvalidArgument(
                "requested Jetty SGE count exceeds device capability");
        }

        auto jetty = std::make_shared<RealJetty>(std::move(real_context),
                                                 std::move(real_jfc));
        auto status = jetty->initialize(options);
        if (!status.ok()) {
            output = std::move(jetty);
            return status;
        }
        output = std::move(jetty);
        return Status::OK();
    }

    Status deleteJetty(JettyPtr& jetty) override {
        return releaseTypedHandle<RealJetty>(jetty, "Jetty");
    }

    Status bindJetty(const JettyPtr& jetty,
                     const RemoteJettyInfo& remote) override {
        std::shared_ptr<RuntimeLease> runtime;
        CHECK_STATUS(getRuntime(runtime));
        auto real_jetty = std::dynamic_pointer_cast<RealJetty>(jetty);
        if (!real_jetty || !real_jetty->valid()) return invalidHandle("Jetty");
        if (remote.id == 0) {
            return Status::InvalidArgument("remote Jetty ID must be non-zero");
        }
        urma_eid_t remote_eid{};
        if (!parseEid(remote.eid, remote_eid) || isAllZero(remote_eid)) {
            return Status::InvalidArgument("invalid or null remote EID");
        }
        return real_jetty->bind(remote, remote_eid);
    }

    Status unbindJetty(const JettyPtr& jetty) override {
        if (!jetty) return Status::OK();
        auto real_jetty = std::dynamic_pointer_cast<RealJetty>(jetty);
        if (!real_jetty || !real_jetty->valid()) return invalidHandle("Jetty");
        return real_jetty->unbind();
    }

    Status resetJetty(const JettyPtr& jetty) override {
        if (!jetty) return Status::OK();
        auto real_jetty = std::dynamic_pointer_cast<RealJetty>(jetty);
        if (!real_jetty || !real_jetty->valid()) return invalidHandle("Jetty");
        return real_jetty->reset();
    }

    Status quiesceJetty(const JettyPtr& jetty, uint32_t timeout_ms,
                        std::vector<Completion>& completions) override {
        completions.clear();
        std::shared_ptr<RuntimeLease> runtime;
        CHECK_STATUS(getRuntime(runtime));
        auto real_jetty = std::dynamic_pointer_cast<RealJetty>(jetty);
        if (!real_jetty || !real_jetty->valid()) return invalidHandle("Jetty");
        if (timeout_ms == 0 || real_jetty->depth() == 0) {
            return Status::InvalidArgument(
                "Jetty quiesce requires a non-zero timeout and depth");
        }
        if (real_jetty->flushFenced()) return Status::OK();

        auto real_jfc = real_jetty->jfc();
        if (!real_jfc || !real_jfc->valid()) return invalidHandle("Jfc");

        // Holding the JFC poll lock before entering ERROR makes consumption
        // of the provider's fake FLUSH_ERR_DONE marker atomic with respect to
        // normal pollers. Holding the Jetty's own lock inside beginError also
        // fences a post already crossing the native boundary.
        std::unique_lock<std::mutex> poll_lock(real_jfc->pollMutex());
        if (real_jetty->flushFenced()) return Status::OK();
        if (!real_jetty->errorStarted()) {
            // Native Jetty IDs may be reused after deletion. Only discard a
            // stale marker when starting a genuinely new ERROR epoch.
            real_jfc->clearFlushDone(real_jetty->id());
        }
        CHECK_STATUS(real_jetty->beginError());

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        bool flush_done = real_jfc->hasFlushDone(real_jetty->id());
        std::array<urma_cr_t, 64> polled{};
        while (!flush_done) {
            const int count =
                urma_poll_jfc(real_jfc->nativeSendJfc(),
                              static_cast<int>(polled.size()), polled.data());
            if (count < 0) return nativeError("urma_poll_jfc", count);
            for (int i = 0; i < count; ++i) {
                const auto& native = polled[static_cast<size_t>(i)];
                if (native.status == URMA_CR_WR_FLUSH_ERR_DONE) {
                    real_jfc->rememberFlushDone(native.local_id);
                }
                auto completion = convertCompletion(native);
                if (completion.token != 0) {
                    real_jfc->releaseSegment(completion.token);
                    completions.push_back(completion);
                }
            }
            flush_done = flush_done || real_jfc->hasFlushDone(real_jetty->id());
            if (flush_done) break;
            if (std::chrono::steady_clock::now() >= deadline) {
                return Status::RdmaError(
                    "timed out waiting for URMA Jetty flush-done fence");
            }
            std::this_thread::sleep_for(std::chrono::microseconds(20));
        }

        const size_t flush_batch =
            std::min<size_t>(real_jetty->depth(), polled.size());
        size_t total_flushed = 0;
        while (true) {
            const int count =
                urma_flush_jetty(real_jetty->native(),
                                 static_cast<int>(flush_batch), polled.data());
            if (count < 0) return nativeError("urma_flush_jetty", count);
            if (count == 0) break;
            total_flushed += static_cast<size_t>(count);
            if (total_flushed > real_jetty->depth()) {
                return Status::InternalError(
                    "URMA Jetty flush exceeded its queue depth");
            }
            for (int i = 0; i < count; ++i) {
                auto completion =
                    convertCompletion(polled[static_cast<size_t>(i)]);
                if (completion.token == 0) {
                    return Status::InternalError(
                        "URMA Jetty flush returned an entity marker");
                }
                real_jfc->releaseSegment(completion.token);
                completions.push_back(completion);
            }
        }
        real_jfc->clearFlushDone(real_jetty->id());
        real_jfc->releaseSegmentsForJetty(real_jetty->id());
        real_jetty->markFlushFenced();
        return Status::OK();
    }

    Status post(const JettyPtr& jetty, const std::vector<WorkRequest>& requests,
                size_t& posted_count) override {
        posted_count = 0;
        std::shared_ptr<RuntimeLease> runtime;
        CHECK_STATUS(getRuntime(runtime));
        auto real_jetty = std::dynamic_pointer_cast<RealJetty>(jetty);
        if (!real_jetty || !real_jetty->valid()) return invalidHandle("Jetty");
        if (requests.empty()) return Status::OK();

        struct NativeWorkRequest {
            urma_jfs_wr_t wr{};
            urma_sge_t local_sge{};
            urma_sge_t remote_sge{};
        };
        std::vector<NativeWorkRequest> native_requests(requests.size());

        std::lock_guard<std::mutex> jetty_lock(real_jetty->mutex());
        if (!real_jetty->postable() || real_jetty->remote() == nullptr) {
            return Status::InvalidArgument("Jetty is not bound and postable");
        }
        for (size_t i = 0; i < requests.size(); ++i) {
            const WorkRequest& request = requests[i];
            if (request.token == 0) {
                return Status::InvalidArgument(
                    "work request token zero is reserved");
            }
            if (request.length == 0 ||
                request.length > std::numeric_limits<uint32_t>::max()) {
                return Status::InvalidArgument("invalid work request length");
            }

            auto local = std::dynamic_pointer_cast<RealLocalSegment>(
                request.local_segment);
            auto remote = std::dynamic_pointer_cast<RealRemoteSegment>(
                request.remote_segment);
            if (!local || !local->valid()) return invalidHandle("LocalSegment");
            if (!remote || !remote->valid()) {
                return invalidHandle("RemoteSegment");
            }
            if (local->context().get() != real_jetty->context().get() ||
                remote->context().get() != real_jetty->context().get()) {
                return Status::InvalidArgument(
                    "work request segments belong to another Context");
            }
            if (!checkedRangeContains(local->address(), local->length(),
                                      request.local_address, request.length) ||
                !checkedRangeContains(remote->address(), remote->length(),
                                      request.remote_address, request.length)) {
                return Status::InvalidArgument(
                    "work request lies outside a registered segment");
            }

            NativeWorkRequest& native = native_requests[i];
            native.local_sge.addr = request.local_address;
            native.local_sge.len = static_cast<uint32_t>(request.length);
            native.local_sge.tseg = local->native();
            native.remote_sge.addr = request.remote_address;
            native.remote_sge.len = static_cast<uint32_t>(request.length);
            native.remote_sge.tseg = remote->native();

            native.wr.opcode = request.operation == Operation::READ
                                   ? URMA_OPC_READ
                                   : URMA_OPC_WRITE;
            native.wr.flag.bs.complete_enable = 1;
            native.wr.tjetty = real_jetty->remote();
            native.wr.user_ctx = request.token;
            if (request.operation == Operation::READ) {
                native.wr.rw.src.sge = &native.remote_sge;
                native.wr.rw.dst.sge = &native.local_sge;
            } else {
                native.wr.rw.src.sge = &native.local_sge;
                native.wr.rw.dst.sge = &native.remote_sge;
            }
            native.wr.rw.src.num_sge = 1;
            native.wr.rw.dst.num_sge = 1;
            native.wr.next =
                i + 1 == requests.size() ? nullptr : &native_requests[i + 1].wr;
        }

        // Retain both segment handles before crossing the native post
        // boundary. This prevents buffer removal from unregistering memory
        // while a WR is still in flight. poll() releases the references by
        // completion token.
        CHECK_STATUS(
            real_jetty->jfc()->retainSegments(real_jetty->id(), requests));

        urma_jfs_wr_t* bad_wr = nullptr;
        const int rc = urma_post_jetty_send_wr(
            real_jetty->native(), &native_requests.front().wr, &bad_wr);
        if (rc == URMA_SUCCESS) {
            posted_count = requests.size();
            return Status::OK();
        }
        if (bad_wr != nullptr) {
            for (size_t i = 0; i < native_requests.size(); ++i) {
                if (bad_wr == &native_requests[i].wr) {
                    posted_count = i;
                    break;
                }
            }
        }
        for (size_t i = posted_count; i < requests.size(); ++i) {
            real_jetty->jfc()->releaseSegment(requests[i].token);
        }
        return nativeError("urma_post_jetty_send_wr", rc);
    }

    Status poll(const JfcPtr& jfc, size_t max_completions,
                std::vector<Completion>& completions) override {
        completions.clear();
        std::shared_ptr<RuntimeLease> runtime;
        CHECK_STATUS(getRuntime(runtime));
        auto real_jfc = std::dynamic_pointer_cast<RealJfc>(jfc);
        if (!real_jfc || !real_jfc->valid()) return invalidHandle("Jfc");
        if (max_completions == 0 ||
            max_completions > static_cast<size_t>(INT_MAX)) {
            return Status::InvalidArgument("invalid maximum completion count");
        }

        std::vector<urma_cr_t> native_completions(max_completions);
        std::lock_guard<std::mutex> lock(real_jfc->pollMutex());
        const int count = urma_poll_jfc(real_jfc->nativeSendJfc(),
                                        static_cast<int>(max_completions),
                                        native_completions.data());
        if (count < 0) return nativeError("urma_poll_jfc", count);

        completions.reserve(static_cast<size_t>(count));
        for (int i = 0; i < count; ++i) {
            const urma_cr_t& native = native_completions[i];
            Completion completion = convertCompletion(native);
            if (native.status == URMA_CR_WR_FLUSH_ERR_DONE) {
                real_jfc->rememberFlushDone(native.local_id);
            }
            completions.push_back(completion);
            real_jfc->releaseSegment(completion.token);
        }
        return Status::OK();
    }

   private:
    Status getRuntime(std::shared_ptr<RuntimeLease>& output) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!runtime_) {
            return Status::InvalidArgument(
                "URMA adapter is not initialized or has been shut down");
        }
        output = runtime_;
        return Status::OK();
    }

    mutable std::mutex mutex_;
    std::shared_ptr<RuntimeLease> runtime_;
};

#endif  // defined(TENT_HAS_REAL_URMA) && TENT_HAS_REAL_URMA

}  // namespace

std::shared_ptr<UrmaAdapter> createDefaultUrmaAdapter() {
#if defined(TENT_HAS_REAL_URMA) && TENT_HAS_REAL_URMA
    return std::make_shared<RealUrmaAdapter>();
#else
    return std::make_shared<UnavailableUrmaAdapter>();
#endif
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake
