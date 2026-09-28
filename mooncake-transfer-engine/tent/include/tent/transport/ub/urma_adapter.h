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

#ifndef TENT_TRANSPORT_UB_URMA_ADAPTER_H_
#define TENT_TRANSPORT_UB_URMA_ADAPTER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {
namespace ub {

// Adapter-neutral subset of the capabilities needed by the TENT UB data
// plane. Values of zero mean that the provider did not report a limit.
struct DeviceCapabilities {
    uint32_t max_jfc = 0;
    uint32_t max_jfc_depth = 0;
    uint32_t max_jfr_depth = 0;
    uint32_t max_jetty = 0;
    uint32_t max_jetty_depth = 0;
    uint32_t max_send_sge = 0;
    uint32_t max_remote_sge = 0;
    uint64_t max_message_size = 0;
    uint32_t max_read_size = 0;
    uint32_t max_write_size = 0;
    uint32_t feature_flags = 0;
    uint16_t transport_modes = 0;
};

// One DeviceInfo represents one native URMA device/EID pair. A native device
// with multiple EIDs therefore produces multiple entries. topology_name is a
// stable TENT-facing identity and native_device_name is passed only to URMA.
struct DeviceInfo {
    std::string topology_name;
    std::string native_device_name;
    std::string native_device_path;
    uint32_t eid_index = 0;
    std::string eid;
    bool active = false;
    DeviceCapabilities capabilities;
};

// The segment descriptor is intentionally an explicit envelope. Raw
// urma_seg_t bytes are ABI-specific; import must reject a different API
// version, structure size, schema, or malformed hex instead of copying an
// arbitrary byte sequence into a native structure.
struct SegmentDescriptor {
    static constexpr uint32_t kSchemaVersion = 1;

    uint32_t schema_version = kSchemaVersion;
    uint32_t urma_api_version = 0;
    uint32_t urma_abi_size = 0;
    std::string hex;
};

// Native handles never escape through this interface. Concrete adapters own
// their raw handles and make explicit release operations retryable. All typed
// handles are reference counted so slices can retain segments until their
// completion has been consumed.
class OpaqueHandle {
   public:
    OpaqueHandle() = default;
    virtual ~OpaqueHandle() = default;

    OpaqueHandle(const OpaqueHandle&) = delete;
    OpaqueHandle& operator=(const OpaqueHandle&) = delete;

    [[nodiscard]] virtual bool valid() const noexcept = 0;
};

class Context : public OpaqueHandle {
   public:
    [[nodiscard]] virtual const DeviceInfo& deviceInfo() const noexcept = 0;
    [[nodiscard]] virtual int asyncFd() const noexcept = 0;
};

class Jfc : public OpaqueHandle {
   public:
    // Returns -1 when completion events were not requested.
    [[nodiscard]] virtual int eventFd() const noexcept = 0;
};

class LocalSegment : public OpaqueHandle {
   public:
    [[nodiscard]] virtual uint64_t address() const noexcept = 0;
    [[nodiscard]] virtual uint64_t length() const noexcept = 0;
    [[nodiscard]] virtual const SegmentDescriptor& descriptor()
        const noexcept = 0;
};

class RemoteSegment : public OpaqueHandle {
   public:
    [[nodiscard]] virtual uint64_t address() const noexcept = 0;
    [[nodiscard]] virtual uint64_t length() const noexcept = 0;
    [[nodiscard]] virtual const SegmentDescriptor& descriptor()
        const noexcept = 0;
};

class Jetty : public OpaqueHandle {
   public:
    [[nodiscard]] virtual uint32_t id() const noexcept = 0;
    [[nodiscard]] virtual uint32_t uasid() const noexcept = 0;
};

using ContextPtr = std::shared_ptr<Context>;
using JfcPtr = std::shared_ptr<Jfc>;
using LocalSegmentPtr = std::shared_ptr<LocalSegment>;
using RemoteSegmentPtr = std::shared_ptr<RemoteSegment>;
using JettyPtr = std::shared_ptr<Jetty>;

enum SegmentAccess : uint32_t {
    SEGMENT_ACCESS_READ = 1U << 0,
    SEGMENT_ACCESS_WRITE = 1U << 1,
    SEGMENT_ACCESS_ATOMIC = 1U << 2,
    // Local CPU/URMA access remains unrestricted, but the provider must reject
    // every access initiated by a remote endpoint.
    SEGMENT_ACCESS_LOCAL_ONLY = 1U << 3,
};

struct SegmentOptions {
    uint32_t access = SEGMENT_ACCESS_READ | SEGMENT_ACCESS_WRITE;
    uint32_t token = 0xACFE;
    bool cacheable = false;
};

struct JfcOptions {
    uint32_t depth = 4096;
    uint32_t receiver_depth = 2048;
    uint32_t token = 0xACFE;
    bool enable_completion_events = false;
};

struct JettyOptions {
    uint32_t depth = 256;
    uint8_t priority = 15;
    uint8_t max_sge = 1;
    uint8_t rnr_retry = 7;
    uint8_t error_timeout = 17;
};

struct RemoteJettyInfo {
    std::string eid;
    uint32_t id = 0;
    uint32_t uasid = 0;
    uint32_t token = 0xACFE;
};

enum class Operation : uint8_t {
    READ,
    WRITE,
};

struct WorkRequest {
    Operation operation = Operation::READ;
    uint64_t local_address = 0;
    uint64_t remote_address = 0;
    size_t length = 0;

    // Zero is reserved for native completion records that do not correspond
    // to a posted work request (for example a synthetic flush-done record).
    uint64_t token = 0;

    LocalSegmentPtr local_segment;
    RemoteSegmentPtr remote_segment;
};

enum class CompletionCategory : uint8_t {
    SUCCESS,
    LOCAL_DEVICE_ERROR,
    REMOTE_PATH_ERROR,
    ENDPOINT_ERROR,
    MEMORY_ERROR,
    TIMEOUT,
    UNKNOWN_ERROR,
};

struct Completion {
    CompletionCategory category = CompletionCategory::UNKNOWN_ERROR;
    int native_status = 0;
    uint64_t token = 0;
    uint32_t completed_bytes = 0;
    // Native Jetty ID that produced the completion. This is also populated
    // for entity-level flush markers whose token is deliberately zero.
    uint32_t local_jetty_id = 0;
};

// Pure, injectable URMA boundary. It deliberately does not know about TENT
// Request, SubBatch, UbTask, UbSlice, scheduling, retry, or rail health.
class UrmaAdapter {
   public:
    virtual ~UrmaAdapter() = default;

    [[nodiscard]] virtual bool available() const noexcept = 0;
    [[nodiscard]] virtual uint32_t nativeApiVersion() const noexcept = 0;
    [[nodiscard]] virtual size_t nativeSegmentDescriptorSize()
        const noexcept = 0;

    virtual Status initialize() = 0;
    virtual Status shutdown() = 0;

    virtual Status discoverDevices(std::vector<DeviceInfo>& devices) = 0;

    virtual Status openContext(const DeviceInfo& device,
                               ContextPtr& context) = 0;
    // close/delete/unregister/unimport reset the caller's shared_ptr only after
    // the provider has released the native resource. If another owner still
    // retains the handle, or if the provider returns busy/error, the operation
    // fails and leaves the shared_ptr intact for a later retry. Calling any of
    // these methods again with a null handle is successful.
    virtual Status closeContext(ContextPtr& context) = 0;

    virtual Status createJfc(const ContextPtr& context,
                             const JfcOptions& options, JfcPtr& jfc) = 0;
    virtual Status deleteJfc(JfcPtr& jfc) = 0;

    virtual Status registerLocalSegment(const ContextPtr& context,
                                        uint64_t address, size_t length,
                                        const SegmentOptions& options,
                                        LocalSegmentPtr& segment) = 0;
    virtual Status unregisterLocalSegment(LocalSegmentPtr& segment) = 0;

    virtual Status importRemoteSegment(const ContextPtr& context,
                                       const SegmentDescriptor& descriptor,
                                       const SegmentOptions& options,
                                       RemoteSegmentPtr& segment) = 0;
    virtual Status unimportRemoteSegment(RemoteSegmentPtr& segment) = 0;

    virtual Status createJetty(const ContextPtr& context, const JfcPtr& jfc,
                               const JettyOptions& options,
                               JettyPtr& jetty) = 0;
    virtual Status deleteJetty(JettyPtr& jetty) = 0;
    virtual Status bindJetty(const JettyPtr& jetty,
                             const RemoteJettyInfo& remote) = 0;
    virtual Status unbindJetty(const JettyPtr& jetty) = 0;
    virtual Status resetJetty(const JettyPtr& jetty) = 0;

    // Synchronously fences one Jetty. A successful return guarantees that no
    // previously posted WR on this Jetty can perform any further DMA. The
    // implementation transitions the native Jetty to ERROR, consumes the
    // provider's flush-done marker, and returns both already-processed and
    // unhandled WR completions. Callers must dispatch every non-zero token
    // through their normal completion path before resetting or deleting the
    // Jetty. On failure no drain guarantee is made and native resources must
    // remain alive.
    virtual Status quiesceJetty(const JettyPtr& jetty, uint32_t timeout_ms,
                                std::vector<Completion>& completions) = 0;

    // On a native post error, posted_count is the number of leading requests
    // accepted before the provider's bad WR. Requests counted as posted must
    // still be completed through poll().
    virtual Status post(const JettyPtr& jetty,
                        const std::vector<WorkRequest>& requests,
                        size_t& posted_count) = 0;
    virtual Status poll(const JfcPtr& jfc, size_t max_completions,
                        std::vector<Completion>& completions) = 0;
};

// Returns the raw-liburma implementation when TENT_HAS_REAL_URMA is enabled;
// otherwise returns an injectable-compatible stub whose operational methods
// report Status::NotImplemented with an explicit unavailable message.
std::shared_ptr<UrmaAdapter> createDefaultUrmaAdapter();

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_URMA_ADAPTER_H_
