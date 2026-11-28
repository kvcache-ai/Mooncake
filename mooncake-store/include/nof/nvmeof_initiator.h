#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "types.h"  // ErrorCode

namespace mooncake {

// Opaque segment handle. Its definition lives inside the initiator
// implementation (e.g. spdk_initiator.cpp); callers only ever hold pointers.
//
// Identity contract: repeated OpenSegment() with the same transport string
// returns handles bound to the same underlying queue pair, and the returned
// pointer is stable for the lifetime of the initiator. The pointer value is
// used as the worker-affinity / QoS map key.
class NofSegmentHandle;

enum class NofIOOp : int {
    kRead = 0,
    kWrite = 1,
    kNum = 2,
};

constexpr uint32_t kInvalidBlockSize = 0xFFFFFFFF;

// Neutral completion result: no spdk_nvme_cpl outside the implementation.
struct NofIOCompletion {
    bool success = false;
    int sc = 0;                // NVMe status code; valid when !success
    int sct = 0;               // NVMe status code type; valid when !success
    std::string error_string;  // empty on success
};

// Raw function pointer + void* so the callback itself carries no hidden
// allocation.
using NofIOCallback = void (*)(void* ctx, const NofIOCompletion& completion);

// Two-slot callback adaptor. Callers embed one in their own (typically
// pooled) sub-task storage and pass its address to SubmitIO; this keeps the
// steady-state submit/completion path free of per-sub-IO heap allocation.
struct NofIOAdaptor {
    NofIOCallback cb;
    void* ctx;
};

struct NofCapabilities {
    bool supports_sgl = false;  // PR #3251 validation hooks
    // Buffer-pointer alignment requirement. Live consumer: the submitter's
    // alignment gate uses it (ptr % (supports_sgl ? dma_alignment
    // : block_size)); see transfer_task.cpp submitNofOperation.
    uint32_t dma_alignment = 4;  // DWORD alignment
    // Upper-bound semantics: true means "this build CAN require
    // registration" (RDMA translation table). Not transport-specific (TCP
    // NVMe-oF doesn't need it); a registration attempt on TCP is harmless,
    // so callers must NOT skip RegisterMemory based on this flag.
    bool requires_memory_registration = true;
};

class NVMeoFInitiator {
   public:
    virtual ~NVMeoFInitiator() = default;

    // Returns a cached, process-unique-per-instance handle for the endpoint,
    // or nullptr on failure. Thread-safe; may be called from any thread.
    virtual NofSegmentHandle* OpenSegment(const std::string& transport_str) = 0;

    // Synchronous probe: issues a 1-block read and polls until done or
    // timeout. Used by the master heartbeat path only.
    virtual bool ProbeSegment(const std::string& transport_str,
                              uint32_t timeout_ms,
                              std::string* error_reason) = 0;

    // kInvalidBlockSize on invalid handle.
    virtual uint32_t GetBlockSize(const NofSegmentHandle* handle) = 0;

    // Submit one I/O.
    //
    // @pre buffer/byte_offset/byte_length are block-size aligned (interface
    //      precondition; implementations reject violations with -EINVAL).
    // @pre Must be called from the thread that polls this handle
    //      (qpair single-thread constraint).
    // @pre adaptor must remain valid until its callback runs.
    // Callback contract: invoked synchronously on the polling thread inside
    // PollCompletion, never from a background thread. Any scratch DMA
    // buffers the implementation needs are released before the callback
    // returns.
    virtual int SubmitIO(NofSegmentHandle* handle, void* buffer,
                         uint64_t byte_offset, uint64_t byte_length, NofIOOp op,
                         NofIOAdaptor* adaptor) = 0;

    // Poll completions on the calling thread. max_completions == 0 means
    // "all pending" (SPDK dialect, now an explicit contract).
    virtual int64_t PollCompletion(NofSegmentHandle* handle,
                                   uint32_t max_completions) = 0;

    // Closes #3131: register a user buffer with the initiator's DMA
    // translation table. No-op (OK) for initiators without one.
    //
    // Registrations are tracked per (initiator instance, ptr): re-registering
    // a covered ptr is an idempotent no-op; a larger size extends coverage.
    // Two instances registering the same ptr each hold an independent
    // reference, so one instance's UnregisterMemory never unmaps pages the
    // other still relies on. The bookkeeping is process-global and outlives
    // any instance.
    //
    // Return contract:
    //   - OK: the range is fully registered (external pages count as
    //     covered).
    //   - other errors except NOF_REGISTRATION_STUCK: failed, everything the
    //     failed call touched is confirmed clean; the caller may munmap
    //     without a pairing UnregisterMemory. Note this covers only the
    //     failed call itself: when a Register extending an earlier,
    //     smaller registration fails, that earlier registration is still
    //     live and still requires its own UnregisterMemory.
    //   - NOF_REGISTRATION_STUCK: cleanup of the failed attempt could not be
    //     confirmed. The affected pages are terminally quarantined (never
    //     unregistered/re-registered/munmapped). The caller MUST treat the
    //     range as still registered and route it through UnregisterMemory at
    //     teardown; a later Register extends only over the unregistered tail.
    //
    // UnregisterMemory returns OK when the range is fully released (or was
    // never registered by this instance); INTERNAL_ERROR means some pages
    // could not be released (which also quarantines them), and the record is
    // retained so the caller's mapping quarantines instead of munmapping.
    //
    // SPDK requires BOTH vaddr and len to be 2MB-aligned and fails with
    // -EBUSY on already-registered pages (verified against v23.01.1
    // lib/env_dpdk/memory.c), so implementations normalize the range to
    // whole 2MB pages and keep a per-page refcount — adjacent/overlapping
    // buffers sharing a page must neither double-register it nor unregister
    // it early.
    virtual ErrorCode RegisterMemory(void* ptr, size_t size) = 0;
    virtual ErrorCode UnregisterMemory(void* ptr) = 0;

    virtual NofCapabilities GetCapabilities() const = 0;
};

// Registration page granularity fixed by the RegisterMemory contract above.
// A range rejected by this check is never touched by RegisterMemory, so a
// caller that skips the attempt keeps the mapping munmap-safe without a
// pairing UnregisterMemory; anything accepted here may still fail inside the
// implementation (reported via the RegisterMemory return code — see its
// three-outcome contract for the cleanup/NOF_REGISTRATION_STUCK semantics).
inline bool NofRangeIsRegistrable(const void* ptr, size_t size) {
    constexpr uintptr_t kNofRegistrationPageSize = 2ULL << 20;  // 2MB
    // spdk_mem_register() also rejects addresses outside the 256TB window it
    // can represent (MASK_256TB) before marking anything (memory.c:339-348).
    constexpr uintptr_t kNofAddressMask = (1ULL << 48) - 1;  // MASK_256TB
    const uintptr_t vaddr = reinterpret_cast<uintptr_t>(ptr);
    return ptr != nullptr && size != 0 && (vaddr & ~kNofAddressMask) == 0 &&
           (vaddr % kNofRegistrationPageSize) == 0 &&
           (size % kNofRegistrationPageSize) == 0;
}

}  // namespace mooncake
