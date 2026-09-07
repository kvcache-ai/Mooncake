#pragma once

// Copier: one way of moving bytes from one place to another inside this node.
//
// V2 used to have exactly one local copy path -- read the source in chunks
// into a scratch buffer, write the chunks into the destination -- which is
// correct for every medium and optimal for none. It also silently ignored
// LocalTransferConfig, so a deployment that asked for the TransferEngine got a
// bounce buffer instead.
//
// A Copier is the unit of "how". Routing picks one from the copy domains and
// capabilities of the two endpoints, never from MemoryType: two tiers can
// share a medium and still need different paths (different NUMA nodes,
// different devices, one registered with the TransferEngine and one not).
//
// Copiers are registered per DataManager instance, not in a process-wide
// singleton, so two managers in one process cannot see each other's fakes and
// a component test can register a FakeCopier without touching global state.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "p2p/client/v2/block.h"
#include "p2p/client/v2/v2_common.h"
#include "types.h"

namespace mooncake::v2 {

/**
 * @enum CopyDomain
 * @brief Where the bytes physically live, at the granularity routing cares
 *        about. Deliberately coarser than MemoryType on one axis (several
 *        media share kFileOrBlock) and finer on another (host memory that a
 *        device cannot reach is still kHostMemory, but the endpoint's
 *        capability flags say what may touch it).
 */
enum class CopyDomain : uint8_t {
    /** Directly addressable by the calling CPU. */
    kHostMemory,
    /** Reached through offsets and IO calls: SSD, file, block device. */
    kFileOrBlock,
    /** Behind a device runtime (Ascend, GPU) that owns its own copy calls. */
    kDevice,
    /** Anything else; only the generic Read/Write path can serve it. */
    kOpaque,
};

const char* ToString(CopyDomain domain);

/**
 * @struct CopyEndpoint
 * @brief One side of a copy. Non-owning: the caller guarantees the underlying
 *        block or buffer outlives the call.
 *
 * Exactly one of `handle` and `host_buffer` is set. The host-buffer form is
 * what lets Put/Get share the routing table with migration: caller slices are
 * an endpoint like any other, instead of a separate code path.
 */
struct CopyEndpoint {
    /** Set for a block endpoint. */
    BlockDataHandle* handle = nullptr;
    /** Set for a plain host-memory endpoint. */
    void* host_buffer = nullptr;

    /** Byte offset into the block or buffer. */
    size_t offset = 0;
    /** Bytes available from `offset`. */
    size_t capacity = 0;

    CopyDomain domain = CopyDomain::kOpaque;
    /** The calling CPU may dereference this endpoint directly. */
    bool direct_cpu_access = false;
    /** The pool this endpoint belongs to was registered with an engine. */
    bool te_addressable = false;
    /**
     * The block's own address, when it has one. For DRAM this is the host
     * pointer whether or not the arena was registered with a TransferEngine;
     * `te_addressable` is the separate question of whether an engine may use
     * it.
     */
    std::optional<TransferAddress> address;
    /** IO size this endpoint prefers. 0 or 1 means "no preference". */
    size_t preferred_io_size = 1;
    size_t minimum_alignment = 1;
    /** Which logical tiler this endpoint belongs to; {0,0} for caller memory.
     */
    UUID tiler_id{0, 0};

    /** A block endpoint covering `size` bytes of `handle` from `offset`. */
    static CopyEndpoint FromBlock(BlockDataHandle& handle, size_t offset,
                                  size_t size,
                                  const BlockPoolCapabilities& capabilities,
                                  const UUID& tiler_id);

    /**
     * @brief The same, deriving the routing facts from the handle itself.
     *
     * Used where the caller has a block but not its pool: the handle knows
     * whether the CPU can reach it, which is the fact routing needs.
     */
    static CopyEndpoint FromHandle(BlockDataHandle& handle, size_t offset,
                                   size_t size, const UUID& tiler_id);

    /** A caller-memory endpoint. */
    static CopyEndpoint FromHost(void* buffer, size_t size);

    /** The host address of this endpoint, if it has one. */
    void* HostAddress() const;

    bool IsBlock() const { return handle != nullptr; }
};

/**
 * @brief Cooperative cancellation. Null means "cannot be cancelled".
 *
 * Shared rather than a reference so a queued request can outlive the caller
 * that created it, which is exactly what happens when a batch is still waiting
 * while its producer is being torn down.
 */
using CancellationToken = std::shared_ptr<const std::atomic<bool>>;

bool IsCancelled(const CancellationToken& token);

/**
 * @struct CopyRequest
 */
struct CopyRequest {
    CopyEndpoint source;
    CopyEndpoint destination;
    size_t length = 0;
    /** Unset means no deadline. Checked before starting and between chunks. */
    std::optional<Clock::time_point> deadline;
    CancellationToken cancellation;
};

/**
 * @struct CopyResult
 * @brief Per-request outcome. A batch returns one of these per item, because
 *        one item failing must not re-run the ones that succeeded.
 */
struct CopyResult {
    ErrorCode status = ErrorCode::OK;
    size_t copied_bytes = 0;
    /**
     * Transient (device busy, short write) rather than deterministic (bad
     * arguments, unreachable domain). Only a retryable failure is worth
     * backing off and trying again; retrying the other kind just burns the
     * budget before reporting the same error.
     */
    bool retryable = false;

    bool Ok() const { return status == ErrorCode::OK; }

    static CopyResult Success(size_t bytes) {
        return CopyResult{ErrorCode::OK, bytes, false};
    }
    static CopyResult Failure(ErrorCode status, bool retryable = false) {
        return CopyResult{status, 0, retryable};
    }
};

/**
 * @struct CopierCapabilities
 * @brief What a copier is, for metrics and for the "batch scheduling" versus
 *        "batch data path" distinction the design insists on: a copier that
 *        loops over Copy() must not be reported as doing batched IO.
 */
struct CopierCapabilities {
    /** Stable identifier; used as a metric label, so never a key. */
    std::string name;
    /** True only if BatchCopy is a real batched data path, not a loop. */
    bool native_batch = false;
    /** This copier needs an intermediate host buffer to do its work. */
    bool requires_staging = false;
    /** 0 means "no preference"; otherwise the IO size the copier likes. */
    size_t preferred_chunk_bytes = 0;
};

/**
 * @class Copier
 */
class Copier {
   public:
    virtual ~Copier() = default;

    /**
     * @brief Can this copier serve that pair of endpoints?
     *
     * A predicate rather than a capability table because the answer depends on
     * both sides at once, and on facts (an address is present, the CPU can
     * touch it) rather than on a medium enum.
     */
    virtual bool CanCopy(const CopyEndpoint& source,
                         const CopyEndpoint& destination) const = 0;

    virtual CopyResult Copy(const CopyRequest& request) = 0;

    /**
     * @brief Optional batched form.
     *
     * The default loops over Copy, which is always correct. Override it only
     * when the underlying device really does have a batched submission path,
     * and set native_batch so the metrics do not claim batching that is not
     * happening.
     */
    virtual std::vector<CopyResult> BatchCopy(
        std::span<const CopyRequest> requests);

    virtual CopierCapabilities Capabilities() const = 0;
};

/**
 * @class CopierRegistry
 * @brief The instance-level routing table.
 *
 * Registration order is priority order: Route returns the first copier that
 * accepts the pair, so the generic fallback is registered last. The registry
 * is frozen when the DataManager starts; after that it is read-only and needs
 * no lock on the copy path.
 */
class CopierRegistry {
   public:
    /** @return INVALID_PARAMS after Freeze(), or for a null copier. */
    tl::expected<void, ErrorCode> Register(std::unique_ptr<Copier> copier);

    /** No further registration. Idempotent. */
    void Freeze();
    bool IsFrozen() const { return frozen_; }

    /**
     * @brief First copier accepting the pair, or nullptr.
     *
     * Counts the choice on the way out, so the metrics say which path the
     * bytes actually took rather than which one was configured.
     */
    Copier* Route(const CopyEndpoint& source,
                  const CopyEndpoint& destination) const;

    size_t Size() const { return copiers_.size(); }
    std::vector<CopierCapabilities> Describe() const;
    /** Route hits per copier, in registration order. */
    std::vector<uint64_t> Uses() const;

   private:
    std::vector<std::unique_ptr<Copier>> copiers_;
    // One counter per copier. Held by pointer because std::atomic is neither
    // copyable nor movable and the vector grows during registration.
    mutable std::vector<std::unique_ptr<std::atomic<uint64_t>>> uses_;
    bool frozen_ = false;
};

class TransferCoordinator;

/**
 * @struct CopierConfig
 * @brief The knobs the copy layer actually reads.
 *
 * Deliberately separate from LocalTransferConfig, which V1 shares: `mode` and
 * `te_endpoint` are a property of the deployment's transport and belong there,
 * while these two are properties of this copy engine. Putting them in the
 * shared struct would give V1 two fields it ignores.
 *
 * The design's section 10 also lists `copier_workers`. That knob already
 * exists as DataManagerV2Config::movement_worker_count -- the threads that run
 * copies are the movement workers -- and adding a second name for it would let
 * the two disagree, so it is not duplicated here.
 */
struct CopierConfig {
    /**
     * Upper bound on one staged Read/Write chunk, and the size of a staging
     * buffer. Larger means fewer IO round trips and more memory held per
     * copying thread; 0 keeps the built-in default.
     */
    size_t staging_buffer_bytes = 4ULL * 1024 * 1024;

    /**
     * Wall-clock budget for one local copy. Zero means unbounded, which is
     * the right answer for a migration nobody is waiting on and the wrong one
     * for a copy on a request path -- so the caller passes a deadline of its
     * own when it has one, and this is the backstop.
     */
    std::chrono::milliseconds copy_timeout{0};
};

tl::expected<void, ErrorCode> ValidateCopierConfig(const CopierConfig& config);

// ---------------------------------------------------------------------------
// The built-in copiers
// ---------------------------------------------------------------------------

/**
 * @brief memcpy between two endpoints the CPU can dereference.
 *
 * The common case -- DRAM tier to DRAM tier, caller slices to a DRAM block --
 * and the one the old single-path engine served worst: it bounced every byte
 * through a scratch buffer, so a DRAM-to-DRAM copy moved each byte twice.
 */
std::unique_ptr<Copier> CreateDramMemcpyCopier();

/**
 * @brief Chunked Read/Write through a reusable staging buffer.
 *
 * The universal fallback: it needs nothing from an endpoint except the generic
 * handle interface, so it serves every pair including SSD-to-SSD. It also
 * covers the file/block case rather than a separate FileOrBlockCopier, because
 * the only difference would be the chunk size, which it already takes from the
 * endpoints' preferred IO size. A distinct implementation is worth adding when
 * there is a real vectored-IO path to call, not before.
 *
 * The staging buffer is thread-local and reused across calls: a migration
 * worker would otherwise allocate and free a multi-megabyte buffer per block.
 */
std::unique_ptr<Copier> CreateStagedReadWriteCopier(size_t default_chunk_bytes);

/**
 * @brief The TransferEngine path, for a deployment that asked for it.
 *
 * Only offered when the destination is registered with the engine and the
 * source can be handed over as a host address. Submission, polling, timeout
 * and batch release are delegated to the TransferCoordinator, which already
 * owns that lifecycle; duplicating it here is how two half-correct versions of
 * "cancel means drain-then-free" come to exist.
 *
 * @param local_endpoint This node's TransferEngine endpoint -- a local copy is
 *        a transfer whose peer is ourselves.
 */
std::unique_ptr<Copier> CreateTransferEngineCopier(
    TransferCoordinator* coordinator, std::string local_endpoint);

}  // namespace mooncake::v2
