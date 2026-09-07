#pragma once

// The stable DataManager abstraction used by every P2P caller
// (P2PClientService, ClientRpcService, HARecoveryManager). It carries only
// methods that have a real production caller, so that an alternative local
// data plane can be dropped in without touching any of those callers.
//
// Two implementations live behind it:
//   - DataManagerV1 (p2p/client/v1/data_manager_v1.h)  — TieredBackend based
//   - DataManagerV2 (p2p/client/v2/data_manager_v2.h)  — Tiler/Block based
//
// Deliberately NOT on this interface:
//   - ReadRemoteData / WriteRemoteData / TransferData: the synchronous
//     variants have no production caller (the RPC handlers use the *Async
//     forms), so they stay as DataManagerV1-only public methods.
//   - DrainForTest and friends: see p2p/client/data_manager_test_hook.h.

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <async_simple/Executor.h>
#include <async_simple/Future.h>
#include <async_simple/coro/Lazy.h>
#include <ylt/util/tl/expected.hpp>

#include "client_buffer.hpp"
#include "p2p/client/client_rpc_types.h"
#include "p2p/client/data_manager_types.h"
#include "p2p/client/task_handle.h"
#include "transfer_engine.h"
#include "types.h"

namespace mooncake {

/**
 * @class DataManager
 * @brief Local data plane for one P2P client: local IO, remote IO, the
 *        PreWrite/Pin forward protocol, topology reporting and route rectify.
 *
 * Lifetime rules that both implementations must honour:
 *  - the destructor is virtual, and `Stop()` / `Destroy()` are idempotent;
 *  - no `std::string_view` argument may be retained past the call. Task
 *    handles returned by Put/Get run their work at `Wait()` time and therefore
 *    own a copy of the key; the caller only has to keep the *slices* alive.
 */
class DataManager {
   public:
    virtual ~DataManager() = default;

    // ================================================================
    // Lifecycle
    // ================================================================

    /** Reject new requests, drain in-flight work, stop background threads. */
    virtual void Stop() = 0;

    /** Release tiers and notify the external metadata system. After Stop(). */
    virtual void Destroy() = 0;

    // ================================================================
    // Local IO
    // ================================================================

    // The Put operation consists of three phases:
    // 1. Allocation: reserve space for the object
    // 2. Write: write the data into the reserved space
    // 3. Commit: make the object visible
    //
    // IMPORTANT: the caller must keep the memory referenced by `slices` alive
    // from the time Put() returns until TaskHandle::Wait() completes. The key
    // does NOT have to outlive the call: implementations copy it.
    virtual tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode> Put(
        std::string_view key, std::vector<Slice>& slices) = 0;

    // IMPORTANT: the caller must keep the memory referenced by `slices` alive
    // from the time Get() returns until TaskHandle::Wait() completes. The key
    // does NOT have to outlive the call.
    virtual tl::expected<ReadTaskHandle, ErrorCode> Get(
        std::string_view key, const std::vector<Slice>& slices) = 0;

    virtual tl::expected<ReadTaskHandle, ErrorCode> Get(
        std::string_view key,
        std::shared_ptr<ClientBufferAllocator> allocator) = 0;

    // ================================================================
    // Metadata
    // ================================================================

    /** @return (tier id of a readable replica, object size). */
    virtual tl::expected<std::pair<UUID, uint64_t>, ErrorCode> Query(
        std::string_view key) = 0;

    virtual tl::expected<size_t, ErrorCode> QueryObjectSize(
        std::string_view key) = 0;

    /** Exact existence check; never satisfied from a cached presence hint. */
    virtual bool Exist(std::string_view key,
                       std::optional<UUID> tier_id = std::nullopt) const = 0;

    virtual tl::expected<void, ErrorCode> Delete(
        std::string_view key, std::optional<UUID> tier_id = std::nullopt,
        bool notify_master = true) = 0;

    /** @return number of distinct keys removed. */
    virtual tl::expected<long, ErrorCode> RemoveAll() = 0;

    // ================================================================
    // Topology / HA
    // ================================================================

    virtual std::vector<TierView> GetTierViews() const = 0;

    /** All tier ids that currently hold an exact replica of `key`. */
    virtual std::vector<UUID> GetReplicaTierIds(std::string_view key) const = 0;

    /**
     * @brief Iterate all local replicas in batches.
     *
     * Granularity is per-replica, not per-key: a key with replicas on N tiers
     * produces N ReplicaLocation entries. HARecoveryManager resyncs metadata
     * per (key, tier_id) and GetLocalKeyCount sums batch sizes, so changing
     * this to per-key would change both. Returning false stops the walk.
     */
    virtual void ForEachKeyBatch(
        const std::function<bool(std::vector<ReplicaLocation>&&)>& callback)
        const = 0;

    /**
     * @param hot_key_num nullopt = implementation default; 0 = all tracked
     *        keys (subject to an internal snapshot cap).
     */
    virtual AccessStats GetHotKeyStats(
        std::optional<size_t> hot_key_num = std::nullopt) const = 0;

    // ================================================================
    // Remote IO
    // ================================================================

    /** Reverse read: local object -> peer buffers, via TransferEngine. */
    virtual async_simple::coro::Lazy<tl::expected<void, ErrorCode>>
    ReadRemoteDataAsync(std::string_view key,
                        const std::vector<RemoteBufferDesc>& dest_buffers) = 0;

    /** Reverse write: peer buffers -> local object. @return target tier id. */
    virtual async_simple::coro::Lazy<tl::expected<UUID, ErrorCode>>
    WriteRemoteDataAsync(std::string_view key,
                         const std::vector<RemoteBufferDesc>& src_buffers,
                         std::optional<UUID> tier_id = std::nullopt) = 0;

    // ================================================================
    // Forward (PreWrite / Pin) protocol
    // ================================================================

    virtual tl::expected<PreWriteResponse, ErrorCode> PreWrite(
        std::string_view key, size_t size_bytes,
        std::optional<UUID> tier_id = std::nullopt) = 0;

    virtual tl::expected<void, ErrorCode> WriteCommit(
        std::string_view key, const UUID& write_operation_id) = 0;

    virtual tl::expected<void, ErrorCode> WriteRevoke(
        std::string_view key, const UUID& write_operation_id) = 0;

    virtual tl::expected<PinKeyResponse, ErrorCode> PinKey(
        std::string_view key, std::optional<UUID> tier_id = std::nullopt) = 0;

    virtual tl::expected<void, ErrorCode> UnPinKey(
        std::string_view key, const UUID& read_operation_id) = 0;

    // ================================================================
    // TransferEngine
    // ================================================================

    /**
     * @brief Transfer between local TE-ready memory and remote buffers.
     * @param opcode WRITE: local -> peer_buffers; READ: peer_buffers -> local
     *
     * The returned Future must always complete, including after Stop().
     */
    virtual async_simple::Future<tl::expected<void, ErrorCode>>
    TransferDataAsync(void* local_transfer_base, size_t total_size,
                      const std::vector<RemoteBufferDesc>& peer_buffers,
                      Transport::TransferRequest::OpCode opcode) = 0;

    /** Never null; owned by the DataManager. */
    virtual async_simple::Executor* GetCoroExecutor() const = 0;

    // ================================================================
    // Route rectification
    // ================================================================

    /**
     * @brief Best-effort: when `key` is not found locally, ask Master to drop
     *        the stale replica route.
     *
     * The miss check and the callback are deliberately not atomic with respect
     * to a concurrent Put/WriteCommit, so a false positive (a remove for a
     * replica that has just become visible) is possible and accepted; holding
     * a metadata lock across a Master RPC costs more.
     */
    virtual void RectifyReadRoute(
        std::string_view key, std::optional<UUID> tier_id = std::nullopt) = 0;

    /** Later calls replace earlier ones; an empty function disables rectify. */
    virtual void SetRectifyCallback(RectifyRouteCallback fn) = 0;
};

}  // namespace mooncake
