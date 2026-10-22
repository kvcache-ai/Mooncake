#pragma once

// LocalCopyEngine: data movement inside this node.
//
// V2 splits transfer into two abstractions. The TransferEngine still owns
// node-to-node movement of registered memory; everything local -- filling a
// block from caller slices, draining one into caller slices, and copying
// between two pools -- goes through here.
//
// It is a router, not a copy loop. Every request is turned into a pair of
// CopyEndpoints and handed to the first Copier in this instance's registry
// that accepts them, so a DRAM-to-DRAM copy is a memcpy, an SSD block is moved
// through the generic Read/Write interface without ever pretending to be
// memory, and a deployment that configured the TransferEngine actually gets
// it. The registry is per instance and frozen at Init, so a component test can
// register a fake copier without touching global state.

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "p2p/client/data_manager_types.h"
#include "p2p/client/v2/block.h"
#include "p2p/client/v2/copier.h"
#include "types.h"

namespace mooncake::v2 {

class TransferCoordinator;

/**
 * @struct LocalCopyStats
 */
struct LocalCopyStats {
    uint64_t copies = 0;
    uint64_t bytes = 0;
    uint64_t failures = 0;
    /** No registered copier accepted the endpoints. Always a bug. */
    uint64_t unroutable = 0;
    /** Per copier, in registration order; pairs with `copier_names`. */
    std::vector<std::string> copier_names;
    std::vector<uint64_t> copier_uses;
};

/**
 * @class LocalCopyEngine
 */
class LocalCopyEngine {
   public:
    /**
     * @param coordinator Optional. Without one there is no TransferEngine
     *        copier, whatever the configured mode says, and the engine logs
     *        that at construction rather than silently downgrading later.
     */
    explicit LocalCopyEngine(const LocalTransferConfig& config,
                             TransferCoordinator* coordinator = nullptr,
                             const CopierConfig& copier_config = {},
                             std::shared_ptr<Clock> clock = nullptr);

    /**
     * @brief Add a copier ahead of the built-in ones.
     *
     * Init only; the registry is frozen once the manager starts. Registration
     * order is priority order, so anything added here outranks the defaults.
     */
    tl::expected<void, ErrorCode> RegisterCopier(
        std::unique_ptr<Copier> copier);

    /** Copy an entire source block into a destination block. */
    tl::expected<void, ErrorCode> Copy(const ImmutableBlock& source,
                                       MutableBlock& destination) const;

    /**
     * @brief Scatter a block across caller slices.
     *
     * The slices must cover the whole object; a short destination is an error
     * rather than a truncated read.
     */
    tl::expected<void, ErrorCode> ReadToSlices(
        const ImmutableBlock& source, const std::vector<Slice>& slices) const;

    /** Gather caller slices into a freshly allocated block. */
    tl::expected<void, ErrorCode> WriteFromSlices(
        const std::vector<Slice>& slices, MutableBlock& destination) const;

    /** Copy a block into a plain buffer (the staging path). */
    tl::expected<void, ErrorCode> ReadToBuffer(const ImmutableBlock& source,
                                               void* destination,
                                               size_t size) const;

    LocalCopyStats Stats() const;

    /** Which copiers are registered, in priority order. */
    std::vector<CopierCapabilities> Describe() const;

   private:
    /** Route, run, and count. */
    tl::expected<void, ErrorCode> Run(const CopyRequest& request) const;

    LocalTransferConfig config_;
    CopierConfig copier_config_;
    std::shared_ptr<Clock> clock_;
    CopierRegistry registry_;

    mutable std::atomic<uint64_t> copies_{0};
    mutable std::atomic<uint64_t> bytes_{0};
    mutable std::atomic<uint64_t> failures_{0};
    mutable std::atomic<uint64_t> unroutable_{0};
};

}  // namespace mooncake::v2
