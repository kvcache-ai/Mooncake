#pragma once

// TierGraph: which tiers may send data to which, and how.
//
// V2 previously derived topology from priority alone: "the next tier with a
// strictly lower priority" was the offload target and "the fastest addressable
// tier" was the onboard target. That is a total order pretending to be a
// graph, and it cannot express deployments that already exist -- three or more
// levels where not every pair is connected, two tiers at the same priority
// that must not feed each other, several devices of the same type reachable by
// different copy paths, or a link that only works through a staging buffer.
//
// So the topology is stated explicitly. Nodes are tiler UUIDs and carry the
// copy domain a movement lands in; an edge says a movement from A to B is
// allowed, what it costs, roughly how fast it is and whether it needs
// staging. Priority survives as a policy input -- it still says which tier is
// "faster" -- but it is no longer the only relation, and no code derives an
// edge from it.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <boost/functional/hash.hpp>
#include <ylt/util/tl/expected.hpp>

#include "p2p/client/v2/copier.h"
#include "p2p/client/v2/movement.h"
#include "p2p/client/v2/movement_tracker.h"
#include "p2p/client/v2/v2_common.h"
#include "types.h"

namespace mooncake::v2 {

/**
 * @struct TierNode
 */
struct TierNode {
    UUID tiler_id{0, 0};
    /** Higher is faster. Ties are legal and are not an ordering. */
    int32_t priority = 0;
    size_t capacity = 0;
    /**
     * May hold a request-path allocation and publish a TE address.
     *
     * Read by the placement policy, which will not onboard into a tier that
     * is not addressable: an onboard exists to make the block servable, and a
     * tier the request path cannot read serves nothing.
     */
    bool addressable = false;
    CopyDomain domain = CopyDomain::kOpaque;
};

/**
 * @struct TierEdge
 * @brief A permitted one-way movement. Direction is explicit: A->B being legal
 *        says nothing about B->A, which matters for a write-once archive tier.
 */
struct TierEdge {
    UUID source{0, 0};
    UUID destination{0, 0};
    /**
     * Relative cost of moving a byte along this edge. Only compared, never
     * interpreted, so any consistent unit works.
     */
    double cost = 1.0;
    /**
     * Rough capability, for scheduling. 0 means unknown. Surfaces on
     * PlacementDecision, copied off whichever edge the target was chosen on.
     */
    size_t bandwidth_bytes_per_second = 0;
    /** The copy needs an intermediate host buffer. Also on the decision. */
    bool requires_staging = false;
};

/**
 * @class TierGraph
 * @brief Immutable after Build(). V2 does not support dynamic mount/unmount,
 *        so lookups need no lock.
 */
class TierGraph {
   public:
    TierGraph() = default;

    /**
     * @brief Validate and freeze.
     * @return INVALID_PARAMS for an unknown endpoint, a self-edge, a
     *         duplicate edge, or an edge between two tiers of equal priority
     *         -- the graph answers "strictly slower" and "strictly faster"
     *         only, so a peer edge could be configured but never selected. A
     *         tier with no edges at all is legal (a single tier deployment)
     *         and is not an error.
     */
    static tl::expected<TierGraph, ErrorCode> Build(
        std::vector<TierNode> nodes, std::vector<TierEdge> edges);

    /**
     * @brief Derive the classic priority chain.
     *
     * Every tier gets an edge to the next strictly slower one and back. This
     * is what a configuration without an explicit topology means, and keeping
     * it as an explicit construction -- rather than as a fallback inside the
     * policy -- is what stops "derive it from priority" from creeping back in.
     */
    static tl::expected<TierGraph, ErrorCode> FromPriorityChain(
        std::vector<TierNode> nodes);

    const TierNode* Node(const UUID& tiler_id) const;
    const TierEdge* Edge(const UUID& source, const UUID& destination) const;

    /** Edges leaving `source`, cheapest first. */
    std::vector<const TierEdge*> OutgoingFrom(const UUID& source) const;

    /** Nodes strictly slower than `from` and reachable in one hop. */
    std::vector<const TierNode*> SlowerNeighbours(const UUID& from) const;
    /**
     * @brief Nodes strictly faster than `from` and reachable in one hop.
     *
     * Topology only: a faster tier that cannot publish a TE address is still
     * a faster tier and is still listed here. Whether it is a useful onboard
     * target is the policy's question, not the graph's.
     */
    std::vector<const TierNode*> FasterNeighbours(const UUID& from) const;

    const std::vector<TierNode>& Nodes() const { return nodes_; }
    size_t EdgeCount() const { return edges_.size(); }
    bool Empty() const { return nodes_.empty(); }

   private:
    struct EdgeKey {
        UUID source{0, 0};
        UUID destination{0, 0};
        bool operator==(const EdgeKey&) const = default;
    };
    struct EdgeKeyHash {
        size_t operator()(const EdgeKey& key) const noexcept;
    };

    std::vector<TierNode> nodes_;
    std::vector<TierEdge> edges_;
    std::unordered_map<UUID, size_t, boost::hash<UUID>> node_index_;
    std::unordered_map<EdgeKey, size_t, EdgeKeyHash> edge_index_;
};

/**
 * @struct PlacementContext
 * @brief Everything a placement decision may look at.
 *
 * Notably absent: anything that would let the policy hold state. It is handed
 * the facts and returns a decision; the facts come from their owners
 * (BlockIndex, FrequencyTracker, the tier's usage) and are never cached here.
 */
struct PlacementContext {
    std::string_view key;
    UUID source_tiler{0, 0};
    MovementDirection direction = MovementDirection::kOffload;
    size_t size_bytes = 0;
    /** Current window frequency from the FrequencyTracker. */
    double frequency = 0.0;
    /**
     * Usage ratio of a candidate tier, in [0, 1]. Supplied as a callback
     * rather than a snapshot so the policy sees live numbers without owning a
     * refresh loop.
     */
    std::function<double(const UUID&)> usage_ratio;
    /**
     * True when the candidate already holds this block. The policy must not
     * answer this itself -- only the destination BlockIndex can, under the
     * canonical registration identity.
     */
    std::function<bool(const UUID&)> already_present;
};

/**
 * @struct PlacementDecision
 */
struct PlacementDecision {
    UUID destination_tiler{0, 0};
    MovementKind kind = MovementKind::kReplicate;
    /**
     * The queue this movement belongs to: the tiler pair plus the two copy
     * domains it crosses, which are the two nodes' own.
     */
    MovementRoute route;
    /**
     * The copy needs an intermediate host buffer, copied from the chosen
     * edge. Without it a deployment that declares a staging-only link gets a
     * direct copy attempted on it, because nothing downstream can see which
     * edge the target was picked on.
     *
     * Beside the route rather than inside it: MovementRoute is the queue
     * identity shared with the movement pipeline, and Build allows only one
     * edge per (source, destination) pair, so there is never a second,
     * differently staged path for the same route to be told apart from.
     */
    bool requires_staging = false;
    /** Rough capability of the chosen edge, for scheduling. 0 is unknown. */
    size_t bandwidth_bytes_per_second = 0;
};

/**
 * @class TierPlacementPolicy
 * @brief Chooses a target and a movement kind on an allowed edge. Nothing
 *        else: it does not copy, does not count accesses, does not remember
 *        cooldowns, and does not decide whether a movement is worth doing.
 */
class TierPlacementPolicy {
   public:
    virtual ~TierPlacementPolicy() = default;

    /** nullopt means "no legal, useful target", which is not an error. */
    virtual std::optional<PlacementDecision> Select(
        const PlacementContext& context) const = 0;
};

/**
 * @struct TierPlacementPolicyConfig
 */
struct TierPlacementPolicyConfig {
    /** Only "nearest_neighbour" today: the cheapest legal one-hop edge. */
    std::string type = "nearest_neighbour";
    /**
     * Skip a candidate already above this usage ratio: offloading into a tier
     * that is itself about to reclaim just moves the problem one level down.
     */
    double max_destination_usage = 0.95;
    /**
     * Whether an onboard keeps the slow copy. Kept explicit because "replicate
     * up and delete the source" is a real policy some deployments want, and
     * hiding it inside the copy implementation is how it becomes unreviewable.
     */
    bool onboard_keeps_source = true;
};

tl::expected<void, ErrorCode> ValidateTierPlacementPolicyConfig(
    const TierPlacementPolicyConfig& config);

tl::expected<std::unique_ptr<TierPlacementPolicy>, ErrorCode>
CreateTierPlacementPolicy(const TierPlacementPolicyConfig& config,
                          std::shared_ptr<const TierGraph> graph);

}  // namespace mooncake::v2
