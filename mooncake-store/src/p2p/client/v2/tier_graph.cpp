#include "p2p/client/v2/tier_graph.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>

namespace mooncake::v2 {
namespace {

/**
 * @brief Cost of an edge derived from a priority chain.
 *
 * Every derived edge costs the same. Priority already says which way is down;
 * turning it into a cost as well would dress one configured number up as two
 * independent measurements, and the cheapest-first ordering would then look
 * like it came from the hardware.
 */
constexpr double kChainEdgeCost = 1.0;

TierEdge ChainEdge(const UUID& source, const UUID& destination) {
    TierEdge edge;
    edge.source = source;
    edge.destination = destination;
    edge.cost = kChainEdgeCost;
    return edge;
}

/**
 * @brief Shapes that are legal, buildable and can never move anything.
 *
 * Warnings, not errors: an archive-only deployment is a real choice, and a
 * config that is merely useless must not stop a node from starting. But both
 * shapes are invisible once running -- the policy answers "nowhere to go",
 * which is exactly what a healthy bottom tier answers -- so they are said
 * once, at the only moment anyone is looking at the topology.
 */
void WarnAboutSilentlyInertTopology(const TierGraph& graph) {
    if (graph.Empty()) {
        LOG(WARNING) << "tier_graph: built with no tiers; every placement "
                        "decision will be 'nowhere to go'";
        return;
    }
    for (const TierNode& node : graph.Nodes()) {
        if (node.addressable) continue;
        bool has_faster = false;
        bool has_addressable_faster = false;
        for (const TierNode* faster : graph.FasterNeighbours(node.tiler_id)) {
            has_faster = true;
            if (faster->addressable) {
                has_addressable_faster = true;
                break;
            }
        }
        if (has_faster && !has_addressable_faster) {
            LOG(WARNING) << "tier_graph: tier " << node.tiler_id
                         << " has faster neighbours but none of them is "
                            "addressable, so a hot block on it can never be "
                            "onboarded anywhere the request path can read";
        }
    }
}

}  // namespace

size_t TierGraph::EdgeKeyHash::operator()(const EdgeKey& key) const noexcept {
    size_t seed = boost::hash<UUID>{}(key.source);
    boost::hash_combine(seed, boost::hash<UUID>{}(key.destination));
    return seed;
}

tl::expected<TierGraph, ErrorCode> TierGraph::Build(
    std::vector<TierNode> nodes, std::vector<TierEdge> edges) {
    TierGraph graph;

    graph.node_index_.reserve(nodes.size());
    for (size_t i = 0; i < nodes.size(); ++i) {
        if (!graph.node_index_.emplace(nodes[i].tiler_id, i).second) {
            // Two tiers answering to one id: every lookup would resolve to
            // whichever won the insert, and the loser would silently never be
            // routed to, with nothing in the topology to show why.
            LOG(ERROR) << "tier_graph: duplicate node " << nodes[i].tiler_id;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    graph.edge_index_.reserve(edges.size());
    for (size_t i = 0; i < edges.size(); ++i) {
        const TierEdge& edge = edges[i];
        if (edge.source == edge.destination) {
            // A tier copying to itself is never a movement; taken literally
            // it would migrate a block onto its own tier and then delete the
            // source, which is the one path that loses data outright.
            LOG(ERROR) << "tier_graph: self edge on node " << edge.source;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!graph.node_index_.contains(edge.source) ||
            !graph.node_index_.contains(edge.destination)) {
            // Rejected rather than dropped: a typo in a tiler id would
            // otherwise produce a graph that starts, looks healthy and
            // quietly never offloads.
            LOG(ERROR) << "tier_graph: edge " << edge.source << " -> "
                       << edge.destination << " names an unknown tier";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        // Indexed into the caller's vector on purpose: node_index_ is
        // already populated but nodes_ is not assigned until validation has
        // passed, so this loop is the only place the two can be read
        // together.
        const TierNode& from = nodes[graph.node_index_.at(edge.source)];
        const TierNode& to = nodes[graph.node_index_.at(edge.destination)];
        if (from.priority == to.priority) {
            // A peer edge. Rejected rather than stored and ignored: the only
            // relations the graph answers are "strictly slower" and
            // "strictly faster", so no direction a policy can ask for would
            // ever return this edge. Keeping it would put a configured
            // rebalance path in the topology, in EdgeCount() and in
            // OutgoingFrom(), and move nothing along it -- the same "starts,
            // looks healthy, quietly never moves anything" failure the
            // unknown-endpoint check above exists to prevent. A rebalance
            // needs a direction that can express it, not an edge nothing
            // reads.
            LOG(ERROR) << "tier_graph: edge " << edge.source << " -> "
                       << edge.destination << " joins two tiers of equal "
                       << "priority " << from.priority
                       << ", which no placement direction can select";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        const EdgeKey key{edge.source, edge.destination};
        if (!graph.edge_index_.emplace(key, i).second) {
            // Edge() can only answer with one of them, so the other's cost
            // and staging flag would be honoured when choosing the target and
            // ignored when the route is built.
            LOG(ERROR) << "tier_graph: duplicate edge " << edge.source << " -> "
                       << edge.destination;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    // A node with no edge at all is deliberately fine: a single-tier
    // deployment is the common small case, and refusing it here would force
    // every caller to special-case "one tier" before building a graph.
    graph.nodes_ = std::move(nodes);
    graph.edges_ = std::move(edges);
    // Diagnostics run here and not in the loops above: they ask the graph its
    // own questions, and the neighbour queries need nodes_ and edges_, which
    // only exist from this point on.
    WarnAboutSilentlyInertTopology(graph);
    return graph;
}

tl::expected<TierGraph, ErrorCode> TierGraph::FromPriorityChain(
    std::vector<TierNode> nodes) {
    // Descending, and stable: within one priority the configured order is the
    // only tie-break there is, and the classic "first strictly slower tier"
    // answer depends on it.
    std::vector<const TierNode*> by_priority;
    by_priority.reserve(nodes.size());
    for (const auto& node : nodes) by_priority.push_back(&node);
    std::stable_sort(by_priority.begin(), by_priority.end(),
                     [](const TierNode* lhs, const TierNode* rhs) {
                         return lhs->priority > rhs->priority;
                     });

    // Half-open [begin, end) ranges of equal priority, fastest level first.
    std::vector<std::pair<size_t, size_t>> levels;
    for (size_t begin = 0; begin < by_priority.size();) {
        size_t end = begin;
        while (end < by_priority.size() &&
               by_priority[end]->priority == by_priority[begin]->priority) {
            ++end;
        }
        levels.emplace_back(begin, end);
        begin = end;
    }

    std::vector<TierEdge> edges;
    // Level to level, never node to node. Two tiers sharing a priority are
    // peers: offloading sideways onto an equally full peer only moves the
    // problem, and it is the one edge the old priority lookup refused to
    // return. Linking levels leaves peers unconnected by construction, so no
    // later filter has to remember to exclude them.
    for (size_t level = 0; level + 1 < levels.size(); ++level) {
        const auto [faster_begin, faster_end] = levels[level];
        const auto [slower_begin, slower_end] = levels[level + 1];
        for (size_t f = faster_begin; f < faster_end; ++f) {
            for (size_t s = slower_begin; s < slower_end; ++s) {
                const UUID& faster = by_priority[f]->tiler_id;
                const UUID& slower = by_priority[s]->tiler_id;
                // Emitted in the sorted order so that, with all chain costs
                // equal, OutgoingFrom's stable order reproduces exactly what
                // the old descending-list scan picked.
                edges.push_back(ChainEdge(faster, slower));
                edges.push_back(ChainEdge(slower, faster));
            }
        }
    }

    // Through Build so a chain is validated like any other topology: the
    // duplicate-node check in particular has nothing to do with edges.
    return Build(std::move(nodes), std::move(edges));
}

const TierNode* TierGraph::Node(const UUID& tiler_id) const {
    auto it = node_index_.find(tiler_id);
    if (it == node_index_.end()) return nullptr;
    return &nodes_[it->second];
}

const TierEdge* TierGraph::Edge(const UUID& source,
                                const UUID& destination) const {
    auto it = edge_index_.find(EdgeKey{source, destination});
    if (it == edge_index_.end()) return nullptr;
    return &edges_[it->second];
}

std::vector<const TierEdge*> TierGraph::OutgoingFrom(const UUID& source) const {
    // Scanned rather than served from an adjacency list: the graph holds one
    // edge per configured link, a deployment has a handful, and a second
    // container would be a second thing to keep consistent with edges_.
    std::vector<const TierEdge*> outgoing;
    for (const auto& edge : edges_) {
        if (edge.source == source) outgoing.push_back(&edge);
    }
    // Cheapest first, stably: equal-cost edges keep their configured order, so
    // the target a deployment gets is the one it listed first rather than
    // whatever the sort happened to do on this run.
    std::stable_sort(outgoing.begin(), outgoing.end(),
                     [](const TierEdge* lhs, const TierEdge* rhs) {
                         return lhs->cost < rhs->cost;
                     });
    return outgoing;
}

std::vector<const TierNode*> TierGraph::SlowerNeighbours(
    const UUID& from) const {
    const TierNode* source = Node(from);
    if (source == nullptr) return {};

    std::vector<const TierNode*> neighbours;
    for (const TierEdge* edge : OutgoingFrom(from)) {
        const TierNode* destination = Node(edge->destination);
        // Strictly slower. Build rejects an edge between equal priorities, so
        // equality cannot reach here on a graph that exists; the comparison
        // stays strict as the statement of what "slower" means rather than as
        // a filter that has work to do.
        if (destination != nullptr &&
            destination->priority < source->priority) {
            neighbours.push_back(destination);
        }
    }
    return neighbours;
}

std::vector<const TierNode*> TierGraph::FasterNeighbours(
    const UUID& from) const {
    const TierNode* source = Node(from);
    if (source == nullptr) return {};

    std::vector<const TierNode*> neighbours;
    // Outgoing, not incoming: onboarding copies out of `from`, so the edge
    // that has to exist is from->faster. A write-once archive that everyone
    // may write to has no way back up, and this is where that shows.
    for (const TierEdge* edge : OutgoingFrom(from)) {
        const TierNode* destination = Node(edge->destination);
        // Strict for the same reason as SlowerNeighbours: Build has already
        // refused the only edge equality could describe.
        if (destination != nullptr &&
            destination->priority > source->priority) {
            neighbours.push_back(destination);
        }
    }
    return neighbours;
}

namespace {

/**
 * @class NearestNeighbourPolicy
 * @brief The cheapest legal one-hop edge in the requested direction.
 *
 * One hop, never a shortcut to the bottom: a full DRAM tier hands its coldest
 * blocks to CXL, and CXL decides for itself whether they carry on to SSD. The
 * alternative -- picking the slowest tier that has room -- throws away the
 * intermediate levels a deployment paid for.
 *
 * Holds the shared graph and the config and nothing else. Select() is const,
 * touches no per-key state and takes no lock, which is what makes it safe to
 * call from every consumer thread at once and safe for the context callbacks
 * to reach into BlockIndex and the tier usage while they run.
 */
class NearestNeighbourPolicy final : public TierPlacementPolicy {
   public:
    NearestNeighbourPolicy(const TierPlacementPolicyConfig& config,
                           std::shared_ptr<const TierGraph> graph)
        : config_(config), graph_(std::move(graph)) {}

    std::optional<PlacementDecision> Select(
        const PlacementContext& context) const override {
        const TierNode* source = graph_->Node(context.source_tiler);
        if (source == nullptr) return std::nullopt;

        const bool offload = context.direction == MovementDirection::kOffload;
        const std::vector<const TierNode*> candidates =
            offload ? graph_->SlowerNeighbours(context.source_tiler)
                    : graph_->FasterNeighbours(context.source_tiler);

        for (const TierNode* candidate : candidates) {
            // Checked before the callbacks, which reach into the destination
            // BlockIndex and the tier accounting: there is no point asking
            // about a candidate the direction has already ruled out.
            if (!offload && !candidate->addressable) continue;
            if (!Admits(context, candidate->tiler_id)) continue;
            // The neighbour lists are derived from the edges, so this can
            // only miss if the two ever disagree. Asking anyway keeps the
            // target, the route it travels on and the copy requirements
            // below on one and the same declared edge.
            const TierEdge* edge =
                graph_->Edge(source->tiler_id, candidate->tiler_id);
            if (edge == nullptr) continue;

            PlacementDecision decision;
            decision.destination_tiler = candidate->tiler_id;
            decision.kind = KindFor(offload);
            decision.route.kind = decision.kind;
            decision.route.source_tiler = source->tiler_id;
            decision.route.destination_tiler = candidate->tiler_id;
            // Domains come from the two nodes, not from the direction: they
            // are what decides which copier can serve the pair at all.
            decision.route.source_domain = source->domain;
            decision.route.destination_domain = candidate->domain;
            // The edge is the only place these two are stated. Dropping them
            // here is how a declared staging-only link ends up being copied
            // directly: nothing downstream can tell which edge was chosen.
            decision.requires_staging = edge->requires_staging;
            decision.bandwidth_bytes_per_second =
                edge->bandwidth_bytes_per_second;
            return decision;
        }

        // Nowhere legal and useful to go. Not an error: the bottom tier of
        // every deployment reaches this, and so does a key the destination
        // already holds. The caller leaves the block where it is.
        return std::nullopt;
    }

   private:
    MovementKind KindFor(bool offload) const {
        // An offload always migrates -- its whole purpose is to free the
        // source. An onboard keeps the slow copy unless the deployment says
        // otherwise, because that copy is usually the durable one.
        if (offload) return MovementKind::kMigrate;
        return config_.onboard_keeps_source ? MovementKind::kReplicate
                                            : MovementKind::kMigrate;
    }

    bool Admits(const PlacementContext& context, const UUID& candidate) const {
        // A tier already at the limit is itself about to reclaim; feeding it
        // turns one tier's pressure into two tiers' pressure.
        if (context.usage_ratio &&
            context.usage_ratio(candidate) >= config_.max_destination_usage) {
            return false;
        }
        // Only the destination BlockIndex can answer this, and the caller has
        // just asked it. Copying a block onto a tier that already holds it
        // spends the bandwidth a real movement is waiting for.
        if (context.already_present && context.already_present(candidate)) {
            return false;
        }
        // An absent callback is the caller asserting there is no such
        // constraint, not an unknown: the policy has no way to find out and
        // must not invent a refresh loop to try.
        return true;
    }

    const TierPlacementPolicyConfig config_;
    const std::shared_ptr<const TierGraph> graph_;
};

}  // namespace

tl::expected<void, ErrorCode> ValidateTierPlacementPolicyConfig(
    const TierPlacementPolicyConfig& config) {
    // The type is checked here rather than only in the factory so a typo in a
    // configuration file fails while the configuration is being validated,
    // not later when something first tries to place a block.
    if (config.type != "nearest_neighbour") {
        LOG(ERROR) << "Unknown tier placement policy type '" << config.type
                   << "'";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!(config.max_destination_usage > 0.0) ||
        config.max_destination_usage > 1.0) {
        LOG(ERROR) << "tier_placement_policy.max_destination_usage must be "
                      "in (0, 1], got "
                   << config.max_destination_usage;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!config.onboard_keeps_source) {
        // Rejected rather than warned about. Reclamation is tier-local and
        // destroys an object that exists on only one tier (section 4.1), so an
        // onboard that deletes the slow copy moves the object from the one
        // tier where it was safe to the one tier that will drop it -- and it
        // does that to precisely the keys that are being read. Every onboarded
        // key would become a candidate for loss.
        //
        // The field is kept rather than removed so that this reasoning has a
        // place to live: someone who wants "promote and free the slow copy"
        // will look for the knob, and what they need to read is why it is off.
        LOG(ERROR) << "tier_placement_policy.onboard_keeps_source must be "
                      "true: dropping the slow copy on onboard leaves the "
                      "object only on a tier whose reclaim path destroys it";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<std::unique_ptr<TierPlacementPolicy>, ErrorCode>
CreateTierPlacementPolicy(const TierPlacementPolicyConfig& config,
                          std::shared_ptr<const TierGraph> graph) {
    auto valid = ValidateTierPlacementPolicyConfig(config);
    if (!valid) return tl::make_unexpected(valid.error());
    if (graph == nullptr) {
        // Shared and const: the policy is one of several readers of a graph
        // that is frozen at Build(), and it has no fallback topology to
        // derive from priority if the graph is missing.
        LOG(ERROR) << "CreateTierPlacementPolicy needs a tier graph";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return std::make_unique<NearestNeighbourPolicy>(config, std::move(graph));
}

}  // namespace mooncake::v2
