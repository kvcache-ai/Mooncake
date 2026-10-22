// Component tests for the explicit tier topology (design doc section 7.1) and
// for the nearest-neighbour policy that walks it.
//
// The topology under test is the thing that replaced "derive the target from
// priority". Three properties matter more than the rest and most of this file
// exists for them: a movement is one hop along a declared edge, two tiers at
// the same priority are not connected at all, and an onboard only ever aims
// at a tier the request path can read. The old lookup got the second right by
// accident of a comparison operator; here it is a property of how the graph
// is built, so PriorityChainReproducesTheLegacySlowerThanAnswer re-derives
// the old answer and demands the new one match it exactly.
//
// The policy is private to tier_graph.cpp and is reached only through
// CreateTierPlacementPolicy, the same surface the consumers use. Tier usage
// and "does the destination already hold this block" are std::functions this
// file owns: in production they read the tier's accounting and the
// destination BlockIndex, and the point of passing them in is that the policy
// keeps no copy of either.

#include "p2p/client/v2/tier_graph.h"

#include <boost/functional/hash.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "p2p/client/v2/copier.h"
#include "p2p/client/v2/movement.h"
#include "p2p/client/v2/movement_tracker.h"
#include "types.h"

namespace mooncake::v2 {
namespace {

constexpr UUID kDram{0xD8A9, 0x0001};
constexpr UUID kCxl{0xC410, 0x0002};
constexpr UUID kSsd{0x55D0, 0x0003};
// A second SSD of the same class: several devices of one type are exactly the
// case a total order over priorities could not express.
constexpr UUID kSsdB{0x55D0, 0x0004};
constexpr UUID kDramB{0xD8A9, 0x0005};
// A fast cache in front of the SSD that cannot publish a TE address. A block
// promoted onto it is exactly as unreachable from the request path as one
// left on the SSD behind it.
constexpr UUID kNvme{0x0FCA, 0x0006};
constexpr UUID kUnknown{0xDEAD, 0xBEEF};

constexpr int32_t kDramPriority = 100;
constexpr int32_t kCxlPriority = 50;
constexpr int32_t kNvmePriority = 50;
constexpr int32_t kSsdPriority = 10;

TierNode MakeNode(const UUID& id, int32_t priority, CopyDomain domain,
                  bool addressable) {
    TierNode node;
    node.tiler_id = id;
    node.priority = priority;
    node.capacity = 1u << 20;
    node.addressable = addressable;
    node.domain = domain;
    return node;
}

TierNode DramNode(const UUID& id = kDram) {
    return MakeNode(id, kDramPriority, CopyDomain::kHostMemory,
                    /*addressable=*/true);
}

TierNode CxlNode() {
    return MakeNode(kCxl, kCxlPriority, CopyDomain::kHostMemory,
                    /*addressable=*/true);
}

TierNode SsdNode(const UUID& id = kSsd) {
    return MakeNode(id, kSsdPriority, CopyDomain::kFileOrBlock,
                    /*addressable=*/false);
}

TierNode NvmeNode() {
    return MakeNode(kNvme, kNvmePriority, CopyDomain::kFileOrBlock,
                    /*addressable=*/false);
}

TierEdge MakeEdge(const UUID& source, const UUID& destination, double cost,
                  size_t bandwidth = 0, bool requires_staging = false) {
    TierEdge edge;
    edge.source = source;
    edge.destination = destination;
    edge.cost = cost;
    edge.bandwidth_bytes_per_second = bandwidth;
    edge.requires_staging = requires_staging;
    return edge;
}

// gtest's message stream cannot find the UUID inserter (it lives in namespace
// mooncake, which is not an associated namespace of std::pair), so failure
// messages format the id themselves.
std::string Describe(const UUID& id) {
    return std::to_string(id.first) + "-" + std::to_string(id.second);
}

std::vector<UUID> IdsOf(const std::vector<const TierNode*>& nodes) {
    std::vector<UUID> ids;
    ids.reserve(nodes.size());
    for (const TierNode* node : nodes) ids.push_back(node->tiler_id);
    return ids;
}

std::vector<UUID> DestinationsOf(const std::vector<const TierEdge*>& edges) {
    std::vector<UUID> ids;
    ids.reserve(edges.size());
    for (const TierEdge* edge : edges) ids.push_back(edge->destination);
    return ids;
}

/**
 * @brief The pre-refactor answer, re-derived here.
 *
 * Same shape as placement_policy.cpp's SlowerThan and the identical copy in
 * evict_engine.cpp: stable descending sort, then the first entry whose
 * priority is strictly lower than the source's. Kept as a local reference
 * implementation so the compatibility test compares against the old rule
 * itself rather than against a table of expectations someone transcribed.
 */
const TierNode* LegacySlowerThan(const std::vector<TierNode>& tiers,
                                 const UUID& from) {
    std::vector<const TierNode*> by_priority;
    by_priority.reserve(tiers.size());
    for (const auto& tier : tiers) by_priority.push_back(&tier);
    std::stable_sort(by_priority.begin(), by_priority.end(),
                     [](const TierNode* lhs, const TierNode* rhs) {
                         return lhs->priority > rhs->priority;
                     });

    const TierNode* source = nullptr;
    for (const TierNode* tier : by_priority) {
        if (tier->tiler_id == from) {
            source = tier;
            break;
        }
    }
    if (source == nullptr) return nullptr;
    for (const TierNode* tier : by_priority) {
        if (tier->priority < source->priority) return tier;
    }
    return nullptr;
}

/**
 * @brief The pre-refactor onboard target, re-derived here.
 *
 * placement_policy.cpp's FastestAddressable: the first entry of the same
 * stable descending sort that can publish a TE address. The new rule
 * deliberately differs on *which* tier it names -- one hop up rather than
 * straight to the top -- so the compatibility test below compares the
 * property the two rules must share rather than the answer they need not.
 */
const TierNode* LegacyFastestAddressable(const std::vector<TierNode>& tiers) {
    std::vector<const TierNode*> by_priority;
    by_priority.reserve(tiers.size());
    for (const auto& tier : tiers) by_priority.push_back(&tier);
    std::stable_sort(by_priority.begin(), by_priority.end(),
                     [](const TierNode* lhs, const TierNode* rhs) {
                         return lhs->priority > rhs->priority;
                     });

    for (const TierNode* tier : by_priority) {
        if (tier->addressable) return tier;
    }
    return nullptr;
}

class TierGraphTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("TierGraphTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        // Every tier starts empty and holding nothing, so a test that says
        // "this target was skipped" has to set the reason itself.
        usage_.clear();
        present_.clear();
    }

    std::shared_ptr<const TierGraph> Chain(std::vector<TierNode> nodes) {
        auto graph = TierGraph::FromPriorityChain(std::move(nodes));
        CHECK(graph.has_value()) << "test setup: FromPriorityChain failed";
        return std::make_shared<const TierGraph>(std::move(graph.value()));
    }

    std::shared_ptr<const TierGraph> Explicit(std::vector<TierNode> nodes,
                                              std::vector<TierEdge> edges) {
        auto graph = TierGraph::Build(std::move(nodes), std::move(edges));
        CHECK(graph.has_value()) << "test setup: Build failed";
        return std::make_shared<const TierGraph>(std::move(graph.value()));
    }

    /** The three-level DRAM -> CXL -> SSD deployment used by most tests. */
    std::shared_ptr<const TierGraph> ThreeLevelChain() {
        return Chain({DramNode(), CxlNode(), SsdNode()});
    }

    std::unique_ptr<TierPlacementPolicy> MakePolicy(
        std::shared_ptr<const TierGraph> graph,
        const TierPlacementPolicyConfig& config = {}) {
        auto policy = CreateTierPlacementPolicy(config, std::move(graph));
        CHECK(policy.has_value())
            << "test setup: CreateTierPlacementPolicy failed";
        return std::move(policy.value());
    }

    PlacementContext Context(const UUID& source,
                             MovementDirection direction) const {
        PlacementContext context;
        context.key = "block/under/test";
        context.source_tiler = source;
        context.direction = direction;
        context.size_bytes = 4096;
        context.frequency = 3.0;
        context.usage_ratio = [this](const UUID& id) {
            auto it = usage_.find(id);
            return it == usage_.end() ? 0.0 : it->second;
        };
        context.already_present = [this](const UUID& id) {
            return present_.count(id) != 0;
        };
        return context;
    }

    std::unordered_map<UUID, double, boost::hash<UUID>> usage_;
    std::unordered_map<UUID, bool, boost::hash<UUID>> present_;
};

TEST_F(TierGraphTest, OffloadWalksTheChainOneHopAtATime) {
    auto graph = ThreeLevelChain();
    auto policy = MakePolicy(graph);

    // If this jumped straight to the SSD, the CXL tier a deployment paid for
    // would never hold anything and every DRAM eviction would pay disk cost.
    EXPECT_EQ(IdsOf(graph->SlowerNeighbours(kDram)), (std::vector<UUID>{kCxl}));
    auto first_hop =
        policy->Select(Context(kDram, MovementDirection::kOffload));
    ASSERT_TRUE(first_hop.has_value());
    EXPECT_EQ(first_hop->destination_tiler, kCxl);
    // An offload exists to free the source; keeping the DRAM copy would leave
    // the tier exactly as full as it was.
    EXPECT_EQ(first_hop->kind, MovementKind::kMigrate);
    EXPECT_EQ(first_hop->route.kind, MovementKind::kMigrate);
    EXPECT_EQ(first_hop->route.source_tiler, kDram);
    EXPECT_EQ(first_hop->route.destination_tiler, kCxl);
    // The route's domains are what pick the copy path and the queue; taking
    // them from the direction instead of the nodes would send an SSD write
    // down the host-memory path.
    EXPECT_EQ(first_hop->route.source_domain, CopyDomain::kHostMemory);
    EXPECT_EQ(first_hop->route.destination_domain, CopyDomain::kHostMemory);

    auto second_hop =
        policy->Select(Context(kCxl, MovementDirection::kOffload));
    ASSERT_TRUE(second_hop.has_value());
    EXPECT_EQ(second_hop->destination_tiler, kSsd);
    EXPECT_EQ(second_hop->route.destination_domain, CopyDomain::kFileOrBlock);

    // The bottom of the chain has nowhere to go, which is a normal steady
    // state and not an error the caller has to handle.
    EXPECT_FALSE(
        policy->Select(Context(kSsd, MovementDirection::kOffload)).has_value());
}

TEST_F(TierGraphTest, OnboardWalksTheChainOneHopAtATime) {
    auto graph = ThreeLevelChain();
    auto policy = MakePolicy(graph);

    EXPECT_EQ(IdsOf(graph->FasterNeighbours(kSsd)), (std::vector<UUID>{kCxl}));
    auto decision = policy->Select(Context(kSsd, MovementDirection::kOnboard));
    ASSERT_TRUE(decision.has_value());
    // Promoting straight to DRAM would evict from the tier under the most
    // pressure to serve a key that CXL can already serve fast.
    EXPECT_EQ(decision->destination_tiler, kCxl);
    EXPECT_EQ(decision->route.source_domain, CopyDomain::kFileOrBlock);
    EXPECT_EQ(decision->route.destination_domain, CopyDomain::kHostMemory);

    EXPECT_FALSE(policy->Select(Context(kDram, MovementDirection::kOnboard))
                     .has_value());
}

TEST_F(TierGraphTest, OnboardIntoATierTheRequestPathCannotReadIsNotADecision) {
    // DRAM publishes a TE address; the NVMe cache between it and the SSD does
    // not. Every request-path source is restricted to TE-addressable tiers,
    // so a block promoted onto that cache is still unservable.
    auto graph = Chain({DramNode(), NvmeNode(), SsdNode()});
    auto policy = MakePolicy(graph);

    // The topology query is unchanged: the cache really is one hop faster.
    EXPECT_EQ(IdsOf(graph->FasterNeighbours(kSsd)), (std::vector<UUID>{kNvme}));

    // Promoting into it spends the copy and leaves the block exactly as
    // unreachable as it was -- and then reports the destination as already
    // holding the key, which suppresses the onboard that would have helped.
    EXPECT_FALSE(
        policy->Select(Context(kSsd, MovementDirection::kOnboard)).has_value());

    // Offload is untouched: migration is the one source allowed to target a
    // tier the request path cannot read.
    auto offload = policy->Select(Context(kDram, MovementDirection::kOffload));
    ASSERT_TRUE(offload.has_value());
    EXPECT_EQ(offload->destination_tiler, kNvme);

    // And onboarding out of the unservable tier still works, because the tier
    // above it can serve what arrives.
    auto onboard = policy->Select(Context(kNvme, MovementDirection::kOnboard));
    ASSERT_TRUE(onboard.has_value());
    EXPECT_EQ(onboard->destination_tiler, kDram);
}

TEST_F(TierGraphTest, OnboardSkipsACheaperEdgeIntoAnUnservableTier) {
    // Both hops are declared and both are faster than the SSD; only DRAM can
    // publish an address for the block once it lands.
    auto graph = Explicit({DramNode(), NvmeNode(), SsdNode()},
                          {MakeEdge(kSsd, kNvme, /*cost=*/1.0),
                           MakeEdge(kSsd, kDram, /*cost=*/5.0)});
    auto policy = MakePolicy(graph);

    EXPECT_EQ(IdsOf(graph->FasterNeighbours(kSsd)),
              (std::vector<UUID>{kNvme, kDram}));

    auto decision = policy->Select(Context(kSsd, MovementDirection::kOnboard));
    ASSERT_TRUE(decision.has_value());
    // Cheapest-first is a tie-break between targets that would serve the
    // request, not a reason to spend the copy on one that serves nothing.
    EXPECT_EQ(decision->destination_tiler, kDram);
    EXPECT_EQ(decision->route.destination_domain, CopyDomain::kHostMemory);
}

TEST_F(TierGraphTest, EqualPriorityTiersAreNeverConnectedToEachOther) {
    auto graph = Chain({DramNode(kDram), DramNode(kDramB), SsdNode()});
    auto policy = MakePolicy(graph);

    // Peers, not a hierarchy. An edge here would let a full DRAM tier push
    // its blocks onto an equally full DRAM tier, which frees nothing and
    // arrives back as another offload one event later.
    EXPECT_EQ(graph->Edge(kDram, kDramB), nullptr);
    EXPECT_EQ(graph->Edge(kDramB, kDram), nullptr);
    EXPECT_EQ(IdsOf(graph->SlowerNeighbours(kDram)), (std::vector<UUID>{kSsd}));
    EXPECT_EQ(IdsOf(graph->FasterNeighbours(kDram)), (std::vector<UUID>{}));
    // Two levels, two nodes on the fast one: one down-edge and one up-edge
    // per (fast, slow) pair and nothing else.
    EXPECT_EQ(graph->EdgeCount(), 4u);

    auto decision = policy->Select(Context(kDram, MovementDirection::kOffload));
    ASSERT_TRUE(decision.has_value());
    EXPECT_EQ(decision->destination_tiler, kSsd);
}

TEST_F(TierGraphTest, AnUnreachableTargetIsNotADecision) {
    // A write-once archive: everything may be pushed down to it, nothing is
    // ever pulled back up. Direction is per edge for exactly this shape.
    auto graph = Explicit({DramNode(), SsdNode()},
                          {MakeEdge(kSsd, kDram, /*cost=*/1.0)});
    auto policy = MakePolicy(graph);

    // The SSD is slower and empty, so a priority-derived rule would offload
    // into it. There is no DRAM -> SSD edge, so this must not.
    EXPECT_EQ(graph->Edge(kDram, kSsd), nullptr);
    EXPECT_NE(graph->Edge(kSsd, kDram), nullptr);
    EXPECT_TRUE(graph->SlowerNeighbours(kDram).empty());
    EXPECT_FALSE(policy->Select(Context(kDram, MovementDirection::kOffload))
                     .has_value());

    // The declared direction still works.
    auto onboard = policy->Select(Context(kSsd, MovementDirection::kOnboard));
    ASSERT_TRUE(onboard.has_value());
    EXPECT_EQ(onboard->destination_tiler, kDram);

    // A tier that is not in the graph at all is a miss, not a crash: a stale
    // event can name a tiler that was never configured.
    EXPECT_FALSE(policy->Select(Context(kUnknown, MovementDirection::kOffload))
                     .has_value());
    EXPECT_EQ(graph->Node(kUnknown), nullptr);
    EXPECT_TRUE(graph->SlowerNeighbours(kUnknown).empty());
    EXPECT_TRUE(graph->FasterNeighbours(kUnknown).empty());
    EXPECT_TRUE(graph->OutgoingFrom(kUnknown).empty());
}

TEST_F(TierGraphTest, BuildRejectsAnEdgeThatNamesAnUnknownTier) {
    // Accepting it and dropping the edge would ship a deployment that starts
    // clean, reports a healthy topology and silently never offloads.
    auto graph = TierGraph::Build({DramNode(), SsdNode()},
                                  {MakeEdge(kDram, kCxl, /*cost=*/1.0)});
    ASSERT_FALSE(graph.has_value());
    EXPECT_EQ(graph.error(), ErrorCode::INVALID_PARAMS);

    auto reversed = TierGraph::Build({DramNode(), SsdNode()},
                                     {MakeEdge(kCxl, kDram, /*cost=*/1.0)});
    ASSERT_FALSE(reversed.has_value());
    EXPECT_EQ(reversed.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(TierGraphTest, BuildRejectsASelfEdge) {
    // Taken literally a self-migration copies a block onto its own tier and
    // then deletes the source, which is the one route that loses data.
    auto graph = TierGraph::Build({DramNode(), SsdNode()},
                                  {MakeEdge(kDram, kDram, /*cost=*/1.0)});
    ASSERT_FALSE(graph.has_value());
    EXPECT_EQ(graph.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(TierGraphTest, BuildRejectsADuplicateEdgeAndADuplicateNode) {
    // Two edges for one pair: Edge() can return only one of them, so the
    // other's cost would be used to choose the target and then ignored when
    // the route -- and therefore the queue -- was built from it.
    auto duplicate_edge = TierGraph::Build(
        {DramNode(), SsdNode()}, {MakeEdge(kDram, kSsd, /*cost=*/1.0),
                                  MakeEdge(kDram, kSsd, /*cost=*/2.0)});
    ASSERT_FALSE(duplicate_edge.has_value());
    EXPECT_EQ(duplicate_edge.error(), ErrorCode::INVALID_PARAMS);

    // Two tiers under one id: one of them could never be looked up, routed to
    // or diagnosed.
    auto duplicate_node =
        TierGraph::Build({DramNode(), DramNode(), SsdNode()}, {});
    ASSERT_FALSE(duplicate_node.has_value());
    EXPECT_EQ(duplicate_node.error(), ErrorCode::INVALID_PARAMS);

    // The opposite direction is a different edge and must stay legal.
    auto both_ways = TierGraph::Build({DramNode(), SsdNode()},
                                      {MakeEdge(kDram, kSsd, /*cost=*/1.0),
                                       MakeEdge(kSsd, kDram, /*cost=*/4.0)});
    ASSERT_TRUE(both_ways.has_value());
    EXPECT_EQ(both_ways->EdgeCount(), 2u);
    // Each direction answers with its own edge: one shared entry would price
    // the climb back up at the cost of the trip down.
    ASSERT_NE(both_ways->Edge(kDram, kSsd), nullptr);
    ASSERT_NE(both_ways->Edge(kSsd, kDram), nullptr);
    EXPECT_EQ(both_ways->Edge(kDram, kSsd)->cost, 1.0);
    EXPECT_EQ(both_ways->Edge(kSsd, kDram)->cost, 4.0);
}

TEST_F(TierGraphTest, BuildRejectsAnEdgeBetweenTwoTiersOfEqualPriority) {
    // Storing it would put a configured rebalance path in EdgeCount() and in
    // OutgoingFrom() that neither SlowerNeighbours nor FasterNeighbours can
    // ever return, so no direction a policy can ask for would select it: a
    // deployment that declares one gets a graph that starts, looks healthy
    // and quietly never moves anything along that edge.
    auto peers =
        TierGraph::Build({DramNode(kDram), DramNode(kDramB), SsdNode()},
                         {MakeEdge(kDram, kDramB, /*cost=*/1.0)});
    ASSERT_FALSE(peers.has_value());
    EXPECT_EQ(peers.error(), ErrorCode::INVALID_PARAMS);

    // What is refused is the equal priority, not the shared device class:
    // two SSDs fed from one DRAM tier is the several-devices-of-one-type
    // deployment the explicit graph exists to express.
    auto same_class = TierGraph::Build({DramNode(), SsdNode(), SsdNode(kSsdB)},
                                       {MakeEdge(kDram, kSsd, /*cost=*/1.0),
                                        MakeEdge(kDram, kSsdB, /*cost=*/1.0)});
    ASSERT_TRUE(same_class.has_value());
    EXPECT_EQ(same_class->EdgeCount(), 2u);

    // The peers themselves are still a legal deployment; it is only the edge
    // between them that nothing could have used.
    auto unlinked = TierGraph::Build({DramNode(kDram), DramNode(kDramB)}, {});
    ASSERT_TRUE(unlinked.has_value());
    EXPECT_EQ(unlinked->EdgeCount(), 0u);
}

TEST_F(TierGraphTest, AGraphWithNoTiersIsLegalAndDecidesNothing) {
    // Legal, because "how many tiers must exist" belongs to whoever reads the
    // configuration, not to the topology. The policy must still answer rather
    // than dereference its way through an empty graph.
    auto empty = TierGraph::Build({}, {});
    ASSERT_TRUE(empty.has_value());
    EXPECT_TRUE(empty->Empty());
    EXPECT_EQ(empty->EdgeCount(), 0u);
    EXPECT_EQ(empty->Node(kDram), nullptr);
    EXPECT_TRUE(empty->OutgoingFrom(kDram).empty());

    auto graph = std::make_shared<const TierGraph>(std::move(empty.value()));
    auto policy = MakePolicy(graph);
    EXPECT_FALSE(policy->Select(Context(kDram, MovementDirection::kOffload))
                     .has_value());
    EXPECT_FALSE(policy->Select(Context(kDram, MovementDirection::kOnboard))
                     .has_value());
}

TEST_F(TierGraphTest, ASingleTierWithNoEdgesIsALegalGraph) {
    // The most common small deployment. Rejecting it would make every caller
    // special-case "one tier" before it could build a graph at all.
    auto built = TierGraph::Build({DramNode()}, {});
    ASSERT_TRUE(built.has_value());
    EXPECT_FALSE(built->Empty());
    EXPECT_EQ(built->Nodes().size(), 1u);
    EXPECT_EQ(built->EdgeCount(), 0u);
    ASSERT_NE(built->Node(kDram), nullptr);
    EXPECT_EQ(built->Node(kDram)->priority, kDramPriority);
    // The graph hands back the tier as configured: capacity feeds the
    // caller's usage ratio, addressable decides whether an onboard may aim
    // here, and the domain decides which copier can serve the pair.
    EXPECT_EQ(built->Node(kDram)->capacity, size_t{1} << 20);
    EXPECT_TRUE(built->Node(kDram)->addressable);
    EXPECT_EQ(built->Node(kDram)->domain, CopyDomain::kHostMemory);
    EXPECT_TRUE(built->OutgoingFrom(kDram).empty());

    auto chained = TierGraph::FromPriorityChain({DramNode()});
    ASSERT_TRUE(chained.has_value());
    EXPECT_EQ(chained->EdgeCount(), 0u);

    auto graph = std::make_shared<const TierGraph>(std::move(chained.value()));
    auto policy = MakePolicy(graph);
    // Nothing to decide, in either direction, and neither is a failure.
    EXPECT_FALSE(policy->Select(Context(kDram, MovementDirection::kOffload))
                     .has_value());
    EXPECT_FALSE(policy->Select(Context(kDram, MovementDirection::kOnboard))
                     .has_value());
}

TEST_F(TierGraphTest, OutgoingEdgesAreCheapestFirst) {
    // Declared out of order and with a tie, so the ordering below cannot come
    // out right by accident of the input order alone.
    auto graph = Explicit({DramNode(), CxlNode(), SsdNode(), SsdNode(kSsdB)},
                          {MakeEdge(kDram, kSsd, /*cost=*/5.0),
                           MakeEdge(kDram, kCxl, /*cost=*/1.0),
                           MakeEdge(kDram, kSsdB, /*cost=*/5.0)});

    // Cheapest first decides which target the policy takes. The two tied
    // edges are here only to show the cost sort does not reorder them by
    // something else; with three edges an unstable sort would keep this order
    // too, so the tie-break itself is pinned by
    // EqualCostEdgesKeepTheirConfiguredOrder below.
    EXPECT_EQ(DestinationsOf(graph->OutgoingFrom(kDram)),
              (std::vector<UUID>{kCxl, kSsd, kSsdB}));
    EXPECT_EQ(IdsOf(graph->SlowerNeighbours(kDram)),
              (std::vector<UUID>{kCxl, kSsd, kSsdB}));

    auto policy = MakePolicy(graph);
    auto decision = policy->Select(Context(kDram, MovementDirection::kOffload));
    ASSERT_TRUE(decision.has_value());
    EXPECT_EQ(decision->destination_tiler, kCxl);
}

TEST_F(TierGraphTest, EqualCostEdgesKeepTheirConfiguredOrder) {
    // Twenty identical devices rather than two: below libstdc++'s
    // insertion-sort threshold an unstable sort is accidentally stable, so a
    // small fan-out cannot tell "ties keep the configured order" from "the
    // sort happened not to move them".
    constexpr size_t kFanOut = 20;
    std::vector<TierNode> nodes{DramNode()};
    std::vector<TierEdge> edges;
    std::vector<UUID> declared;
    for (size_t i = 0; i < kFanOut; ++i) {
        const UUID device{0x55D0, 0x1000 + i};
        nodes.push_back(MakeNode(device, kSsdPriority, CopyDomain::kFileOrBlock,
                                 /*addressable=*/false));
        edges.push_back(MakeEdge(kDram, device, /*cost=*/1.0));
        declared.push_back(device);
    }
    auto graph = Explicit(std::move(nodes), std::move(edges));

    // The order a deployment listed is the order it gets: identical devices
    // are filled in a predictable sequence, and the same configuration picks
    // the same target on every run and on every node.
    EXPECT_EQ(DestinationsOf(graph->OutgoingFrom(kDram)), declared);
    EXPECT_EQ(IdsOf(graph->SlowerNeighbours(kDram)), declared);

    auto policy = MakePolicy(graph);
    auto decision = policy->Select(Context(kDram, MovementDirection::kOffload));
    ASSERT_TRUE(decision.has_value());
    EXPECT_EQ(decision->destination_tiler, declared.front())
        << "offload landed on " << Describe(decision->destination_tiler);
}

TEST_F(TierGraphTest, PriorityChainKeepsTheConfiguredOrderWithinALevel) {
    // Same size argument as above, applied to the chain's own sort: the
    // compatibility claim in PriorityChainReproducesTheLegacySlowerThanAnswer
    // rests on equal priorities keeping their configured order, and three
    // tiers cannot show that.
    constexpr size_t kFastTiers = 20;
    std::vector<TierNode> nodes;
    std::vector<UUID> declared;
    for (size_t i = 0; i < kFastTiers; ++i) {
        const UUID device{0xD8A9, 0x2000 + i};
        nodes.push_back(MakeNode(device, kDramPriority, CopyDomain::kHostMemory,
                                 /*addressable=*/true));
        declared.push_back(device);
    }
    nodes.push_back(SsdNode());
    auto graph = Chain(nodes);

    // The chain links level to level, so every fast tier is one hop up from
    // the SSD; which of them the SSD onboards to is decided by the level's
    // own order, and that order is the configured one.
    EXPECT_EQ(IdsOf(graph->FasterNeighbours(kSsd)), declared);
    EXPECT_EQ(graph->EdgeCount(), 2 * kFastTiers);

    auto policy = MakePolicy(graph);
    auto decision = policy->Select(Context(kSsd, MovementDirection::kOnboard));
    ASSERT_TRUE(decision.has_value());
    EXPECT_EQ(decision->destination_tiler, declared.front())
        << "onboard landed on " << Describe(decision->destination_tiler);
}

TEST_F(TierGraphTest, TheDecisionCarriesTheChosenEdgesCopyRequirements) {
    constexpr size_t kDirectBandwidth = 20ull << 30;
    constexpr size_t kStagedBandwidth = 2ull << 30;
    // Two declared links out of DRAM: a direct one to CXL and a staging-only
    // one to the SSD. Which edge was taken is decided here and nowhere else,
    // so whatever the decision does not carry is lost.
    auto graph =
        Explicit({DramNode(), CxlNode(), SsdNode()},
                 {MakeEdge(kDram, kCxl, /*cost=*/1.0, kDirectBandwidth),
                  MakeEdge(kDram, kSsd, /*cost=*/5.0, kStagedBandwidth,
                           /*requires_staging=*/true)});
    auto policy = MakePolicy(graph);

    auto direct = policy->Select(Context(kDram, MovementDirection::kOffload));
    ASSERT_TRUE(direct.has_value());
    ASSERT_EQ(direct->destination_tiler, kCxl);
    EXPECT_FALSE(direct->requires_staging);
    EXPECT_EQ(direct->bandwidth_bytes_per_second, kDirectBandwidth);

    // Push the cheap edge out of reach. The facts must follow the edge the
    // policy actually took, not the first one leaving the source.
    usage_[kCxl] = 0.99;
    auto staged = policy->Select(Context(kDram, MovementDirection::kOffload));
    ASSERT_TRUE(staged.has_value());
    ASSERT_EQ(staged->destination_tiler, kSsd);
    // A deployment that declares a staging-only link and gets a direct copy
    // attempted on it fails one queue deep in the copier, with nothing in the
    // decision to say why the link was configured that way.
    EXPECT_TRUE(staged->requires_staging);
    EXPECT_EQ(staged->bandwidth_bytes_per_second, kStagedBandwidth);
}

TEST_F(TierGraphTest, SelectIsSafeFromEveryConsumerThreadAtOnce) {
    // The policy takes no lock, and the reason it may is that the graph is
    // frozen at Build and Select keeps no per-key state. Nothing else here
    // exercises that: a race would surface as an occasional wrong target
    // under load, which no single-threaded test can see.
    auto graph = ThreeLevelChain();
    auto policy = MakePolicy(graph);

    constexpr int kThreads = 8;
    constexpr int kRounds = 500;
    std::atomic<int> wrong{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            for (int round = 0; round < kRounds; ++round) {
                auto offload =
                    policy->Select(Context(kDram, MovementDirection::kOffload));
                auto onboard =
                    policy->Select(Context(kSsd, MovementDirection::kOnboard));
                if (!offload.has_value() ||
                    offload->destination_tiler != kCxl ||
                    !onboard.has_value() ||
                    onboard->destination_tiler != kCxl) {
                    wrong.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(wrong.load(), 0);
}

TEST_F(TierGraphTest, AFullDestinationIsSkippedForTheNextCandidate) {
    auto graph = Explicit({DramNode(), CxlNode(), SsdNode(), SsdNode(kSsdB)},
                          {MakeEdge(kDram, kCxl, /*cost=*/1.0),
                           MakeEdge(kDram, kSsd, /*cost=*/5.0),
                           MakeEdge(kDram, kSsdB, /*cost=*/9.0)});
    auto policy = MakePolicy(graph);

    // Below the limit: the cheapest edge still wins.
    usage_[kCxl] = 0.90;
    auto below = policy->Select(Context(kDram, MovementDirection::kOffload));
    ASSERT_TRUE(below.has_value());
    EXPECT_EQ(below->destination_tiler, kCxl);

    // At the limit exactly. A tier at its cap is already reclaiming, so
    // sending it more just converts one tier's pressure into two.
    usage_[kCxl] = 0.95;
    auto at_limit = policy->Select(Context(kDram, MovementDirection::kOffload));
    ASSERT_TRUE(at_limit.has_value());
    EXPECT_EQ(at_limit->destination_tiler, kSsd);

    // Over the limit on the next one too: the search keeps going rather than
    // giving up at the first full tier.
    usage_[kCxl] = 0.99;
    usage_[kSsd] = 0.97;
    auto skipped = policy->Select(Context(kDram, MovementDirection::kOffload));
    ASSERT_TRUE(skipped.has_value());
    EXPECT_EQ(skipped->destination_tiler, kSsdB);

    // Everything full: no target, and still not an error.
    usage_[kSsdB] = 1.0;
    EXPECT_FALSE(policy->Select(Context(kDram, MovementDirection::kOffload))
                     .has_value());
}

TEST_F(TierGraphTest, ADestinationThatAlreadyHoldsTheBlockIsSuppressed) {
    auto fan_out = Explicit({DramNode(), CxlNode(), SsdNode()},
                            {MakeEdge(kDram, kCxl, /*cost=*/1.0),
                             MakeEdge(kDram, kSsd, /*cost=*/5.0)});
    auto policy = MakePolicy(fan_out);

    // Copying a block onto a tier that already has it spends the bandwidth a
    // real movement is queued behind and, for a migrate, would delete the
    // source in favour of a copy that was already there.
    present_[kCxl] = true;
    auto decision = policy->Select(Context(kDram, MovementDirection::kOffload));
    ASSERT_TRUE(decision.has_value());
    EXPECT_EQ(decision->destination_tiler, kSsd);

    // The only candidate holding it means there is nothing useful to do.
    auto chain_policy = MakePolicy(ThreeLevelChain());
    EXPECT_FALSE(
        chain_policy->Select(Context(kDram, MovementDirection::kOffload))
            .has_value());
}

TEST_F(TierGraphTest, OnboardAlwaysKeepsTheSourceCopy) {
    auto graph = ThreeLevelChain();

    // The slow copy is normally the durable one, and there is no reason to
    // drop it just because the key turned hot.
    auto keeping = MakePolicy(graph);
    auto replicated =
        keeping->Select(Context(kSsd, MovementDirection::kOnboard));
    ASSERT_TRUE(replicated.has_value());
    EXPECT_EQ(replicated->kind, MovementKind::kReplicate);
    EXPECT_EQ(replicated->route.kind, MovementKind::kReplicate);

    // "Promote and free the slow copy" is no longer a deployment choice. With
    // a tier-local reclaim path, deleting the slow copy on onboard leaves the
    // object only on the tier that will destroy it -- and it does so to
    // exactly the keys being read. The configuration is refused outright, so
    // the kMigrate-on-onboard branch is unreachable by construction.
    TierPlacementPolicyConfig config;
    config.onboard_keeps_source = false;
    EXPECT_EQ(ValidateTierPlacementPolicyConfig(config).error(),
              ErrorCode::INVALID_PARAMS);
    auto refused = CreateTierPlacementPolicy(config, graph);
    EXPECT_FALSE(refused.has_value());

    // An offload still migrates: moving a cold block down is the whole point,
    // and the destination keeps it, so nothing is lost.
    auto offload = keeping->Select(Context(kDram, MovementDirection::kOffload));
    ASSERT_TRUE(offload.has_value());
    EXPECT_EQ(offload->kind, MovementKind::kMigrate);
}

TEST_F(TierGraphTest, PriorityChainReproducesTheLegacySlowerThanAnswer) {
    // The headline case: the two-tier fast/slow set every existing
    // deployment runs. If this disagrees, the refactor changed where blocks
    // land rather than only how the target is expressed.
    const std::vector<std::vector<TierNode>> deployments = {
        {DramNode(), SsdNode()},
        // Reversed input: the old rule sorted first, and so must this one.
        {SsdNode(), DramNode()},
        // Peers only: the old rule answered "nowhere", never "sideways".
        {DramNode(kDram), DramNode(kDramB)},
        {DramNode(kDram), DramNode(kDramB), SsdNode()},
        {DramNode(), CxlNode(), SsdNode()},
        // Two devices on the bottom level: the old rule took the first in
        // configured order.
        {DramNode(), SsdNode(), SsdNode(kSsdB)},
        // A middle tier the request path cannot read: the old rule offloaded
        // into it and never onboarded into it.
        {DramNode(), NvmeNode(), SsdNode()},
    };

    for (const auto& nodes : deployments) {
        auto graph = Chain(nodes);
        for (const auto& node : nodes) {
            const TierNode* legacy = LegacySlowerThan(nodes, node.tiler_id);
            const auto neighbours = graph->SlowerNeighbours(node.tiler_id);
            if (legacy == nullptr) {
                EXPECT_TRUE(neighbours.empty())
                    << "tier " << Describe(node.tiler_id)
                    << " gained an offload target the old rule refused";
                continue;
            }
            ASSERT_FALSE(neighbours.empty())
                << "tier " << Describe(node.tiler_id)
                << " lost its offload target";
            EXPECT_EQ(neighbours.front()->tiler_id, legacy->tiler_id)
                << "tier " << Describe(node.tiler_id)
                << " offloads somewhere new";
        }

        // The other half of the old pair of rules. FastestAddressable jumped
        // to the top of the addressable tiers and this one takes a single
        // hop, so the two names differ on purpose -- but a promotion into a
        // tier the request path cannot read was not something the old rule
        // could produce, and it must not become something this one can.
        auto policy = MakePolicy(graph);
        const TierNode* servable = LegacyFastestAddressable(nodes);
        for (const auto& node : nodes) {
            auto onboard = policy->Select(
                Context(node.tiler_id, MovementDirection::kOnboard));
            if (!onboard.has_value()) continue;
            const TierNode* target = graph->Node(onboard->destination_tiler);
            ASSERT_NE(target, nullptr);
            EXPECT_TRUE(target->addressable)
                << "tier " << Describe(node.tiler_id)
                << " onboards into a tier the request path cannot read";
            EXPECT_NE(servable, nullptr)
                << "tier " << Describe(node.tiler_id)
                << " onboards although no tier is addressable at all";
        }
    }
}

TEST_F(TierGraphTest, TheFactoryRejectsAnUnusableConfiguration) {
    auto graph = ThreeLevelChain();

    TierPlacementPolicyConfig unknown_type;
    unknown_type.type = "shortest_path";
    // Silently falling back to the only implementation would let a typo
    // choose the placement strategy for a whole cluster.
    EXPECT_FALSE(ValidateTierPlacementPolicyConfig(unknown_type).has_value());
    auto rejected = CreateTierPlacementPolicy(unknown_type, graph);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::INVALID_PARAMS);

    TierPlacementPolicyConfig out_of_range;
    out_of_range.max_destination_usage = 1.5;
    // A ratio above 1 can never skip anything, so a "protect the destination"
    // setting would be silently off.
    EXPECT_FALSE(ValidateTierPlacementPolicyConfig(out_of_range).has_value());
    out_of_range.max_destination_usage = 0.0;
    // And zero would skip every tier, stopping offload altogether.
    EXPECT_FALSE(ValidateTierPlacementPolicyConfig(out_of_range).has_value());

    TierPlacementPolicyConfig usable;
    EXPECT_TRUE(ValidateTierPlacementPolicyConfig(usable).has_value());
    usable.max_destination_usage = 1.0;
    EXPECT_TRUE(ValidateTierPlacementPolicyConfig(usable).has_value());

    // Without a graph there is no topology to walk, and deriving one from
    // priority is the behaviour this component exists to remove.
    auto no_graph =
        CreateTierPlacementPolicy(TierPlacementPolicyConfig{}, nullptr);
    ASSERT_FALSE(no_graph.has_value());
    EXPECT_EQ(no_graph.error(), ErrorCode::INVALID_PARAMS);
}

}  // namespace
}  // namespace mooncake::v2
