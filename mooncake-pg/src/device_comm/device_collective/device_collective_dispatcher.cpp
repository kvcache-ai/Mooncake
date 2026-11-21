#include "device_comm/device_collective/device_collective_dispatcher.h"

#include <unordered_set>
#include <utility>

#include <glog/logging.h>

#include "device_comm/device_collective/algorithms/one_shot/one_shot_algorithm.h"
#include "device_comm/device_collective/algorithms/pairwise/pairwise_algorithm.h"
#include "device_comm/device_collective/algorithms/ring/ring_algorithm.h"
#include "device_comm/device_collective/algorithms/star/star_algorithm.h"
#include "device_comm/device_collective/protocols/ll/ll_protocol.h"
#include "device_comm/device_collective/protocols/simple/simple_protocol.h"
#include "device_comm/device_transfer/transfer_service.h"

namespace mooncake {

template <typename Protocol, typename... Args>
Protocol* DeviceCollectiveDispatcher::tryLoadProtocol(Args&&... args) {
    auto result = Protocol::create(std::forward<Args>(args)...);
    if (!result.has_value()) {
        const auto& error = result.error();
        if (error.code == PGErrorCode::NotSupported) {
            LOG(INFO) << "[PG] " << Protocol::kProtocolKey
                      << " device collective protocol is unavailable: "
                      << error.message;
        } else {
            LOG(WARNING)
                << "[PG] " << Protocol::kProtocolKey
                << " device collective protocol initialization failed; "
                   "omitting protocol: "
                << error.message;
        }
        return nullptr;
    }
    auto* protocol = result.value().get();
    protocols_.push_back(std::move(result).value());
    return protocol;
}

template <typename Algorithm, typename... Args>
void DeviceCollectiveDispatcher::tryLoadAlgorithm(Args&&... args) {
    auto result = Algorithm::create(std::forward<Args>(args)...);
    if (!result.has_value()) {
        const auto& error = result.error();
        if (error.code == PGErrorCode::NotSupported) {
            LOG(INFO) << "[PG] " << Algorithm::kAlgorithmKey
                      << " device collective algorithm is unavailable: "
                      << error.message;
        } else {
            LOG(WARNING)
                << "[PG] " << Algorithm::kAlgorithmKey
                << " device collective algorithm initialization failed; "
                   "omitting algorithm: "
                << error.message;
        }
        return;
    }
    algorithms_.push_back(std::move(result).value());
}

PGResult<std::unique_ptr<DeviceCollectiveDispatcher>>
DeviceCollectiveDispatcher::create(DeviceTransferService& transfer_service,
                                   DeviceCollectiveWorkspace& workspace,
                                   int device_index, InGroupRank self_rank,
                                   uint32_t max_group_size,
                                   CollectiveRuntimeBindings collective) {
    auto dispatcher = std::unique_ptr<DeviceCollectiveDispatcher>(
        new DeviceCollectiveDispatcher());

    auto* simple = dispatcher->tryLoadProtocol<SimpleProtocol>(
        transfer_service, self_rank, max_group_size);
    auto* ll = dispatcher->tryLoadProtocol<LLProtocol>(
        transfer_service, self_rank, max_group_size);

    if (simple) {
        dispatcher->tryLoadAlgorithm<RingAlgorithm>(device_index, collective,
                                                    workspace, *simple);
        dispatcher->tryLoadAlgorithm<PairwiseAlgorithm>(
            device_index, collective, workspace, *simple);
        dispatcher->tryLoadAlgorithm<StarAlgorithm>(device_index, collective,
                                                    workspace, *simple);
    }
    if (ll) {
        dispatcher->tryLoadAlgorithm<OneShotAlgorithm>(
            transfer_service, max_group_size, collective, *ll);
    }
    return dispatcher;
}

DeviceCollectiveDispatcher::~DeviceCollectiveDispatcher() noexcept = default;

void DeviceCollectiveDispatcher::fillLocalEndpoint(
    DeviceGroupEndpoint& endpoint) const {
    for (const auto& protocol : protocols_) {
        endpoint.protocols.push_back(protocol->localEndpoint());
    }
    for (const auto& algorithm : algorithms_) {
        endpoint.supported_algorithms.push_back(algorithm->type());
        endpoint.algorithms.push_back(algorithm->localEndpoint());
    }
}

PGResult<void> DeviceCollectiveDispatcher::installGroupView(
    const ResolvedGroupView& view, ControlUpdateBuilder& builder) {
    std::unordered_set<const AlgorithmProvider*> selected_algorithms;
    std::unordered_set<const ProtocolProvider*> required_protocols;
    if (view.self_active_index >= 0) {
        for (const auto& choice : view.collective_algorithm_choices) {
            PG_TRY(auto algorithm, findAlgorithm(choice.algorithm));
            PG_VALIDATE_STATE(
                algorithm->supports(choice.operation),
                "configured algorithm is unavailable for this op");
            selected_algorithms.insert(algorithm);
            required_protocols.insert(&algorithm->protocol());
        }
    }

    // Shared protocol instances contribute one update regardless of how many
    // selected algorithms depend on them.
    for (const auto& protocol : protocols_) {
        if (required_protocols.contains(protocol.get())) {
            PG_TRY(protocol->updateState(view, builder));
        }
    }

    for (const auto& algorithm : algorithms_) {
        if (selected_algorithms.contains(algorithm.get())) {
            PG_TRY(algorithm->updatePlan(view, builder));
        } else {
            PG_TRY(algorithm->invalidatePlan(builder));
        }
    }
    algorithm_choices_ = view.collective_algorithm_choices;
    return {};
}

PGResult<void> DeviceCollectiveDispatcher::invalidatePlans(
    ControlUpdateBuilder& builder) {
    for (const auto& algorithm : algorithms_) {
        PG_TRY(algorithm->invalidatePlan(builder));
    }
    algorithm_choices_.clear();
    return {};
}

PGResult<const AlgorithmProvider*> DeviceCollectiveDispatcher::findAlgorithm(
    DeviceCollectiveAlgorithm type) const {
    for (const auto& algorithm : algorithms_) {
        if (algorithm->type() == type) return algorithm.get();
    }
    return makePGError(PGErrorCode::InvalidState,
                       "configured algorithm resources are unavailable");
}

PGResult<const AlgorithmProvider*> DeviceCollectiveDispatcher::selectAlgorithm(
    const DeviceCollectiveRequest& request) const {
    const auto [operation, bytes] = std::visit(
        [](const auto& value) {
            if constexpr (value.kOperation == OpType::Barrier) {
                // Barrier has no user payload.
                return std::pair{value.kOperation, uint64_t{0}};
            } else {
                return std::pair{value.kOperation,
                                 value.count * elementSize(value.datatype)};
            }
        },
        request);
    for (const auto& choice : algorithm_choices_) {
        if (choice.operation != operation || bytes > choice.max_bytes) continue;
        return findAlgorithm(choice.algorithm);
    }
    return makePGError(
        PGErrorCode::InvalidState,
        "group policy has no implementation for this collective");
}

}  // namespace mooncake
