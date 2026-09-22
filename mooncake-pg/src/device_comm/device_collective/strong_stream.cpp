#include "device_comm/device_collective/strong_stream.h"

#include <chrono>
#include <exception>
#include <list>
#include <new>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>

namespace mooncake {
namespace {

bool sameCapture(const GpuCaptureInfo& left,
                 const GpuCaptureInfo& right) noexcept {
    if (left.active != right.active) return false;
    if (!left.active) return true;
    return left.graph_id == right.graph_id && left.graph == right.graph &&
           left.origin == right.origin;
}

}  // namespace

StrongStream::Lease::~Lease() noexcept { releaseAndLogError(); }

StrongStream::Lease::Lease(Lease&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)),
      capture_(other.capture_),
      stream_(std::move(other.stream_)) {}

StrongStream::Lease& StrongStream::Lease::operator=(Lease&& other) noexcept {
    if (this != &other) {
        releaseAndLogError();
        owner_ = std::exchange(other.owner_, nullptr);
        capture_ = other.capture_;
        stream_ = std::move(other.stream_);
    }
    return *this;
}

PGResult<void> StrongStream::Lease::release() {
    if (!owner_) return {};
    auto* owner = std::exchange(owner_, nullptr);
    return owner->release(capture_);
}

void StrongStream::Lease::releaseAndLogError() noexcept {
    if (!owner_) return;
    try {
        auto result = release();
        if (!result.has_value()) {
            LOG(ERROR) << "Failed to release StrongStream lease: "
                       << result.error().message;
        }
    } catch (const std::exception& error) {
        LOG(ERROR) << "Failed to release StrongStream lease: " << error.what();
    } catch (...) {
        LOG(ERROR) << "Failed to release StrongStream lease";
    }
}

PGResult<std::unique_ptr<StrongStream>> StrongStream::create(int device) {
    PG_TRY(auto eager_order_stream, GpuStream::createNonBlocking(device));
    PG_TRY(auto serial_event, GpuEvent::create(device));

    auto strong_stream =
        std::unique_ptr<StrongStream>(new (std::nothrow) StrongStream(
            device, std::move(eager_order_stream), std::move(serial_event)));
    if (!strong_stream) {
        return makePGError(PGErrorCode::SystemError,
                           "failed to allocate StrongStream");
    }
    return strong_stream;
}

StrongStream::StrongStream(int device, GpuStream eager_order_stream,
                           GpuEvent serial_event) noexcept
    : device_index_(device),
      eager_order_stream_(std::move(eager_order_stream)),
      serial_event_(std::move(serial_event)) {}

StrongStream::~StrongStream() noexcept {
    if (pending_release_.has_value()) {
        LOG(ERROR) << "StrongStream destroyed with an unmatched acquire";
    }
}

PGResult<StrongStream::Lease> StrongStream::acquire(
    const GpuCaptureInfo& capture) {
    std::lock_guard<std::mutex> lock(mutex_);
    PG_VALIDATE_STATE(!pending_release_.has_value(),
                      "StrongStream already has an unmatched acquire");

    cudaStream_t order_stream = nullptr;

    if (!capture.active) {
        // Eager calls share this physical order stream, so CUDA stream order
        // directly linearizes them. Once a Graph has used this StrongStream,
        // the latest tail may instead have been published by a Graph replay;
        // import that dynamic completion before extending the eager order.
        if (ever_captured_) {
            PG_TRY(eager_order_stream_.waitEvent(serial_event_));
        }
        order_stream = eager_order_stream_.get();
    } else {
        // A CUDA stream can participate in only one active capture. Keep one
        // construction lane per active Graph capture; all calls in that same
        // capture reuse its CUDA-maintained dependency frontier. Collective
        // kernels themselves remain on their user streams.
        PG_ASSERT(capture.graph,
                  "active CUDA Graph capture has no graph handle");

        // acquire() may be called more than once during the same capture. Find
        // its existing ordering state, and discard ended captures.
        GraphOrder* graph_order = nullptr;
        for (auto current = graph_orders_.begin();
             current != graph_orders_.end();) {
            PG_TRY(auto status, current->stream.captureStatus());

            if (status != cudaStreamCaptureStatusActive) {
                current = graph_orders_.erase(current);
                continue;
            }
            if (current->graph_id == capture.graph_id) {
                // A previous collective in this same capture already created
                // and seeded the GraphOrder with its one external wait. Reuse
                // its current captured dependencies; the caller's next
                // ordinary entry handoff will transfer those dependencies to
                // the new user stream.
                graph_order = &*current;
                break;
            }
            ++current;
        }

        if (!graph_order) {
            PG_TRY(auto graph_order_stream,
                   GpuStream::createNonBlocking(device_index_));

            // Join this private order stream to the user's active capture
            // without inheriting work already captured on the user stream.
            // The external wait below supplies its first real dependency.
            PG_TRY(joinCaptureWithoutDependencies(capture, graph_order_stream));

            // Initialize serial_event_ from prior eager work before the first
            // Graph starts using it.
            if (!ever_captured_) {
                PG_TRY(serial_event_.record(eager_order_stream_));
            }
            // This wait does NOT order two calls in this same capture. It is
            // added only when the GraphOrder is created. Later calls find this
            // GraphOrder above and inherit its current static frontier, which
            // the caller advances with ordinary handoff events around each
            // collective kernel.
            PG_TRY(graph_order_stream.waitExternalEvent(serial_event_));

            graph_orders_.emplace_front(capture.graph_id,
                                        std::move(graph_order_stream));
            graph_order = &graph_orders_.front();
            ever_captured_ = true;
        }
        order_stream = graph_order->stream.get();
    }

    pending_release_.emplace(PendingRelease{.capture = capture});
    return Lease(*this, capture,
                 GpuStream::borrow(order_stream, device_index_));
}

PGResult<void> StrongStream::release(const GpuCaptureInfo& capture) {
    std::lock_guard<std::mutex> lock(mutex_);
    PG_ASSERT(pending_release_.has_value(),
              "StrongStream release has no matching acquire");
    PG_ASSERT(sameCapture(pending_release_->capture, capture),
              "StrongStream release uses a different CUDA capture");

    GraphOrder* graph_order = nullptr;
    if (capture.active) {
        for (auto& current : graph_orders_) {
            if (current.graph_id == capture.graph_id) {
                graph_order = &current;
                break;
            }
        }
        PG_ASSERT(graph_order,
                  "StrongStream Graph order is no longer available");
    }

    // Clear the protocol state before the fallible CUDA publication below. A
    // failed release is an operation error, not a permanently unmatched
    // acquire that poisons every later call with a misleading error.
    pending_release_.reset();

    if (capture.active) {
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
        cudaStreamCaptureStatus status;
        const cudaGraphNode_t* dependencies = nullptr;
        size_t count = 0;
        // capture.origin is this call's user stream.
#if CUDART_VERSION >= 13000
        PG_TRY_CUDA(cudaStreamGetCaptureInfo(capture.origin, &status, nullptr,
                                             nullptr, &dependencies, nullptr,
                                             &count));
#else
        PG_TRY_CUDA(cudaStreamGetCaptureInfo_v2(
            capture.origin, &status, nullptr, nullptr, &dependencies, &count));
#endif
        PG_VALIDATE_STATE(status == cudaStreamCaptureStatusActive,
                          "StrongStream release capture is not active");

        // Copy the CUDA-owned frontier before updating capture dependencies.
        std::vector<cudaGraphNode_t> frontier;
        if (count) frontier.assign(dependencies, dependencies + count);

        // CUDA maintains a capture frontier for each stream: the nodes its next
        // captured node must depend on. `frontier` above is a copy of the USER
        // stream's frontier, queried from capture.origin.
        //
        // In enqueueAllReduce(), handoff_event_.record(user_stream) captures
        // that user frontier. order_stream.waitEvent(handoff_event_) then adds
        // those nodes to the ORDER stream's capture frontier. Its old nodes can
        // remain there: for a single kernel, the order frontier can contain
        // {old nodes, kernel}, while the user frontier is just {kernel}.
        //
        // SET below replaces graph_order->stream's frontier with `frontier`,
        // i.e. {kernel} in that example. It does not modify the user frontier
        // or delete existing graph nodes/edges. The entry handoff already made
        // the kernel depend on the old nodes, so those remain indirect
        // dependencies of subsequent work even after removal from the order
        // frontier.
        //
        // This direct SET supplies the return dependency in capture, making
        // the caller's return record/wait redundant. But eager still needs it.
#if CUDART_VERSION >= 13000
        PG_TRY_CUDA(cudaStreamUpdateCaptureDependencies(
            graph_order->stream.get(), frontier.data(), nullptr, count,
            cudaStreamSetCaptureDependencies));
#else
        PG_TRY_CUDA(cudaStreamUpdateCaptureDependencies(
            graph_order->stream.get(), frontier.data(), count,
            cudaStreamSetCaptureDependencies));
#endif
        // tail is a graph node that records the CUDA event serial_event_. Its
        // input edges come from the user frontier above. On each replay, it
        // records completion after those predecessors finish, for subsequent
        // graphs/eager calls to wait on. Adding it directly to the graph leaves
        // every stream's frontier unchanged: the next collective inherits the
        // current kernel, not tail. Keep one record for this GraphOrder region.
        auto& tail = graph_order->completion_node;
        if (!tail) {
            PG_TRY_CUDA(cudaGraphAddEventRecordNode(&tail, capture.graph,
                                                    frontier.data(), count,
                                                    serial_event_.get()));
            return {};
        }

        // Read tail's old predecessors, independently of stream frontiers.
        // The new predecessors are in frontier, queried from the user stream;
        // serial_event_ only names the event to record. Capture forbids node
        // removal, so keep tail and replace its incoming edges.
        size_t previous_count = 0;
        const auto get_dependencies = [&](cudaGraphNode_t* nodes,
                                          size_t* node_count) {
#if CUDART_VERSION >= 13000
            return cudaGraphNodeGetDependencies(tail, nodes, nullptr,
                                                node_count);
#else
            return cudaGraphNodeGetDependencies(tail, nodes, node_count);
#endif
        };
        PG_TRY_CUDA(get_dependencies(nullptr, &previous_count));
        std::vector<cudaGraphNode_t> previous(previous_count);
        PG_TRY_CUDA(get_dependencies(previous.data(), &previous_count));
        if (previous == frontier) return {};

        // For two collective kernels:
        //   first release:          collective1 -> tail
        //   second kernel captured: collective1 -> {collective2, tail}
        //   second release:         collective1 -> collective2 -> tail
        // These edits happen during capture, before the graph executes. No
        // collective depends on tail, so moving it cannot introduce a cycle.
        for (auto node : previous) {
#if CUDART_VERSION >= 13000
            PG_TRY_CUDA(cudaGraphRemoveDependencies(capture.graph, &node, &tail,
                                                    nullptr, 1));
#else
            PG_TRY_CUDA(
                cudaGraphRemoveDependencies(capture.graph, &node, &tail, 1));
#endif
        }
        for (auto node : frontier) {
#if CUDART_VERSION >= 13000
            PG_TRY_CUDA(cudaGraphAddDependencies(capture.graph, &node, &tail,
                                                 nullptr, 1));
#else
            PG_TRY_CUDA(
                cudaGraphAddDependencies(capture.graph, &node, &tail, 1));
#endif
        }
        return {};
    }
    // Eager's return handoff makes the order stream wait for the kernel.
    // Also publish its completion once this domain has been used by graphs.
    if (ever_captured_) return serial_event_.record(eager_order_stream_);
    return {};
}

PGResult<void> StrongStream::waitUntilIdle() {
    std::lock_guard<std::mutex> lock(mutex_);
    PG_ASSERT(!pending_release_.has_value(),
              "StrongStream cannot wait before release");

    if (ever_captured_) {
        PG_TRY(eager_order_stream_.waitEvent(serial_event_));
    }
    PG_TRY(auto idle, GpuEvent::create(device_index_));
    PG_TRY(idle.record(eager_order_stream_));

    while (true) {
        PG_TRY(auto complete, idle.query());
        if (complete) return {};
        std::this_thread::sleep_for(kIdlePollInterval);
    }
}

}  // namespace mooncake
