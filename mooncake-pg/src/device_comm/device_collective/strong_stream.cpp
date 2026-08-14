#include "device_comm/device_collective/strong_stream.h"

#include <chrono>
#include <exception>
#include <list>
#include <new>
#include <mutex>
#include <thread>
#include <utility>

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
        return serial_event_.recordExternal(graph_order->stream);
    }
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
