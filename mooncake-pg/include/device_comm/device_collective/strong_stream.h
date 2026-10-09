#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_STRONG_STREAM_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_STRONG_STREAM_H

#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

#include "gpu_runtime.h"

namespace mooncake {

// StrongStream gives one ordering domain a logical collective order while each
// kernel remains on the user-provided stream. Device collectives use one domain
// per CUDA device, shared by all communicators on that device. acquire()
// returns a scoped lease for an order stream; the caller transfers its current
// tail to the user stream before the kernel, then transfers kernel completion
// back before releasing the lease:
//
//      order stream                         user stream
//           |                                   |
//      record handoff --------------------> wait handoff
//           |                             collective kernel
//      wait handoff <--------------------- record handoff
//           |                                   |
//
// The two records above are successive uses of the same handoff event. In
// eager execution they are real CUDA stream operations. During capture CUDA
// converts them into dependencies inside the Graph.
//
// The four cases below build from this common handoff, starting with the
// simplest case.
//
// 1. Eager call followed by eager call
//
//    All eager calls use the same physical eager_order_stream_. Suppose the
//    host enqueues A and then B, possibly on different user streams:
//
//      eager order stream         user stream A          user stream B
//              |                        |                      |
//         record H1 --------------->  wait H1                  |
//              |                     kernel A                  |
//         wait H2 <----------------- record H2                 |
//              |                        |                      |
//         record H3 ---------------------------------------> wait H3
//              |                        |                   kernel B
//         wait H4 <---------------------------------------- record H4
//              |                        |                      |
//
//    H1/H2 are A's entry/return records; H3/H4 are B's. They may use different
//    event objects when A and B belong to different communicators. Correctness
//    comes from the common order stream: record H3 is after wait H2, so B
//    cannot start before A completes. serial_event_ is unnecessary when
//    execution stays entirely in eager mode.
//
// 2. Two calls captured into the same Graph
//
//    Each active capture gets one private GraphOrder stream. Calls belonging
//    to the same capture reuse it, even if their kernels use different user
//    streams. CUDA remembers the Graph nodes that the next operation on each
//    captured stream must follow; that current dependency set is called the
//    stream's capture frontier below.
//
//      Graph G
//
//        GraphOrder              user stream A          user stream B
//              |                       |                      |
//      waitExternal(serial_event)      |                      |  entry boundary
//              |                       |                      |
//           record H1 ------------>  wait H1                  |
//              |                    kernel A                  |
//            wait H2 <------------- record H2                 |
//              |                       |                      |
//      recordExternal(serial_event)    |                      |  post-A record
//              |                       |                      |
//          record H3 -------------------------------------> wait H3
//              |                       |                   kernel B
//            wait H4 <------------------------------------ record H4
//              |                       |                      |
//      recordExternal(serial_event)    |                      |  G1 final tail
//              |                       |                      |
//
//    The ordinary H1-H4 operations become static Graph edges. A's return
//    handoff advances GraphOrder's capture frontier; B's entry handoff copies
//    that frontier to user stream B. The resulting Graph therefore contains
//    the static dependency A -> B. The external-event operations are needed
//    for cases 3 and 4; they are not what establishes A -> B.
//
//    Capture only constructs these nodes and edges. It does not wait for or
//    execute either kernel.
//
// 3. Eager execution mixed with a Graph replay
//
//    serial_event_ is the stable bridge between the physical eager order
//    stream and a captured GraphOrder. To order two submissions, the earlier
//    collective records serial_event_ after it completes, and the later
//    collective waits for that record before it starts.
//
//    Eager A submitted before Graph G:
//
//      eager order:  ... -> A -> record(serial_event)
//                                      |
//                                      | replay-time dependency
//                                      v
//      Graph G:       waitExternal(serial_event) -> B
//
//    Graph G submitted before eager C:
//
//      Graph G:       ... -> B -> recordExternal(serial_event)
//                                      |
//                                      | runtime dependency
//                                      v
//      eager order:        wait(serial_event) -> C -> ...
//
//    Before the first Graph uses serial_event_, StrongStream records the
//    current eager tail into it. After Graph capture has been used, every eager
//    release records the new eager tail, and every eager acquire first waits
//    for the latest tail that may have come from a Graph replay. The Graph uses
//    external event nodes because its wait and record must execute on every
//    replay; the eager side uses ordinary CUDA event operations.
//
// 4. Replays of two different Graphs
//
//    Different captures have different GraphOrder streams and no static edge
//    between them. Each Graph nevertheless has one statically ordered
//    collective region, as established in case 2:
//
//      G1: waitExternal(serial_event)
//             -> A -> recordExternal(serial_event) after A
//             -> B -> recordExternal(serial_event) after B
//
//      G2: waitExternal(serial_event)
//             -> C -> recordExternal(serial_event) after C
//             -> D -> recordExternal(serial_event) after D
//
//    The event handle and Graph nodes remain fixed after capture, but every
//    replay executes those event nodes again. Each record creates a new
//    conceptual generation, and each wait binds to the latest generation
//    submitted before it. Later records do not retarget an already submitted
//    wait.
//
//    Suppose the host submits one replay of G1 and then one replay of G2:
//
//      launch(G1):
//        G1 entry wait  -> previous serial_event_ generation
//        post-A record  -> generation n
//        post-B record  -> generation n+1  (latest G1 generation)
//
//      launch(G2):
//        G2 entry wait  -> generation n+1
//        post-C record  -> generation n+2
//        post-D record  -> generation n+3  (latest G2 generation)
//
//    G2's entry wait binds to post-B before G2's own later records replace the
//    event's current state; a wait is not changed by later records. At runtime:
//
//      GPU completes A -> generation n becomes ready
//                         G2 still waits for generation n+1
//      GPU completes B -> generation n+1 becomes ready
//                         G2 may now run C, then D
//
//    A later replay executes the same static nodes, but its entry wait binds to
//    the latest generation from previously submitted work and its records
//    create fresh generations. This is the same replay-time mechanism used in
//    case 3.
//
//    The collective order is therefore A -> B -> C -> D, never A -> C -> B ->
//    D. release() adds a record after every captured collective because it
//    cannot know whether another call will be appended to that Graph. If
//    another call is appended, its later record becomes the Graph's published
//    tail for subsequently submitted work.
//
//    Reversing the host launch order reverses the two complete collective
//    regions. Racing Graph launches from different host threads do not define
//    an order and are outside the API contract.
//
// Only the collective regions above are serialized. Graph nodes unrelated to
// their GraphOrder dependency chains may still overlap across Graphs.
//
// This design requires a well-defined host submission order for eager enqueues
// and Graph launches in the ordering domain. Submissions may come from
// different host threads when the application externally serializes them;
// racing submissions are outside the contract. Internal locking protects object
// state but does not establish these orders.
class StrongStream {
   public:
    class Lease {
       public:
        ~Lease() noexcept;

        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        Lease(Lease&& other) noexcept;
        Lease& operator=(Lease&& other) noexcept;

        const GpuStream& stream() const noexcept { return stream_; }
        PGResult<void> release();

       private:
        friend class StrongStream;

        Lease(StrongStream& owner, GpuCaptureInfo capture, GpuStream stream)
            : owner_(&owner), capture_(capture), stream_(std::move(stream)) {}

        void releaseAndLogError() noexcept;

        StrongStream* owner_ = nullptr;
        GpuCaptureInfo capture_;
        GpuStream stream_;
    };

    [[nodiscard]] static PGResult<std::unique_ptr<StrongStream>> create(
        int device);

    ~StrongStream() noexcept;

    StrongStream(const StrongStream&) = delete;
    StrongStream& operator=(const StrongStream&) = delete;
    StrongStream(StrongStream&&) = delete;
    StrongStream& operator=(StrongStream&&) = delete;

    PGResult<Lease> acquire(const GpuCaptureInfo& capture);
    PGResult<void> waitUntilIdle();

   private:
    static constexpr auto kIdlePollInterval = std::chrono::microseconds(100);

    StrongStream(int device, GpuStream eager_order_stream,
                 GpuEvent serial_event) noexcept;

    PGResult<void> release(const GpuCaptureInfo& capture);

    struct GraphOrder {
        GraphOrder(uint64_t graph_id, GpuStream stream)
            : graph_id(graph_id), stream(std::move(stream)) {}

        uint64_t graph_id;
        GpuStream stream;
    };

    struct PendingRelease {
        GpuCaptureInfo capture;
    };

    int device_index_;
    // Carries domain order between uncaptured user-stream launches.
    GpuStream eager_order_stream_;
    // Dynamically publishes completion between eager execution and Graph
    // replays. This event is a cross-execution bridge, not a FIFO or the source
    // of static ordering between calls in one captured Graph.
    GpuEvent serial_event_;

    std::mutex mutex_;
    std::list<GraphOrder> graph_orders_;
    std::optional<PendingRelease> pending_release_;
    bool ever_captured_ = false;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_STRONG_STREAM_H
