// Component tests for the event pipeline (sections 3.8 and 5.10).
//
// Everything below is about what a queue does when it is full. The pipeline
// exists so that a fact can never stall the request path that produced it, so
// each case fixes one of the three permitted outcomes -- fold, lose, or apply
// on the producer's thread -- and none of them is "wait".

#include "p2p/client/v2/event_center.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "p2p/client/v2/block.h"
#include "p2p/client/v2/block_registry.h"
#include "p2p/client/v2/movement.h"
#include "types.h"

namespace mooncake::v2 {
namespace {

constexpr UUID kTilerA{0xA1, 0x0A};
constexpr UUID kTilerB{0xB2, 0x0B};

// Upper bound on a wait that must not deadlock. It is a liveness guard, not a
// timing assertion: a pipeline that behaves satisfies it immediately, and only
// one that parks a producer ever reaches it.
constexpr std::chrono::seconds kLivenessBound{5};

BlockEvent MakeEvent(EventType type, std::string key, const UUID& tiler) {
    BlockEvent event;
    event.type = type;
    event.key = std::move(key);
    event.tiler_id = tiler;
    event.size_bytes = 4096;
    return event;
}

/**
 * @class RecordingPolicy
 * @brief A PlacementPolicy that only remembers, and answers Consume with
 *        whatever the test scripted. Scripting the commands is what lets the
 *        dedup assertions name exact requests instead of depending on what a
 *        real policy happens to propose for a given fact.
 */
class RecordingConsumer final : public EventConsumer {
   public:
    /**
     * The two delivery modes, under their old names. A queued delivery is
     * what the shard worker does; an inline one is the producer applying a
     * lifecycle event it could not queue. Keeping the names means every
     * assertion written against the single-consumer center still says what it
     * said before.
     */
    enum class CallKind : uint8_t { kConsume, kApplyInline };

    RecordingConsumer() = default;
    RecordingConsumer(std::string name, uint32_t mask)
        : name_(std::move(name)), mask_(mask) {}

    Subscription SubscriptionInfo() const override {
        Subscription subscription;
        subscription.name = name_;
        subscription.event_mask = mask_;
        return subscription;
    }

    /** The Nth Consume returns the Nth scripted list, then nothing. */
    void ScriptNext(std::vector<MovementRequest> commands) {
        std::lock_guard<std::mutex> lock(mu_);
        scripted_.push_back(std::move(commands));
    }

    ConsumeResult Consume(const BlockEvent& event, DeliveryMode mode) override {
        std::vector<MovementRequest> commands;
        {
            std::lock_guard<std::mutex> lock(mu_);
            calls_.push_back(Call{mode == DeliveryMode::kInline
                                      ? CallKind::kApplyInline
                                      : CallKind::kConsume,
                                  event});
            if (mode == DeliveryMode::kQueued && !scripted_.empty()) {
                commands = std::move(scripted_.front());
                scripted_.pop_front();
            }
        }
        cv_.notify_all();
        return commands.empty() ? ConsumeResult::kApplied
                                : ConsumeResult::kCommandEnqueued;
    }

    std::vector<BlockEvent> EventsOf(CallKind kind) const {
        std::lock_guard<std::mutex> lock(mu_);
        std::vector<BlockEvent> events;
        for (const auto& call : calls_) {
            if (call.kind == kind) events.push_back(call.event);
        }
        return events;
    }

    size_t CountOf(CallKind kind) const { return EventsOf(kind).size(); }

    size_t TotalCalls() const {
        std::lock_guard<std::mutex> lock(mu_);
        return calls_.size();
    }

    /** False if the calls never arrived, which means a worker is stuck. */
    bool WaitForTotalCalls(size_t expected) const {
        std::unique_lock<std::mutex> lock(mu_);
        return cv_.wait_for(lock, kLivenessBound,
                            [&] { return calls_.size() >= expected; });
    }

   private:
    struct Call {
        CallKind kind;
        BlockEvent event;
    };

    std::string name_ = "recording";
    uint32_t mask_ = SubscriptionMask(
        {EventType::kAccess, EventType::kCommit, EventType::kDelete,
         EventType::kAllocationFailure, EventType::kWatermark});
    mutable std::mutex mu_;
    mutable std::condition_variable cv_;
    std::vector<Call> calls_;
    std::deque<std::vector<MovementRequest>> scripted_;
};

/** A value with something to fold, so a fold can be told from a drop. */
struct Sample {
    std::string key;
    int count = 1;
};

BoundedQueue<Sample>::Coalescer SumSamplesWithTheSameKey() {
    return [](Sample& existing, const Sample& incoming) {
        if (existing.key != incoming.key) return false;
        existing.count += incoming.count;
        return true;
    };
}

}  // namespace

// ---------------------------------------------------------------------------
// BoundedQueue
// ---------------------------------------------------------------------------

TEST(BoundedQueueTest, PushAndPopRoundTripInOrder) {
    BoundedQueue<int> queue(4);
    EXPECT_EQ(queue.Capacity(), 4U);
    EXPECT_EQ(queue.Size(), 0U);

    EXPECT_EQ(queue.Push(1, QueuePushMode::kBestEffort),
              QueuePushResult::kEnqueued);
    EXPECT_EQ(queue.Push(2, QueuePushMode::kReliable),
              QueuePushResult::kEnqueued);
    EXPECT_EQ(queue.Size(), 2U);

    EXPECT_EQ(queue.Pop(), std::optional<int>(1));
    EXPECT_EQ(queue.Pop(), std::optional<int>(2));
    EXPECT_EQ(queue.Size(), 0U);
    EXPECT_EQ(queue.DroppedCount(), 0U);
    EXPECT_EQ(queue.CoalescedCount(), 0U);
}

// The answer to a full queue is the same for both modes on purpose: a
// reliable push must get a deterministic value back so its producer can apply
// the event itself. Blocking here would drive a stalled policy worker into
// every write path.
TEST(BoundedQueueTest, AFullQueueReportsDroppedForEveryPushMode) {
    BoundedQueue<int> queue(2);
    ASSERT_EQ(queue.Push(1, QueuePushMode::kBestEffort),
              QueuePushResult::kEnqueued);
    ASSERT_EQ(queue.Push(2, QueuePushMode::kBestEffort),
              QueuePushResult::kEnqueued);

    EXPECT_EQ(queue.Push(3, QueuePushMode::kBestEffort),
              QueuePushResult::kDropped);
    EXPECT_EQ(queue.Push(4, QueuePushMode::kReliable),
              QueuePushResult::kDropped);

    EXPECT_EQ(queue.DroppedCount(), 2U);
    // The queued values are the ones that arrived first; a full queue rejects
    // newcomers rather than evicting what a consumer is about to read.
    EXPECT_EQ(queue.Size(), 2U);
    EXPECT_EQ(queue.Pop(), std::optional<int>(1));
    EXPECT_EQ(queue.Pop(), std::optional<int>(2));
}

// Shutdown has to release a consumer that is already parked, or Stop() would
// join a thread that never returns.
TEST(BoundedQueueTest, CloseWakesABlockedPopWithNoValue) {
    BoundedQueue<int> queue(2);
    auto popped =
        std::async(std::launch::async, [&queue] { return queue.Pop(); });

    queue.Close();
    ASSERT_EQ(popped.wait_for(kLivenessBound), std::future_status::ready)
        << "Close() left a consumer parked in Pop()";
    EXPECT_FALSE(popped.get().has_value());

    // Still empty and still closed: every later Pop answers at once.
    EXPECT_FALSE(queue.Pop().has_value());
}

TEST(BoundedQueueTest, PushOnAClosedQueueIsRejectedRatherThanQueued) {
    BoundedQueue<int> queue(4);
    ASSERT_EQ(queue.Push(1, QueuePushMode::kBestEffort),
              QueuePushResult::kEnqueued);
    queue.Close();

    EXPECT_EQ(queue.Push(2, QueuePushMode::kBestEffort),
              QueuePushResult::kClosed);
    EXPECT_EQ(queue.Push(3, QueuePushMode::kReliable),
              QueuePushResult::kClosed);
    // kClosed is not kDropped: nothing was lost, so the drop counter must not
    // move and a draining shutdown still sees what was already queued.
    EXPECT_EQ(queue.DroppedCount(), 0U);
    EXPECT_EQ(queue.Size(), 1U);
    EXPECT_EQ(queue.TryPop(), std::optional<int>(1));
}

TEST(BoundedQueueTest, TryPopNeverWaitsForAProducer) {
    BoundedQueue<int> queue(4);
    EXPECT_FALSE(queue.TryPop().has_value());

    ASSERT_EQ(queue.Push(7, QueuePushMode::kBestEffort),
              QueuePushResult::kEnqueued);
    EXPECT_EQ(queue.TryPop(), std::optional<int>(7));
    EXPECT_FALSE(queue.TryPop().has_value());
}

TEST(BoundedQueueTest, ClearDiscardsEverythingQueuedAndLeavesTheQueueUsable) {
    BoundedQueue<int> queue(4);
    ASSERT_EQ(queue.Push(1, QueuePushMode::kBestEffort),
              QueuePushResult::kEnqueued);
    ASSERT_EQ(queue.Push(2, QueuePushMode::kReliable),
              QueuePushResult::kEnqueued);

    queue.Clear();
    EXPECT_EQ(queue.Size(), 0U);
    EXPECT_FALSE(queue.TryPop().has_value());
    // Clear() drops content, not the queue itself: a cancelling shutdown
    // clears before it closes, and the two steps must stay independent.
    EXPECT_EQ(queue.Push(3, QueuePushMode::kBestEffort),
              QueuePushResult::kEnqueued);
    EXPECT_EQ(queue.TryPop(), std::optional<int>(3));
}

TEST(BoundedQueueTest, TheCoalescerFoldsAMatchingEntryWhenTheQueueIsFull) {
    BoundedQueue<Sample> queue(1, SumSamplesWithTheSameKey());
    ASSERT_EQ(queue.Push(Sample{"a", 1}, QueuePushMode::kBestEffort),
              QueuePushResult::kEnqueued);

    EXPECT_EQ(queue.Push(Sample{"a", 1}, QueuePushMode::kBestEffort),
              QueuePushResult::kCoalesced);
    EXPECT_EQ(queue.Push(Sample{"a", 1}, QueuePushMode::kBestEffort),
              QueuePushResult::kCoalesced);
    // A fold that never reaches the queued value would be a silent drop
    // wearing a different name, so the merged payload is what is checked.
    EXPECT_EQ(queue.CoalescedCount(), 2U);
    EXPECT_EQ(queue.DroppedCount(), 0U);
    ASSERT_EQ(queue.Size(), 1U);
    auto merged = queue.TryPop();
    ASSERT_TRUE(merged.has_value());
    EXPECT_EQ(merged->count, 3);

    // Nothing to fold into means the value is simply lost.
    ASSERT_EQ(queue.Push(Sample{"a", 1}, QueuePushMode::kBestEffort),
              QueuePushResult::kEnqueued);
    EXPECT_EQ(queue.Push(Sample{"b", 1}, QueuePushMode::kBestEffort),
              QueuePushResult::kDropped);
    EXPECT_EQ(queue.DroppedCount(), 1U);
    EXPECT_EQ(queue.CoalescedCount(), 2U);
}

// This is what separates BoundedQueue from bounded_dedup_queue: coalescing is
// a spill valve for a full queue, not a deduplicator on every push. Folding
// while there is room would silently discard facts the consumer could have
// processed individually.
TEST(BoundedQueueTest, TheCoalescerIsOnlyConsultedOnAFullQueue) {
    BoundedQueue<Sample> queue(2, SumSamplesWithTheSameKey());
    ASSERT_EQ(queue.Push(Sample{"a", 1}, QueuePushMode::kBestEffort),
              QueuePushResult::kEnqueued);
    EXPECT_EQ(queue.Push(Sample{"a", 1}, QueuePushMode::kBestEffort),
              QueuePushResult::kEnqueued);

    EXPECT_EQ(queue.CoalescedCount(), 0U);
    EXPECT_EQ(queue.Size(), 2U);
    EXPECT_EQ(queue.TryPop()->count, 1);
    EXPECT_EQ(queue.TryPop()->count, 1);
}

// A capacity of zero would make every push fail forever, which for the
// movement queue means no command is ever executed again.
TEST(BoundedQueueTest, ZeroCapacityIsClampedToOne) {
    BoundedQueue<int> queue(0);
    EXPECT_EQ(queue.Capacity(), 1U);
    EXPECT_EQ(queue.Push(1, QueuePushMode::kBestEffort),
              QueuePushResult::kEnqueued);
    EXPECT_EQ(queue.Push(2, QueuePushMode::kBestEffort),
              QueuePushResult::kDropped);
}

// ---------------------------------------------------------------------------
// EventCenter
// ---------------------------------------------------------------------------

class EventCenterTest : public ::testing::Test {
   protected:
    using CallKind = RecordingConsumer::CallKind;

    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("EventCenterTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        registry_ = BlockRegistry(BlockRegistryConfig{});
        policy_ = std::make_unique<RecordingConsumer>();
    }

    void TearDown() override {
        if (center_) center_->Stop(EventCenterStopMode::kCancel);
        center_.reset();
    }

    // A single shard on purpose: "the queue is full" is only a statement a
    // test can make when every key lands in the same queue.
    void MakeCenter(size_t event_capacity, size_t /*unused*/ = 0) {
        EventCenterConfig config;
        config.shard_count = 1;
        config.event_queue_capacity = event_capacity;
        center_ = std::make_shared<EventCenter>(config);
        CHECK(center_->RegisterConsumer(policy_.get()).has_value());
    }

    QueuePushResult Publish(EventType type, std::string key,
                            const UUID& tiler) {
        return center_->Publish(MakeEvent(type, std::move(key), tiler));
    }

    /** A live registration, kept strong so its weak handles stay upgradable. */
    WeakBlockRegistrationHandle Registration(std::string_view key) {
        auto handle = registry_.Register(key);
        CHECK(handle.has_value()) << "test setup: Register failed";
        registrations_.push_back(handle.value());
        return handle->Downgrade();
    }

    static MovementRequest MakeRequest(
        MovementKind kind, const WeakBlockRegistrationHandle& registration,
        const BlockId& source_block, const UUID& source, const UUID& dest) {
        MovementRequest request;
        request.kind = kind;
        request.key = "movement_key";
        request.source_tiler = source;
        request.destination_tiler = dest;
        request.source_block_id = source_block;
        request.registration = registration;
        return request;
    }

    BlockRegistry registry_;
    std::vector<BlockRegistrationHandle> registrations_;
    std::unique_ptr<RecordingConsumer> policy_;
    std::shared_ptr<EventCenter> center_;
};

// Access is a sampling signal. Folding a burst is what stops it from pushing
// the lifecycle events queued behind it out of a full queue.
TEST_F(EventCenterTest, RepeatedAccessOnOneKeyAndTierFoldsIntoTheQueuedFact) {
    MakeCenter(/*event_capacity=*/1, /*movement_capacity=*/8);
    ASSERT_EQ(Publish(EventType::kAccess, "hot", kTilerA),
              QueuePushResult::kEnqueued);

    EXPECT_EQ(Publish(EventType::kAccess, "hot", kTilerA),
              QueuePushResult::kCoalesced);
    EXPECT_EQ(Publish(EventType::kAccess, "hot", kTilerA),
              QueuePushResult::kCoalesced);

    const EventCenterMetrics metrics = center_->Metrics();
    EXPECT_EQ(metrics.events_coalesced, 2U);
    EXPECT_EQ(metrics.events_enqueued, 1U);
    EXPECT_EQ(metrics.event_queue_depth, 1U);

    center_->DrainForTest();
    EXPECT_EQ(policy_->CountOf(CallKind::kConsume), 1U);
}

// Coalescing is only sound when the two facts really are the same fact: a
// different key is a different object, and the same key on another tier is a
// different replica, so folding either would corrupt the policy's view.
TEST_F(EventCenterTest, OnlyAccessOnTheSameKeyAndTierIsAllowedToFold) {
    MakeCenter(/*event_capacity=*/1, /*movement_capacity=*/8);
    ASSERT_EQ(Publish(EventType::kAccess, "hot", kTilerA),
              QueuePushResult::kEnqueued);

    EXPECT_EQ(Publish(EventType::kAccess, "cold", kTilerA),
              QueuePushResult::kDropped);
    EXPECT_EQ(Publish(EventType::kAccess, "hot", kTilerB),
              QueuePushResult::kDropped);
    EXPECT_EQ(Publish(EventType::kAllocationFailure, "hot", kTilerA),
              QueuePushResult::kDropped);

    const EventCenterMetrics metrics = center_->Metrics();
    EXPECT_EQ(metrics.events_coalesced, 0U);
    EXPECT_EQ(metrics.event_queue_depth, 1U);
}

TEST_F(EventCenterTest, AnAccessEventThatCannotBeQueuedIsSimplyLost) {
    MakeCenter(/*event_capacity=*/1, /*movement_capacity=*/8);
    ASSERT_EQ(Publish(EventType::kAccess, "hot", kTilerA),
              QueuePushResult::kEnqueued);

    EXPECT_EQ(Publish(EventType::kAccess, "cold", kTilerA),
              QueuePushResult::kDropped);

    // Best effort means exactly that: no synchronous policy work on the
    // reader's thread, and no inline accounting either.
    EXPECT_EQ(policy_->CountOf(CallKind::kApplyInline), 0U);
    EXPECT_EQ(center_->Metrics().lifecycle_event_inline_applied, 0U);
    EXPECT_EQ(center_->Metrics().events_enqueued, 1U);
}

// The central rule of section 3.8: Commit and Delete may not be silently
// lost, and may not block their publisher either. A full queue therefore
// leaves exactly one option, applying the fact on the producer's thread.
TEST_F(EventCenterTest, ALifecycleEventThatCannotBeQueuedIsAppliedInline) {
    MakeCenter(/*event_capacity=*/1, /*movement_capacity=*/8);
    ASSERT_EQ(Publish(EventType::kAccess, "filler", kTilerA),
              QueuePushResult::kEnqueued);

    // Published from another thread with a liveness bound, because the
    // failure being pinned is a producer parking on a full queue and only an
    // off-thread publish can tell "hung" from "finished".
    auto published = std::async(std::launch::async, [this] {
        std::vector<QueuePushResult> results;
        results.push_back(Publish(EventType::kCommit, "commit_key", kTilerA));
        results.push_back(Publish(EventType::kDelete, "delete_key", kTilerA));
        return results;
    });
    ASSERT_EQ(published.wait_for(kLivenessBound), std::future_status::ready)
        << "Publish blocked on a full event queue";

    const std::vector<QueuePushResult> results = published.get();
    ASSERT_EQ(results.size(), 2U);
    EXPECT_EQ(results[0], QueuePushResult::kDropped);
    EXPECT_EQ(results[1], QueuePushResult::kDropped);

    const std::vector<BlockEvent> applied =
        policy_->EventsOf(CallKind::kApplyInline);
    ASSERT_EQ(applied.size(), 2U);
    EXPECT_EQ(applied[0].type, EventType::kCommit);
    EXPECT_EQ(applied[0].key, "commit_key");
    EXPECT_EQ(applied[1].type, EventType::kDelete);
    EXPECT_EQ(applied[1].key, "delete_key");

    const EventCenterMetrics metrics = center_->Metrics();
    EXPECT_EQ(metrics.lifecycle_event_inline_applied, 2U);
    // Applied, not queued: the queue is exactly as full as it was, and the
    // asynchronous path never ran for these two facts.
    EXPECT_EQ(metrics.events_enqueued, 1U);
    EXPECT_EQ(metrics.event_queue_depth, 1U);
    EXPECT_EQ(policy_->CountOf(CallKind::kConsume), 0U);
}

TEST_F(EventCenterTest, PublishAfterStopIsRejectedAndNotAppliedInline) {
    MakeCenter(/*event_capacity=*/8, /*movement_capacity=*/8);
    center_->Stop(EventCenterStopMode::kDrain);

    EXPECT_EQ(Publish(EventType::kCommit, "late_commit", kTilerA),
              QueuePushResult::kClosed);
    EXPECT_EQ(Publish(EventType::kDelete, "late_delete", kTilerA),
              QueuePushResult::kClosed);
    EXPECT_EQ(Publish(EventType::kAccess, "late_access", kTilerA),
              QueuePushResult::kClosed);

    EXPECT_EQ(policy_->TotalCalls(), 0U);
    EXPECT_EQ(center_->Metrics().lifecycle_event_inline_applied, 0U);
}

// A Commit published just before Stop still has to reach the policy, so a
// draining stop consumes the backlog before it closes the queues.
TEST_F(EventCenterTest, ADrainingStopAppliesWhatWasStillQueued) {
    MakeCenter(/*event_capacity=*/8, /*movement_capacity=*/8);
    ASSERT_EQ(Publish(EventType::kCommit, "drain_first", kTilerA),
              QueuePushResult::kEnqueued);
    ASSERT_EQ(Publish(EventType::kAccess, "drain_second", kTilerA),
              QueuePushResult::kEnqueued);

    center_->Stop(EventCenterStopMode::kDrain);

    const std::vector<BlockEvent> consumed =
        policy_->EventsOf(CallKind::kConsume);
    ASSERT_EQ(consumed.size(), 2U);
    EXPECT_EQ(consumed[0].key, "drain_first");
    EXPECT_EQ(consumed[1].key, "drain_second");
    EXPECT_TRUE(center_->IsStopped());
}

// The other half of the choice: a cancelling stop is for a teardown that can
// no longer afford to call into the policy at all, so the backlog is dropped.
TEST_F(EventCenterTest, ACancellingStopDiscardsWhatWasStillQueued) {
    MakeCenter(/*event_capacity=*/8, /*movement_capacity=*/8);
    ASSERT_EQ(Publish(EventType::kCommit, "cancel_first", kTilerA),
              QueuePushResult::kEnqueued);
    ASSERT_EQ(Publish(EventType::kAccess, "cancel_second", kTilerA),
              QueuePushResult::kEnqueued);

    center_->Stop(EventCenterStopMode::kCancel);

    EXPECT_EQ(policy_->TotalCalls(), 0U);
    EXPECT_TRUE(center_->IsStopped());
    EXPECT_EQ(center_->Metrics().event_queue_depth, 0U);
}

// The destructor calls Stop as well, so a second Stop must be a no-op rather
// than a second join of already-joined threads.
TEST_F(EventCenterTest, StopIsIdempotentAndLeavesNoWorkerRunning) {
    MakeCenter(/*event_capacity=*/8, /*movement_capacity=*/8);
    const WeakBlockRegistrationHandle registration = Registration("worker");
    policy_->ScriptNext(
        {MakeRequest(MovementKind::kReplicate, registration,
                     BlockId{kTilerA, 3, 1}, kTilerA, kTilerB)});
    center_->Start();

    ASSERT_EQ(Publish(EventType::kCommit, "worker", kTilerA),
              QueuePushResult::kEnqueued);
    // The shard worker, not the test thread, is what turns the fact into a
    // command here.
    ASSERT_TRUE(policy_->WaitForTotalCalls(1))
        << "the shard worker never consumed the queued fact";

    center_->Stop(EventCenterStopMode::kDrain);
    const size_t calls_after_stop = policy_->TotalCalls();

    center_->Stop(EventCenterStopMode::kCancel);
    center_->Stop(EventCenterStopMode::kDrain);
    EXPECT_TRUE(center_->IsStopped());
    EXPECT_EQ(policy_->TotalCalls(), calls_after_stop);
}

// ---------------------------------------------------------------------------
// EventPublisher
// ---------------------------------------------------------------------------

// A producer built before its sink exists (or in a test that never wires one)
// must degrade to "closed", not to a null dereference.
TEST_F(EventCenterTest, ADefaultConstructedPublisherReportsClosed) {
    EventPublisher publisher;
    EXPECT_FALSE(publisher.IsAttached());
    EXPECT_EQ(
        publisher.Publish(MakeEvent(EventType::kCommit, "orphan", kTilerA)),
        QueuePushResult::kClosed);
}

// The handle is weak precisely so a producer can still be running while the
// center is torn down; losing a fact then beats keeping the sink alive from
// the producer side.
TEST_F(EventCenterTest, APublisherWhoseSinkIsGoneReportsClosed) {
    MakeCenter(/*event_capacity=*/8, /*movement_capacity=*/8);
    EventPublisher publisher = center_->Publisher();
    ASSERT_TRUE(publisher.IsAttached());
    ASSERT_EQ(publisher.Publish(MakeEvent(EventType::kAccess, "live", kTilerA)),
              QueuePushResult::kEnqueued);

    center_->Stop(EventCenterStopMode::kCancel);
    center_.reset();

    EXPECT_FALSE(publisher.IsAttached());
    EXPECT_EQ(
        publisher.Publish(MakeEvent(EventType::kCommit, "after_sink", kTilerA)),
        QueuePushResult::kClosed);
}

// ---------------------------------------------------------------------------
// Broadcast
// ---------------------------------------------------------------------------

// The defining property of the pub/sub model: an accepted event reaches every
// consumer subscribed to its type, and reaches each of them once. A center
// that delivered to the first subscriber only would look correct in every
// single-consumer test in this file.
TEST_F(EventCenterTest, EveryAcceptedEventReachesEverySubscriberExactlyOnce) {
    MakeCenter(/*event_capacity=*/16, /*movement_capacity=*/8);
    RecordingConsumer second("second", SubscriptionMask({EventType::kCommit}));
    RecordingConsumer third("third", SubscriptionMask({EventType::kCommit}));
    ASSERT_TRUE(center_->RegisterConsumer(&second).has_value());
    ASSERT_TRUE(center_->RegisterConsumer(&third).has_value());

    ASSERT_EQ(Publish(EventType::kCommit, "fan", kTilerA),
              QueuePushResult::kEnqueued);
    center_->DrainForTest();

    EXPECT_EQ(policy_->CountOf(CallKind::kConsume), 1U);
    EXPECT_EQ(second.CountOf(CallKind::kConsume), 1U);
    EXPECT_EQ(third.CountOf(CallKind::kConsume), 1U);

    const EventCenterMetrics metrics = center_->Metrics();
    EXPECT_EQ(metrics.events_broadcast, 1U);
    EXPECT_EQ(metrics.consumer_deliveries, 3U);
}

// A consumer sees only what it asked for. Without this the subscription is
// decoration and every consumer pays for every event.
TEST_F(EventCenterTest, AConsumerNeverSeesATypeItDidNotSubscribeTo) {
    MakeCenter(/*event_capacity=*/16, /*movement_capacity=*/8);
    RecordingConsumer commits_only("commits",
                                   SubscriptionMask({EventType::kCommit}));
    ASSERT_TRUE(center_->RegisterConsumer(&commits_only).has_value());

    ASSERT_EQ(Publish(EventType::kAccess, "only_access", kTilerA),
              QueuePushResult::kEnqueued);
    ASSERT_EQ(Publish(EventType::kCommit, "a_commit", kTilerA),
              QueuePushResult::kEnqueued);
    center_->DrainForTest();

    EXPECT_EQ(commits_only.CountOf(CallKind::kConsume), 1U);
    const auto seen = commits_only.EventsOf(CallKind::kConsume);
    ASSERT_EQ(seen.size(), 1U);
    EXPECT_EQ(seen[0].type, EventType::kCommit);
    // The subscriber to everything still saw both.
    EXPECT_EQ(policy_->CountOf(CallKind::kConsume), 2U);
}

// Ordering is per consumer, not merely global: two consumers must each see a
// key's Commit before its Delete, or one of them will act on a block the other
// already knows is gone.
TEST_F(EventCenterTest, EveryConsumerSeesOneKeysEventsInOrder) {
    MakeCenter(/*event_capacity=*/16, /*movement_capacity=*/8);
    RecordingConsumer second(
        "second", SubscriptionMask({EventType::kCommit, EventType::kAccess,
                                    EventType::kDelete}));
    ASSERT_TRUE(center_->RegisterConsumer(&second).has_value());

    ASSERT_EQ(Publish(EventType::kCommit, "ordered", kTilerA),
              QueuePushResult::kEnqueued);
    ASSERT_EQ(Publish(EventType::kAccess, "ordered", kTilerA),
              QueuePushResult::kEnqueued);
    ASSERT_EQ(Publish(EventType::kDelete, "ordered", kTilerA),
              QueuePushResult::kEnqueued);
    center_->DrainForTest();

    for (const RecordingConsumer* consumer : {policy_.get(), &second}) {
        const auto seen = consumer->EventsOf(CallKind::kConsume);
        ASSERT_EQ(seen.size(), 3U);
        EXPECT_EQ(seen[0].type, EventType::kCommit);
        EXPECT_EQ(seen[1].type, EventType::kAccess);
        EXPECT_EQ(seen[2].type, EventType::kDelete);
    }
}

// The inline fallback is a delivery path like any other: a lifecycle event
// that could not be queued still has to reach every subscriber, or one
// consumer's state silently diverges from the rest exactly when the node is
// busiest.
TEST_F(EventCenterTest, TheInlineFallbackAlsoReachesEverySubscriber) {
    MakeCenter(/*event_capacity=*/1, /*movement_capacity=*/8);
    RecordingConsumer second("second", SubscriptionMask({EventType::kCommit}));
    ASSERT_TRUE(center_->RegisterConsumer(&second).has_value());

    // Fill the single slot, then publish a lifecycle event into a full queue.
    ASSERT_EQ(Publish(EventType::kCommit, "filler", kTilerA),
              QueuePushResult::kEnqueued);
    ASSERT_EQ(Publish(EventType::kCommit, "inlined", kTilerA),
              QueuePushResult::kDropped);

    EXPECT_EQ(policy_->CountOf(CallKind::kApplyInline), 1U);
    EXPECT_EQ(second.CountOf(CallKind::kApplyInline), 1U);
    EXPECT_EQ(center_->Metrics().lifecycle_event_inline_applied, 1U);
}

// A read burst must not push a lifecycle event onto the inline path. The
// inline path applies the event immediately, ahead of everything already
// queued for that key -- so a Delete could be applied before the Commit that
// is still waiting in the shard queue, and with N consumers that is N
// out-of-order deliveries. The reserve is what keeps the fallback for a flood
// of lifecycle events rather than a consequence of reads.
TEST_F(EventCenterTest, AReadBurstDoesNotPushLifecycleEventsInline) {
    MakeCenter(/*event_capacity=*/16, /*movement_capacity=*/8);

    // No worker is running, so nothing drains: the queue fills as far as the
    // best-effort limit and stops.
    size_t accepted = 0;
    for (int i = 0; i < 64; ++i) {
        if (Publish(EventType::kAccess, "burst" + std::to_string(i), kTilerA) ==
            QueuePushResult::kEnqueued) {
            ++accepted;
        }
    }
    ASSERT_GT(accepted, 0U) << "the burst never got in, so nothing is proven";
    ASSERT_LT(accepted, 16U)
        << "the access stream filled the whole queue, leaving no reserve";

    // The lifecycle event still gets a slot.
    EXPECT_EQ(Publish(EventType::kCommit, "after_burst", kTilerA),
              QueuePushResult::kEnqueued);
    EXPECT_EQ(center_->Metrics().lifecycle_event_inline_applied, 0U);
    // And the dropped reads are visible rather than silent.
    EXPECT_GT(center_->Metrics().events_dropped, 0U);
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

TEST_F(EventCenterTest, RegistrationIsFrozenOnceTheCenterStarts) {
    MakeCenter(/*event_capacity=*/8, /*movement_capacity=*/8);
    center_->Start();

    RecordingConsumer late("late", SubscriptionMask({EventType::kCommit}));
    auto refused = center_->RegisterConsumer(&late);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(EventCenterTest, ADuplicateConsumerNameIsRefused) {
    MakeCenter(/*event_capacity=*/8, /*movement_capacity=*/8);
    // Same name as the fixture's consumer: names are metric labels, and two
    // consumers sharing one would merge their counters into nonsense.
    RecordingConsumer clash("recording",
                            SubscriptionMask({EventType::kCommit}));
    auto refused = center_->RegisterConsumer(&clash);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(EventCenterTest, AConsumerThatWantsNothingIsRefused) {
    MakeCenter(/*event_capacity=*/8, /*movement_capacity=*/8);
    RecordingConsumer silent("silent", 0);
    auto refused = center_->RegisterConsumer(&silent);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error(), ErrorCode::INVALID_PARAMS);

    EXPECT_FALSE(center_->RegisterConsumer(nullptr).has_value());
}

// ---------------------------------------------------------------------------
// Per-consumer accounting
// ---------------------------------------------------------------------------

// A failing consumer must be visible. The design is explicit that a Failed
// result is counted and alarmed on rather than swallowed, because a consumer
// that cannot apply a fact has diverged from the index.
TEST_F(EventCenterTest, EachConsumersOutcomesAreCountedSeparately) {
    MakeCenter(/*event_capacity=*/16, /*movement_capacity=*/8);
    RecordingConsumer second("second", SubscriptionMask({EventType::kCommit}));
    ASSERT_TRUE(center_->RegisterConsumer(&second).has_value());

    ASSERT_EQ(Publish(EventType::kCommit, "counted", kTilerA),
              QueuePushResult::kEnqueued);
    ASSERT_EQ(Publish(EventType::kAccess, "counted", kTilerA),
              QueuePushResult::kEnqueued);
    center_->DrainForTest();

    const EventCenterMetrics metrics = center_->Metrics();
    ASSERT_EQ(metrics.consumers.size(), 2U);
    EXPECT_EQ(metrics.consumers[0].name, "recording");
    EXPECT_EQ(metrics.consumers[1].name, "second");
    // The first saw both events, the second only the commit.
    EXPECT_EQ(metrics.consumers[0].applied, 2U);
    EXPECT_EQ(metrics.consumers[1].applied, 1U);
}

}  // namespace mooncake::v2
