#include "metadata/namespace.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace metadata {
namespace {

// The route's own behaviour (publication, generations, groups, cursors, slot
// collection) is covered by object_route_test.cpp. These cases cover what the
// namespace adds on top: the in-flight list it keeps from every released write
// guard, and Empty().

std::vector<std::string> SortedInFlightKeys(const Namespace& ns) {
    auto keys = ns.InFlightKeys();
    std::sort(keys.begin(), keys.end());
    return keys;
}

// Starts or finishes a primary write on the object under `key`, under a write
// guard of its own.
void SetProcessing(Namespace& ns, const std::string& key, bool processing) {
    auto guard = ns.objects.Write(key);
    ASSERT_TRUE(guard.has_value());
    guard->state().is_processing = processing;
}

TEST(NamespaceTest, InFlightListsOnlyObjectsWithWork) {
    Namespace ns;
    ASSERT_NE(test::PublishObject(ns.objects, "idle"), 0u);
    ASSERT_NE(test::PublishObject(ns.objects, "busy"), 0u);
    EXPECT_TRUE(ns.InFlightKeys().empty());

    {
        auto guard = ns.objects.Write("busy");
        ASSERT_TRUE(guard.has_value());
        guard->state().is_processing = true;
        // Listed as the guard is released, not before.
        EXPECT_TRUE(ns.InFlightKeys().empty());
    }
    EXPECT_EQ(SortedInFlightKeys(ns), (std::vector<std::string>{"busy"}));

    // A write guard on an object with no work lists nothing.
    {
        auto guard = ns.objects.Write("idle");
        ASSERT_TRUE(guard.has_value());
    }
    // Starting work again on a listed object lists it once.
    SetProcessing(ns, "busy", true);
    EXPECT_EQ(SortedInFlightKeys(ns), (std::vector<std::string>{"busy"}));

    SetProcessing(ns, "busy", false);
    EXPECT_TRUE(ns.InFlightKeys().empty());
}

TEST(NamespaceTest, AKeyStaysListedWhileAnyWorkRemains) {
    Namespace ns;
    ASSERT_NE(test::PublishObject(ns.objects, "k1"), 0u);

    {
        auto guard = ns.objects.Write("k1");
        ASSERT_TRUE(guard.has_value());
        guard->state().is_processing = true;
        guard->state().offloading_task =
            OffloadingTask{1, std::chrono::system_clock::now(), {}};
    }

    // The write finished but the offload has not, so the key stays listed.
    SetProcessing(ns, "k1", false);
    EXPECT_EQ(SortedInFlightKeys(ns), (std::vector<std::string>{"k1"}));

    {
        auto guard = ns.objects.Write("k1");
        ASSERT_TRUE(guard.has_value());
        guard->state().offloading_task.reset();
    }
    EXPECT_TRUE(ns.InFlightKeys().empty());
}

TEST(NamespaceTest, PublishingWithWorkListsTheKeyOnRelease) {
    Namespace ns;
    {
        // The write that creates the object starts with work in flight.
        auto guard = ns.objects.WriteKey("k1");
        ASSERT_FALSE(guard.has_object());
        (void)test::PublishEnvelope(guard);
        guard.state().is_processing = true;
        EXPECT_TRUE(ns.InFlightKeys().empty());
    }
    EXPECT_EQ(SortedInFlightKeys(ns), (std::vector<std::string>{"k1"}));

    // A second writer that finds the key taken publishes nothing and leaves
    // the listing as it is.
    EXPECT_EQ(test::PublishObject(ns.objects, "k1"), 0u);
    EXPECT_EQ(SortedInFlightKeys(ns), (std::vector<std::string>{"k1"}));

    SetProcessing(ns, "k1", false);
    EXPECT_TRUE(ns.InFlightKeys().empty());
}

TEST(NamespaceTest, AWriterThatPublishesNothingListsNothing) {
    Namespace ns;
    {
        auto guard = ns.objects.WriteKey("k1");
    }
    EXPECT_TRUE(ns.InFlightKeys().empty());
    EXPECT_TRUE(ns.Empty());
}

TEST(NamespaceTest, TearDownTakesAKeyOffTheInFlightList) {
    Namespace ns;
    ASSERT_NE(test::PublishObject(ns.objects, "k1"), 0u);
    ASSERT_NE(test::PublishObject(ns.objects, "k2"), 0u);
    SetProcessing(ns, "k1", true);
    SetProcessing(ns, "k2", true);
    ASSERT_EQ(SortedInFlightKeys(ns), (std::vector<std::string>{"k1", "k2"}));

    // The object still carried its write, yet the key leaves the list with
    // it.
    {
        auto guard = ns.objects.Write("k1");
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
    }
    EXPECT_EQ(SortedInFlightKeys(ns), (std::vector<std::string>{"k2"}));

    SetProcessing(ns, "k2", false);
    EXPECT_TRUE(ns.InFlightKeys().empty());
}

TEST(NamespaceTest, AReplacementIsListedByItsOwnWork) {
    Namespace ns;
    ASSERT_NE(test::PublishObject(ns.objects, "k1"), 0u);
    SetProcessing(ns, "k1", true);

    // A replacement under one guard starts with empty state, so the key is
    // unlisted unless the new publication carries work of its own.
    {
        auto guard = ns.objects.Write("k1");
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
        (void)test::PublishEnvelope(*guard);
    }
    EXPECT_TRUE(ns.InFlightKeys().empty());

    {
        auto guard = ns.objects.Write("k1");
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
        (void)test::PublishEnvelope(*guard);
        guard->state().is_processing = true;
    }
    EXPECT_EQ(SortedInFlightKeys(ns), (std::vector<std::string>{"k1"}));

    SetProcessing(ns, "k1", false);
}

TEST(NamespaceTest, DestroyingANamespaceWithListedKeysIsClean) {
    // The list is declared after the route, so it lets go of the slots' hooks
    // before the route destroys the slots: none is destroyed still linked.
    Namespace ns;
    ASSERT_NE(test::PublishObject(ns.objects, "k1"), 0u);
    ASSERT_NE(test::PublishObject(ns.objects, "k2", "g1"), 0u);
    SetProcessing(ns, "k1", true);
    SetProcessing(ns, "k2", true);
    ASSERT_EQ(ns.InFlightKeys().size(), 2u);
}

TEST(NamespaceTest, EmptyTracksObjectsAndGroups) {
    Namespace ns;
    EXPECT_TRUE(ns.Empty());

    ASSERT_NE(test::PublishObject(ns.objects, "k1", "g1"), 0u);
    EXPECT_FALSE(ns.Empty());

    // Tearing the object down drops its membership with it.
    {
        auto guard = ns.objects.Write("k1");
        ASSERT_TRUE(guard.has_value());
        guard->TearDown();
    }
    EXPECT_TRUE(ns.Empty());
    EXPECT_TRUE(ns.objects.GroupMembers("g1").empty());
}

TEST(NamespaceTest, ConcurrentWritersKeepTheInFlightListExact) {
    // Writers on different keys share in-flight stripes; each ends with work
    // on its even keys only, and the list must say exactly that.
    Namespace ns;
    constexpr int kThreads = 8;
    constexpr int kKeysPerThread = 64;
    constexpr int kRounds = 50;
    const auto key_of = [](int thread, int k) {
        return "t" + std::to_string(thread) + "-k" + std::to_string(k);
    };
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int k = 0; k < kKeysPerThread; ++k) {
                ASSERT_NE(test::PublishObject(ns.objects, key_of(t, k)), 0u);
            }
            for (int round = 0; round < kRounds; ++round) {
                const bool last = round == kRounds - 1;
                for (int k = 0; k < kKeysPerThread; ++k) {
                    SetProcessing(ns, key_of(t, k),
                                  last ? k % 2 == 0 : round % 2 == 0);
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    std::vector<std::string> expected;
    for (int t = 0; t < kThreads; ++t) {
        for (int k = 0; k < kKeysPerThread; k += 2) {
            expected.push_back(key_of(t, k));
        }
    }
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(SortedInFlightKeys(ns), expected);
}

// A policy that records what the namespace tells it, keeps a count in the
// key's first policy word while the key holds an object, and refuses growth
// past a limit.
class RecordingPolicy final : public NamespacePolicy {
   public:
    struct Attachment final : PolicyAttachment {
        int id;
        explicit Attachment(int value) : id(value) {}
    };

    tl::expected<void, ErrorCode> OnGrow(const PolicyContext& ctx,
                                         uint64_t bytes) override {
        Record(ctx, "grow");
        if (bytes > kGrowLimit) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        return {};
    }
    void OnReplace(const PolicyContext& ctx) override {
        Record(ctx, "replace");
    }
    void OnTearDown(const PolicyContext& ctx) override {
        Record(ctx, "teardown");
    }
    void OnWriteRelease(const PolicyContext& ctx) override {
        Record(ctx, ctx.guard.has_object() ? "release" : "release-empty");
        auto& words = ctx.guard.owner_state().policy_words;
        words[0] = ctx.guard.has_object() ? words[0] + 1 : 0;
    }

    static constexpr uint64_t kGrowLimit = 1024;
    std::vector<std::string> calls;
    const TenantId* last_tenant = nullptr;
    PolicyAttachment* last_attachment = nullptr;

   private:
    void Record(const PolicyContext& ctx, const char* call) {
        calls.push_back(std::string(call) + ":" + ctx.guard.key());
        last_tenant = &ctx.tenant_id;
        last_attachment = ctx.attachment;
    }
};

TEST(NamespacePolicyTest, WritesReachThePolicyWithTheirContext) {
    RecordingPolicy policy;
    auto attachment = std::make_unique<RecordingPolicy::Attachment>(7);
    PolicyAttachment* const attached = attachment.get();
    Namespace ns(TenantId("tenant-a"), &policy, std::move(attachment));

    ASSERT_NE(test::PublishObject(ns.objects, "k1"), 0u);
    EXPECT_EQ(policy.calls, (std::vector<std::string>{"release:k1"}));
    ASSERT_NE(policy.last_tenant, nullptr);
    EXPECT_EQ(policy.last_tenant->value(), "tenant-a");
    EXPECT_EQ(policy.last_attachment, attached);

    policy.calls.clear();
    {
        auto guard = ns.objects.Write("k1");
        ASSERT_TRUE(guard.has_value());
        EXPECT_TRUE(ns.Grow(*guard, RecordingPolicy::kGrowLimit).has_value());
        EXPECT_EQ(ns.Grow(*guard, RecordingPolicy::kGrowLimit + 1).error(),
                  ErrorCode::NO_AVAILABLE_HANDLE);
        // The policy hears of a write only as its guard is released.
        EXPECT_EQ(policy.calls,
                  (std::vector<std::string>{"grow:k1", "grow:k1"}));
        // The words it keeps survive from one write to the next.
        EXPECT_EQ(guard->owner_state().policy_words[0], 1u);
    }
    EXPECT_EQ(policy.calls.back(), "release:k1");

    // Reads are not the policy's business.
    policy.calls.clear();
    ASSERT_TRUE(ns.objects.Read("k1").has_value());
    EXPECT_TRUE(policy.calls.empty());
}

TEST(NamespacePolicyTest, ReplaceAndTearDownAreAnnouncedBeforeTheyHappen) {
    RecordingPolicy policy;
    Namespace ns(TenantId(), &policy);
    ASSERT_NE(test::PublishObject(ns.objects, "k1"), 0u);
    policy.calls.clear();

    {
        auto guard = ns.objects.Write("k1");
        ASSERT_TRUE(guard.has_value());
        ns.BeforeReplace(*guard);
        guard->TearDown();
        (void)test::PublishEnvelope(*guard);
    }
    {
        auto guard = ns.objects.Write("k1");
        ASSERT_TRUE(guard.has_value());
        ns.BeforeTearDown(*guard);
        guard->TearDown();
    }
    EXPECT_EQ(policy.calls,
              (std::vector<std::string>{"replace:k1", "release:k1",
                                        "teardown:k1", "release-empty:k1"}));
    EXPECT_TRUE(ns.Empty());
}

TEST(NamespacePolicyTest, WithoutAPolicyEveryGrowthIsGranted) {
    Namespace ns;
    ASSERT_NE(test::PublishObject(ns.objects, "k1"), 0u);
    auto guard = ns.objects.Write("k1");
    ASSERT_TRUE(guard.has_value());
    EXPECT_TRUE(ns.Grow(*guard, UINT64_MAX).has_value());
    ns.BeforeReplace(*guard);
    ns.BeforeTearDown(*guard);
}

// --- Write hooks -----------------------------------------------------------
//
// A write lock on a key is reported to the namespace's chain twice: under the
// key's lock before the first change, where a hook may refuse it, and under
// that same lock as it is released, where it sees what the write left. These
// cases pin what a hook is promised, because the OpLog will hang on it:
//
//   * both calls hold the key exclusively, and the second sees the post-state;
//   * a write that changed nothing is never reported, so a refused delete can
//     not be recorded as a delete;
//   * a hook finishes before the namespace settles accounting and indexes, so
//     what it changes is accounted for;
//   * each hook gets the reservation its own begin filled, in chain order.

// Names its owner, so a reservation handed to the wrong hook is visible.
struct Token final : WriteReservation {
    explicit Token(std::string owner) : owner(std::move(owner)) {}
    std::string owner;
};

// Records what the chain told it, in order, and whether the key's lock was
// held at the time. `route` is optional: only the lock cases set it.
class ChainHook final : public WriteHook {
   public:
    ChainHook(std::string name, std::vector<std::string>* order, bool refuse)
        : name_(std::move(name)), order_(order), refuse_(refuse) {}

    ChainHook(std::string name, std::vector<std::string>* order,
              const route::ObjectRoute* route)
        : name_(std::move(name)), order_(order), route_(route) {}

    tl::expected<void, ErrorCode> OnWriteBegin(
        const WriteGuard& guard, WriteChange change,
        std::unique_ptr<WriteReservation>& reservation) override {
        ++begins_;
        begin_change_ = change;
        begin_object_ = guard.has_object();
        begin_processing_ = guard.has_object() && guard.state().is_processing;
        begin_locked_ = LockedByAnotherThread(guard);
        reservation = std::make_unique<Token>(name_);
        if (refuse_) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        order_->push_back(name_ + ":begin");
        return {};
    }

    void OnWriteRelease(
        const WriteGuard& guard, WriteChange change,
        std::unique_ptr<WriteReservation>& reservation) override {
        ++releases_;
        release_change_ = change;
        release_object_ = guard.has_object();
        release_processing_ = guard.has_object() && guard.state().is_processing;
        release_locked_ = LockedByAnotherThread(guard);
        auto* token = dynamic_cast<Token*>(reservation.get());
        reservation_owner_ = token != nullptr ? token->owner : "<none>";
        order_->push_back(name_ + ":release");
    }

    int begins_ = 0;
    int releases_ = 0;
    WriteChange begin_change_{WriteChange::kNone};
    WriteChange release_change_{WriteChange::kNone};
    bool begin_object_ = false;
    bool release_object_ = false;
    bool begin_processing_ = false;
    bool release_processing_ = false;
    bool begin_locked_ = false;
    bool release_locked_ = false;
    std::string reservation_owner_;

   private:
    // A different thread tries the key's lock, so the answer is about this
    // thread's hold and not a recursive try of its own.
    bool LockedByAnotherThread(const WriteGuard& guard) {
        if (route_ == nullptr) {
            return false;
        }
        const std::string key = guard.key();
        return !std::async(std::launch::async, [this, key] {
                    return test::ObjectRouteTestPeer::KeyLockFree(*route_, key);
                }).get();
    }

    std::string name_;
    std::vector<std::string>* order_;
    const route::ObjectRoute* route_ = nullptr;
    bool refuse_ = false;
};

// Records what the namespace told the policy, and when relative to the chain.
class AccountingPolicy final : public NamespacePolicy {
   public:
    void OnWriteRelease(const PolicyContext& ctx) override {
        ++releases;
        saw_object = ctx.guard.has_object();
        saw_processing =
            ctx.guard.has_object() && ctx.guard.state().is_processing;
    }
    int releases = 0;
    bool saw_object = false;
    bool saw_processing = false;
};

TEST(NamespaceWriteHookTest, BothCallsHoldTheKeyAndTheSecondSeesTheChange) {
    Namespace ns;
    std::vector<std::string> order;
    ChainHook hook("h", &order, &ns.objects);
    ns.RegisterWriteHook(&hook);
    ASSERT_NE(test::PublishObject(ns.objects, "k"), 0u);

    {
        auto result = ns.objects.Write("k", WriteChange::kNewWrite);
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result->has_value());
        auto& guard = result->value();
        // The first call ran, under the lock, before anything changed.
        EXPECT_EQ(hook.begins_, 1);
        EXPECT_EQ(hook.releases_, 0);
        EXPECT_TRUE(hook.begin_locked_);
        EXPECT_FALSE(hook.begin_processing_);
        guard.state().is_processing = true;
        guard.Commit();
        EXPECT_EQ(hook.releases_, 0);
    }
    EXPECT_EQ(hook.releases_, 1);
    EXPECT_TRUE(hook.release_locked_);
    EXPECT_TRUE(hook.release_processing_);
    EXPECT_EQ(hook.release_change_, WriteChange::kNewWrite);
    EXPECT_EQ(order, (std::vector<std::string>{"h:begin", "h:release"}));
}

// The case that makes a refused delete safe: taking a guard for a removal and
// then deciding not to remove must not be reported as a removal.
TEST(NamespaceWriteHookTest, AWriteThatChangedNothingIsNeverReported) {
    Namespace ns;
    std::vector<std::string> order;
    ChainHook hook("h", &order, /*refuse=*/false);
    ns.RegisterWriteHook(&hook);
    ASSERT_NE(test::PublishObject(ns.objects, "k"), 0u);

    {
        auto result = ns.objects.Write("k", WriteChange::kKeyRemoved);
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result->has_value());
        // An ordinary refusal on the way to the removal: nothing touched.
    }

    EXPECT_EQ(hook.begins_, 1);
    EXPECT_EQ(hook.releases_, 0);
    EXPECT_EQ(order, (std::vector<std::string>{"h:begin"}));
    EXPECT_TRUE(ns.objects.Contains("k"));
}

TEST(NamespaceWriteHookTest, PublishingAndTearingDownCommitTheWrite) {
    Namespace ns;
    std::vector<std::string> order;
    ChainHook hook("h", &order, /*refuse=*/false);
    ns.RegisterWriteHook(&hook);

    {
        auto created = ns.objects.WriteKey("k", WriteChange::kNewWrite);
        ASSERT_TRUE(created.has_value());
        (void)test::PublishEnvelope(*created);
    }
    EXPECT_EQ(hook.releases_, 1);
    EXPECT_EQ(hook.release_change_, WriteChange::kNewWrite);
    EXPECT_TRUE(hook.release_object_);

    {
        auto removed = ns.objects.Write("k", WriteChange::kKeyRemoved);
        ASSERT_TRUE(removed.has_value());
        ASSERT_TRUE(removed->has_value());
        removed->value().TearDown();
    }
    EXPECT_EQ(hook.releases_, 2);
    EXPECT_EQ(hook.release_change_, WriteChange::kKeyRemoved);
    EXPECT_FALSE(hook.release_object_);
    EXPECT_TRUE(order == (std::vector<std::string>{"h:begin", "h:release",
                                                   "h:begin", "h:release"}));
}

TEST(NamespaceWriteHookTest, CancelUndoesACommit) {
    Namespace ns;
    std::vector<std::string> order;
    ChainHook hook("h", &order, /*refuse=*/false);
    ns.RegisterWriteHook(&hook);
    ASSERT_NE(test::PublishObject(ns.objects, "k"), 0u);

    {
        auto result = ns.objects.Write("k", WriteChange::kReplicasRemoved);
        ASSERT_TRUE(result.has_value() && result->has_value());
        auto& guard = result->value();
        guard.Commit();
        guard.Cancel();
    }
    EXPECT_EQ(hook.releases_, 0);
    EXPECT_TRUE(ns.objects.Contains("k"));
}

TEST(NamespaceWriteHookTest, EachHookGetsItsOwnReservationInChainOrder) {
    Namespace ns;
    std::vector<std::string> order;
    ChainHook first("first", &order, /*refuse=*/false);
    ChainHook second("second", &order, /*refuse=*/false);
    ns.RegisterWriteHook(&first);
    ns.RegisterWriteHook(&second);
    ASSERT_NE(test::PublishObject(ns.objects, "k"), 0u);

    {
        auto result = ns.objects.Write("k", WriteChange::kNewWrite);
        ASSERT_TRUE(result.has_value() && result->has_value());
        result->value().Commit();
    }

    EXPECT_EQ(order,
              (std::vector<std::string>{"first:begin", "second:begin",
                                        "first:release", "second:release"}));
    // Each release got the reservation its own begin filled.
    EXPECT_EQ(first.reservation_owner_, "first");
    EXPECT_EQ(second.reservation_owner_, "second");
}

TEST(NamespaceWriteHookTest, ARefusedBeginLeavesTheKeyAsItWas) {
    Namespace ns;
    std::vector<std::string> order;
    ChainHook kept("kept", &order, /*refuse=*/false);
    ChainHook refusing("refusing", &order, /*refuse=*/true);
    ns.RegisterWriteHook(&kept);
    ns.RegisterWriteHook(&refusing);
    ASSERT_NE(test::PublishObject(ns.objects, "k"), 0u);
    {
        auto guard = ns.objects.Write("k");
        guard->state().is_processing = true;
    }
    const auto in_flight = ns.InFlightKeys();

    {
        auto refused = ns.objects.Write("k", WriteChange::kReplicasRemoved);
        ASSERT_FALSE(refused.has_value());
        EXPECT_EQ(refused.error(), ErrorCode::INVALID_PARAMS);
    }
    // The key, its work in flight and the chain are as they were.
    EXPECT_TRUE(ns.objects.Contains("k"));
    EXPECT_EQ(ns.InFlightKeys(), in_flight);
    EXPECT_EQ(kept.releases_, 0);
    EXPECT_EQ(refusing.releases_, 0);
    EXPECT_EQ(order, (std::vector<std::string>{"kept:begin"}));

    // A refused write that would have created the key leaves no slot behind.
    {
        auto refused = ns.objects.WriteKey("fresh", WriteChange::kNewWrite);
        EXPECT_FALSE(refused.has_value());
    }
    EXPECT_FALSE(ns.objects.Contains("fresh"));
    EXPECT_EQ(test::ObjectRouteTestPeer::SlotCount(ns.objects), 1u);
}

// A hook that finishes a deferred removal at release: what it removes must be
// settled by the accounting that follows, not before it.
class RemovingHook final : public WriteHook {
   public:
    tl::expected<void, ErrorCode> OnWriteBegin(
        const WriteGuard&, WriteChange,
        std::unique_ptr<WriteReservation>&) override {
        return {};
    }
    void OnWriteRelease(const WriteGuard& guard, WriteChange,
                        std::unique_ptr<WriteReservation>&) override {
        guard.TearDown();
    }
};

TEST(NamespaceWriteHookTest,
     AReleaseThatRemovesTheObjectIsAccountedAfterwards) {
    AccountingPolicy policy;
    RemovingHook hook;
    Namespace ns_with_policy(TenantId(), &policy);
    ns_with_policy.RegisterWriteHook(&hook);
    ASSERT_NE(test::PublishObject(ns_with_policy.objects, "k"), 0u);
    // Work in flight, so the removal has to take the key off that list too.
    {
        auto guard = ns_with_policy.objects.Write("k");
        guard->state().is_processing = true;
    }
    ASSERT_EQ(ns_with_policy.InFlightKeys(), (std::vector<std::string>{"k"}));
    policy = AccountingPolicy{};

    {
        auto result =
            ns_with_policy.objects.Write("k", WriteChange::kKeyRemoved);
        ASSERT_TRUE(result.has_value() && result->has_value());
        result->value().Commit();
    }

    EXPECT_FALSE(ns_with_policy.objects.Contains("k"));
    EXPECT_TRUE(ns_with_policy.InFlightKeys().empty());
    // The policy settled a key that no longer holds an object.
    EXPECT_EQ(policy.releases, 1);
    EXPECT_FALSE(policy.saw_object);
    EXPECT_FALSE(policy.saw_processing);
}

TEST(NamespaceWriteHookTest, AMovedGuardReleasesOnceAndKeepsItsReservation) {
    Namespace ns;
    std::vector<std::string> order;
    ChainHook hook("h", &order, /*refuse=*/false);
    ns.RegisterWriteHook(&hook);
    ASSERT_NE(test::PublishObject(ns.objects, "k"), 0u);

    {
        auto result = ns.objects.Write("k", WriteChange::kNewWrite);
        ASSERT_TRUE(result.has_value() && result->has_value());
        auto guard = std::move(result->value());
        EXPECT_EQ(guard.change(), WriteChange::kNewWrite);
        EXPECT_EQ(hook.releases_, 0);
        guard.Commit();
    }
    EXPECT_EQ(hook.releases_, 1);
    EXPECT_EQ(hook.reservation_owner_, "h");
}

TEST(NamespaceWriteHookTest, OnlyAHeldPublicationIsReported) {
    Namespace ns;
    std::vector<std::string> order;
    ChainHook hook("h", &order, /*refuse=*/false);
    ns.RegisterWriteHook(&hook);

    auto missing = ns.objects.Write("missing", WriteChange::kNewWrite);
    ASSERT_TRUE(missing.has_value());
    EXPECT_FALSE(missing->has_value());

    const auto generation = test::PublishObject(ns.objects, "k");
    ASSERT_NE(generation, 0u);
    auto stale = ns.objects.Write(route::ObjectRef{"k", generation + 1},
                                  WriteChange::kNewWrite);
    ASSERT_TRUE(stale.has_value());
    EXPECT_FALSE(stale->has_value());
    EXPECT_EQ(hook.begins_, 0);

    {
        auto held = ns.objects.Write(route::ObjectRef{"k", generation},
                                     WriteChange::kNewWrite);
        ASSERT_TRUE(held.has_value() && held->has_value());
        held->value().Commit();
    }
    EXPECT_EQ(hook.begins_, 1);
    EXPECT_EQ(hook.releases_, 1);
}

// Without an observer the route still records what the caller is about to do,
// so the intent does not depend on who is listening.
TEST(NamespaceWriteHookTest, TheIntentIsKeptWithoutAnObserver) {
    route::ObjectRoute route;
    {
        auto created = route.WriteKey("k", WriteChange::kKeyRemoved);
        ASSERT_TRUE(created.has_value());
        EXPECT_EQ(created->change(), WriteChange::kKeyRemoved);
        EXPECT_FALSE(created->committed());
        (void)test::PublishEnvelope(*created);
        // Publishing commits the write by itself.
        EXPECT_TRUE(created->committed());
    }

    auto found = route.Write("k", WriteChange::kReplicasRemoved);
    ASSERT_TRUE(found.has_value() && found->has_value());
    EXPECT_EQ(found->value().change(), WriteChange::kReplicasRemoved);
    EXPECT_FALSE(found->value().committed());
}

TEST(NamespaceWriteHookTest, AWriteWithNoHookStillCommits) {
    Namespace ns;
    ASSERT_NE(test::PublishObject(ns.objects, "k"), 0u);
    {
        auto result = ns.objects.Write("k", WriteChange::kNewWrite);
        ASSERT_TRUE(result.has_value() && result->has_value());
        EXPECT_FALSE(result->value().committed());
        result->value().Commit();
        EXPECT_TRUE(result->value().committed());
    }
    EXPECT_TRUE(ns.objects.Contains("k"));
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
