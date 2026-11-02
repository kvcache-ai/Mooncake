#include "metadata/namespace.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
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

}  // namespace
}  // namespace metadata
}  // namespace mooncake
