#include "read_plan.h"
#include "dummy_client.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <optional>
#include <stdexcept>
#include <string>

namespace mooncake {
namespace {
using namespace std::chrono_literals;

class ReadPlanPipelineTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (const char* value = std::getenv("MOONCAKE_READ_PLAN_PIPELINE"))
            pipeline_ = value;
        setenv("MOONCAKE_READ_PLAN_PIPELINE", "1", 1);
    }
    void TearDown() override {
        if (pipeline_)
            setenv("MOONCAKE_READ_PLAN_PIPELINE", pipeline_->c_str(), 1);
        else
            unsetenv("MOONCAKE_READ_PLAN_PIPELINE");
    }

   private:
    std::optional<std::string> pipeline_;
};

struct PipelineBackend : DummyClient {
    std::atomic<int> active{0}, peak{0}, ends{0}, calls{0};
    std::promise<void> first_entered, second_entered;
    std::promise<void> release_first, release_second;
    std::shared_future<void> first_gate = release_first.get_future().share();
    std::shared_future<void> second_gate = release_second.get_future().share();
    bool fail_first = false, fail_second = false;

    std::vector<int> batch_get_session_start(
        const std::vector<std::string>& keys) override {
        return std::vector<int>(keys.size(), 0);
    }

    std::vector<int> batch_get_into_multi_buffer_ranges(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& buffers,
        const std::vector<std::vector<size_t>>& sizes,
        const std::vector<std::vector<size_t>>& offsets) override {
        ++calls;
        const int n = ++active;
        int old = peak.load();
        while (old < n && !peak.compare_exchange_weak(old, n)) {
        }
        const int group = static_cast<int>(offsets[0][0]);
        if (group == 0) {
            first_entered.set_value();
            if (first_gate.wait_for(5s) != std::future_status::ready) {
                --active;
                throw std::runtime_error("first gate timeout");
            }
        }
        if (group == 1) {
            second_entered.set_value();
            if (second_gate.wait_for(5s) != std::future_status::ready) {
                --active;
                throw std::runtime_error("second gate timeout");
            }
        }
        std::vector<int> result(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            for (size_t j = 0; j < buffers[i].size(); ++j) {
                *static_cast<unsigned char*>(buffers[i][j]) = group + 1;
                result[i] += sizes[i][j];
            }
        }
        --active;
        if ((group == 0 && fail_first) || (group == 1 && fail_second))
            result[0] = -1;
        return result;
    }

    int batch_get_session_end(const std::vector<std::string>&) override {
        EXPECT_EQ(active.load(), 0);
        ++ends;
        return 0;
    }
};

std::vector<ReadLayout> pipeline_layout(unsigned char* dst) {
    std::vector<std::vector<ReadComponent>> groups;
    for (int group = 0; group < 4; ++group) {
        groups.push_back({{reinterpret_cast<size_t>(dst + group), 0, 1,
                           static_cast<size_t>(group)}});
    }
    return {{{"a"}, {0}, true, groups}};
}

TEST_F(ReadPlanPipelineTest, PublishesInOrderWithinTwoGroupWindow) {
    for (bool fail : {false, true}) {
        SCOPED_TRACE(fail);
        unsigned char dst[4]{};
        auto backend = std::make_shared<PipelineBackend>();
        backend->fail_first = fail;
        ReadPlan plan(backend, pipeline_layout(dst), 4);
        auto done = std::async(std::launch::async, [&] { plan.run(); });
        ASSERT_EQ(backend->first_entered.get_future().wait_for(3s),
                  std::future_status::ready);
        ASSERT_EQ(backend->second_entered.get_future().wait_for(3s),
                  std::future_status::ready);
        EXPECT_EQ(backend->peak.load(), 2);
        EXPECT_EQ(backend->calls.load(), 2);

        // Layer 1 can complete first, but layer 0 stays unready and layer 2
        // cannot be admitted beyond the two-layer window.
        backend->release_second.set_value();
        auto waiter = std::async(std::launch::async, [&] { plan.wait(0); });
        EXPECT_EQ(waiter.wait_for(20ms), std::future_status::timeout);
        EXPECT_EQ(backend->calls.load(), 2);
        EXPECT_EQ(backend->ends.load(), 0);
        backend->release_first.set_value();
        if (fail) {
            EXPECT_THROW(done.get(), std::runtime_error);
            EXPECT_THROW(waiter.get(), std::runtime_error);
            EXPECT_EQ(backend->calls.load(), 2);
        } else {
            done.get();
            waiter.get();
            plan.wait(3);
            for (int group = 0; group < 4; ++group)
                EXPECT_EQ(dst[group], group + 1);
            EXPECT_EQ(plan.stats(), std::vector<uint64_t>({4, 4, 4}));
        }
        EXPECT_EQ(backend->ends.load(), 1);
        EXPECT_EQ(backend->active.load(), 0);
    }
}

TEST_F(ReadPlanPipelineTest, DrainsInFlightReadsBeforeSessionCleanup) {
    unsigned char dst[4]{};
    auto backend = std::make_shared<PipelineBackend>();
    backend->fail_first = true;
    ReadPlan plan(backend, pipeline_layout(dst), 4);
    auto done = std::async(std::launch::async, [&] { plan.run(); });
    ASSERT_EQ(backend->first_entered.get_future().wait_for(3s),
              std::future_status::ready);
    ASSERT_EQ(backend->second_entered.get_future().wait_for(3s),
              std::future_status::ready);
    backend->release_first.set_value();
    EXPECT_EQ(done.wait_for(30ms), std::future_status::timeout);
    EXPECT_EQ(backend->ends.load(), 0);
    backend->release_second.set_value();
    EXPECT_THROW(done.get(), std::runtime_error);
    EXPECT_EQ(backend->ends.load(), 1);
    EXPECT_EQ(backend->calls.load(), 2);
}

TEST_F(ReadPlanPipelineTest, LaterFailureDoesNotRevokePublishedGroups) {
    for (const char* pipeline : {"0", "1"}) {
        SCOPED_TRACE(pipeline);
        setenv("MOONCAKE_READ_PLAN_PIPELINE", pipeline, 1);
        unsigned char dst[4]{};
        auto backend = std::make_shared<PipelineBackend>();
        backend->fail_second = true;
        ReadPlan plan(backend, pipeline_layout(dst), 4);
        EXPECT_FALSE(plan.is_finished());
        auto done = std::async(std::launch::async, [&] { plan.run(); });
        backend->release_first.set_value();
        ASSERT_EQ(backend->second_entered.get_future().wait_for(3s),
                  std::future_status::ready);
        plan.wait(0);
        EXPECT_EQ(dst[0], 1);
        EXPECT_FALSE(plan.is_finished());
        backend->release_second.set_value();
        EXPECT_THROW(done.get(), std::runtime_error);
        EXPECT_TRUE(plan.is_finished());
        // A late consumer must get the same successful result as the early one.
        plan.wait(0);
        for (int group = 1; group < 4; ++group) {
            EXPECT_THROW(plan.wait(group), std::runtime_error);
        }
    }
}

TEST_F(ReadPlanPipelineTest, RequiresExplicitBooleanOptIn) {
    for (const char* value : {"", "0", "2", "3"}) {
        SCOPED_TRACE(value);
        if (*value) {
            setenv("MOONCAKE_READ_PLAN_PIPELINE", value, 1);
        } else {
            unsetenv("MOONCAKE_READ_PLAN_PIPELINE");
        }
        unsigned char dst[4]{};
        auto backend = std::make_shared<PipelineBackend>();
        backend->release_first.set_value();
        backend->release_second.set_value();
        ReadPlan plan(backend, pipeline_layout(dst), 4);
        plan.run();
        EXPECT_EQ(backend->peak.load(), 1);
        EXPECT_EQ(backend->calls.load(), 4);
        EXPECT_EQ(backend->ends.load(), 1);
        for (int group = 0; group < 4; ++group)
            EXPECT_EQ(dst[group], group + 1);
    }
}

}  // namespace
}  // namespace mooncake
