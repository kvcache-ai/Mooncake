#include "random.h"

#include <cstdint>
#include <future>
#include <limits>
#include <random>
#include <thread>

#include <gtest/gtest.h>

using namespace mooncake;

TEST(RandomTest, ReusesEngineWithinThread) {
    EXPECT_EQ(&threadLocalRandomEngine(), &threadLocalRandomEngine());
}

TEST(RandomTest, UsesDifferentEngineAcrossThreads) {
    auto* main_engine = &threadLocalRandomEngine();
    std::promise<RandomEngine*> worker_address;
    std::promise<void> release_worker;
    auto release_future = release_worker.get_future();

    std::thread worker([&] {
        worker_address.set_value(&threadLocalRandomEngine());
        release_future.wait();
    });

    EXPECT_NE(main_engine, worker_address.get_future().get());
    release_worker.set_value();
    worker.join();
}

TEST(RandomTest, RandomIndexStaysWithinBounds) {
    std::mt19937_64 engine(42);
    for (size_t i = 0; i < 10000; ++i) {
        EXPECT_LT(randomIndex(17, engine), 17);
    }
}

TEST(RandomTest, ExplicitEngineIsDeterministic) {
    std::mt19937_64 first(42);
    std::mt19937_64 second(42);
    for (size_t i = 0; i < 100; ++i) {
        EXPECT_EQ(randomIndex(1000, first), randomIndex(1000, second));
    }
}

TEST(RandomTest, RandomUniformIncludesRequestedBounds) {
    std::mt19937_64 engine(42);
    for (size_t i = 0; i < 10000; ++i) {
        int value = randomUniform(-5, 9, engine);
        EXPECT_GE(value, -5);
        EXPECT_LE(value, 9);
    }
    EXPECT_EQ(randomUniform(7, 7, engine), 7);
}

TEST(RandomTest, RandomUniformSupportsNarrowIntegers) {
    std::mt19937_64 engine(42);
    for (size_t i = 0; i < 1000; ++i) {
        auto signed_value = randomUniform<int8_t>(-5, 9, engine);
        EXPECT_GE(signed_value, -5);
        EXPECT_LE(signed_value, 9);

        auto unsigned_value = randomUniform<uint8_t>(2, 7, engine);
        EXPECT_GE(unsigned_value, 2);
        EXPECT_LE(unsigned_value, 7);
    }
    EXPECT_FALSE(randomUniform<bool>(false, false, engine));
    EXPECT_TRUE(randomUniform<bool>(true, true, engine));
}

TEST(RandomTest, RejectsInvalidBounds) {
    std::mt19937_64 engine(42);
    EXPECT_THROW(randomIndex(0, engine), std::invalid_argument);
    EXPECT_THROW(randomUniform(2, 1, engine), std::invalid_argument);
}

TEST(RandomTest, CoprimeStepMatchesSingleUnbiasedCandidate) {
    std::mt19937_64 actual(42);
    std::mt19937_64 expected(42);
    for (size_t i = 0; i < 1000; ++i) {
        const size_t candidate = 1 + randomIndex(29, expected);
        const auto step = tryRandomCoprimeStep(30, actual, 1);
        if (std::gcd(candidate, size_t{30}) == 1) {
            ASSERT_TRUE(step);
            EXPECT_EQ(*step, candidate);
        } else {
            EXPECT_FALSE(step);
        }
        EXPECT_EQ(actual, expected);
    }
}

TEST(RandomTest, CoprimeStepsCoverCompositePrimeAndLargeBounds) {
    std::mt19937_64 engine(43);
    for (const size_t size :
         {size_t{2}, size_t{127}, size_t{1000}, size_t{1001}, size_t{10000},
          std::numeric_limits<size_t>::max()}) {
        SCOPED_TRACE(size);
        for (size_t i = 0; i < 100; ++i) {
            const auto step = tryRandomCoprimeStep(size, engine);
            if (step) {
                EXPECT_GT(*step, 0u);
                EXPECT_LT(*step, size);
                EXPECT_EQ(std::gcd(*step, size), 1u);
            }
        }
    }
}

namespace {
// A power-of-two engine range avoids bit-assembly rejection in libc++.
// Both libc++ and libstdc++ map this draw to step 2, not coprime to 4.
struct NonCoprimeStepEngine {
    using result_type = uint64_t;
    static constexpr result_type min() { return 0; }
    static constexpr result_type max() { return 3; }
    result_type operator()() {
        ++calls;
        return 1;
    }
    size_t calls{0};
};
}  // namespace

TEST(RandomTest, CoprimeStepReportsExhaustionWithoutInventingASample) {
    NonCoprimeStepEngine engine;
    EXPECT_FALSE(tryRandomCoprimeStep(4, engine, 3));
    EXPECT_EQ(engine.calls, 3u);
    EXPECT_FALSE(tryRandomCoprimeStep(4, engine, 0));
    EXPECT_EQ(engine.calls, 3u);
    EXPECT_THROW(tryRandomCoprimeStep(0, engine), std::invalid_argument);
    EXPECT_THROW(tryRandomCoprimeStep(1, engine), std::invalid_argument);
}
