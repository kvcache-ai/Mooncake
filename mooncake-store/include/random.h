#pragma once

#include <concepts>
#include <cstddef>
#include <numeric>
#include <optional>
#include <random>
#include <stdexcept>

namespace mooncake {

using RandomEngine = std::mt19937_64;

namespace detail {

template <typename Result, std::integral DistributionInteger,
          std::uniform_random_bit_generator Generator>
Result sampleUniform(Result lower_bound, Result upper_bound,
                     Generator& generator) {
    std::uniform_int_distribution<DistributionInteger> distribution(
        static_cast<DistributionInteger>(lower_bound),
        static_cast<DistributionInteger>(upper_bound));
    return static_cast<Result>(distribution(generator));
}

}  // namespace detail

// Returns the pseudo-random engine shared by random helpers on this thread.
// The engine is seeded once per thread and is not safe to use from another
// thread.
inline RandomEngine& threadLocalRandomEngine() {
    thread_local RandomEngine engine = [] {
        std::random_device device;
        std::seed_seq seed{device(), device(), device(), device(),
                           device(), device(), device(), device()};
        return RandomEngine(seed);
    }();
    return engine;
}

template <std::uniform_random_bit_generator Generator>
size_t randomIndex(size_t upper_bound, Generator& generator) {
    if (upper_bound == 0) {
        throw std::invalid_argument("randomIndex upper bound must be positive");
    }
    std::uniform_int_distribution<size_t> distribution(0, upper_bound - 1);
    return distribution(generator);
}

// Returns an unbiased random index in [0, upper_bound).
inline size_t randomIndex(size_t upper_bound) {
    return randomIndex(upper_bound, threadLocalRandomEngine());
}

// Samples a step in [1, size) that visits a cyclic index range without
// repetition. Conditional on success, all coprime steps are equally likely.
// The bound counts candidate draws, not raw URBG calls made by a distribution.
// Exhaustion is explicit so callers can choose their own traversal fallback.
template <std::uniform_random_bit_generator Generator>
std::optional<size_t> tryRandomCoprimeStep(size_t size, Generator& generator,
                                           size_t max_candidates = 16) {
    if (size < 2) {
        throw std::invalid_argument(
            "coprime step requires at least two indices");
    }
    for (size_t i = 0; i < max_candidates; ++i) {
        const size_t step = 1 + randomIndex(size - 1, generator);
        if (std::gcd(step, size) == 1) {
            return step;
        }
    }
    return std::nullopt;
}

inline std::optional<size_t> tryRandomCoprimeStep(size_t size,
                                                  size_t max_candidates = 16) {
    return tryRandomCoprimeStep(size, threadLocalRandomEngine(),
                                max_candidates);
}

template <std::integral Integer, std::uniform_random_bit_generator Generator>
Integer randomUniform(Integer lower_bound, Integer upper_bound,
                      Generator& generator) {
    if (lower_bound > upper_bound) {
        throw std::invalid_argument(
            "randomUniform lower bound must not exceed upper bound");
    }
    if constexpr (std::signed_integral<Integer>) {
        return detail::sampleUniform<Integer, long long>(
            lower_bound, upper_bound, generator);
    } else {
        return detail::sampleUniform<Integer, unsigned long long>(
            lower_bound, upper_bound, generator);
    }
}

// Returns an unbiased random integer in [lower_bound, upper_bound].
template <std::integral Integer>
Integer randomUniform(Integer lower_bound, Integer upper_bound) {
    return randomUniform(lower_bound, upper_bound, threadLocalRandomEngine());
}

}  // namespace mooncake
