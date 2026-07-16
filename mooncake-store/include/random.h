#pragma once

#include <cstddef>
#include <random>
#include <stdexcept>
#include <type_traits>

namespace mooncake {

using RandomEngine = std::mt19937_64;

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

template <typename Generator>
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

template <typename Integer, typename Generator>
Integer randomUniform(Integer lower_bound, Integer upper_bound,
                      Generator& generator) {
    static_assert(std::is_integral_v<Integer>,
                  "randomUniform requires an integral result type");
    if (lower_bound > upper_bound) {
        throw std::invalid_argument(
            "randomUniform lower bound must not exceed upper bound");
    }
    std::uniform_int_distribution<Integer> distribution(lower_bound,
                                                        upper_bound);
    return distribution(generator);
}

// Returns an unbiased random integer in [lower_bound, upper_bound].
template <typename Integer>
Integer randomUniform(Integer lower_bound, Integer upper_bound) {
    return randomUniform(lower_bound, upper_bound, threadLocalRandomEngine());
}

}  // namespace mooncake
