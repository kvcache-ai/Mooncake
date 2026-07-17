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
    // uniform_int_distribution only accepts the standard signed and unsigned
    // integer types. Map other integral types, such as char and char16_t, to a
    // permitted type with the same signedness and sufficient width.
    static_assert(
        sizeof(Integer) <= sizeof(long long),
        "randomUniform does not support integers wider than long long");
    using DistributionInteger = std::conditional_t<
        std::is_signed_v<Integer>,
        std::conditional_t<sizeof(Integer) <= sizeof(int), int,
                           std::conditional_t<sizeof(Integer) <= sizeof(long),
                                              long, long long>>,
        std::conditional_t<
            sizeof(Integer) <= sizeof(unsigned int), unsigned int,
            std::conditional_t<sizeof(Integer) <= sizeof(unsigned long),
                               unsigned long, unsigned long long>>>;
    std::uniform_int_distribution<DistributionInteger> distribution(
        static_cast<DistributionInteger>(lower_bound),
        static_cast<DistributionInteger>(upper_bound));
    return static_cast<Integer>(distribution(generator));
}

// Returns an unbiased random integer in [lower_bound, upper_bound].
template <typename Integer>
Integer randomUniform(Integer lower_bound, Integer upper_bound) {
    return randomUniform(lower_bound, upper_bound, threadLocalRandomEngine());
}

}  // namespace mooncake
