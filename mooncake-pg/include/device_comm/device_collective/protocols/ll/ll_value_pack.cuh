#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_VALUE_PACK_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_VALUE_PACK_CUH

#include <cstring>

#include "device_comm/device_primitives/value_primitives.cuh"

namespace mooncake {

namespace ll_value_detail {

// Lane-wise reduction over the CUDA packed 2x16-bit pair types only.
template <ReduceOp Op, typename Pair>
__device__ __forceinline__ Pair reduce16x2(Pair left, Pair right) {
    static_assert(std::is_same_v<Pair, __half2> ||
                  std::is_same_v<Pair, __nv_bfloat162>);
    if constexpr (Op == ReduceOp::Sum) {
        return __hadd2(left, right);
    } else if constexpr (Op == ReduceOp::Product) {
        return __hmul2(left, right);
    } else if constexpr (Op == ReduceOp::Min) {
        return __hmin2(left, right);
    } else {
        static_assert(Op == ReduceOp::Max);
        return __hmax2(left, right);
    }
}

}  // namespace ll_value_detail

// A logical pack holds kValueCount values and occupies kPayloadWords 32-bit
// LL payloads.
template <typename T, typename Enable = void>
struct LLValuePack {
    static_assert(sizeof(T) == sizeof(uint32_t));
    using Value = uint32_t;
    static constexpr uint32_t kValueCount = 1;
    static constexpr uint32_t kPayloadWords = 1;

    [[nodiscard]] __device__ __forceinline__ static uint32_t load(
        const T* values, uint64_t, uint64_t pack_index) {
        uint32_t bits;
        memcpy(&bits, values + pack_index * kValueCount, sizeof(bits));
        return bits;
    }

    __device__ __forceinline__ static void store(T* values, uint64_t,
                                                 uint64_t pack_index,
                                                 uint32_t bits) {
        memcpy(values + pack_index * kValueCount, &bits, sizeof(bits));
    }

    template <ReduceOp Op>
    __device__ __forceinline__ static uint32_t reduce(uint32_t left,
                                                      uint32_t right) {
        if constexpr (std::is_same_v<T, float>) {
            return __float_as_uint(DeviceReductionTraits<float, Op>::apply(
                __uint_as_float(left), __uint_as_float(right)));
        } else {
            static_assert(std::is_same_v<T, int32_t>);
            return static_cast<uint32_t>(
                DeviceReductionTraits<int32_t, Op>::apply(
                    static_cast<int32_t>(left), static_cast<int32_t>(right)));
        }
    }
};

template <typename T>
struct LLValuePack<T, std::enable_if_t<std::is_integral_v<T> &&
                                       (sizeof(T) < sizeof(uint32_t))>> {
    using Value = uint32_t;
    static constexpr uint32_t kValueCount = sizeof(uint32_t) / sizeof(T);
    static constexpr uint32_t kPayloadWords = 1;
    static constexpr uint32_t kValueBits = sizeof(T) * 8;
    static constexpr uint32_t kValueMask = (uint32_t{1} << kValueBits) - 1;

    [[nodiscard]] __device__ __forceinline__ static Value load(
        const T* values, uint64_t count, uint64_t pack_index) {
        const uint64_t first = pack_index * kValueCount;
        const auto* input = values + first;
        if (count - first >= kValueCount &&
            (reinterpret_cast<uintptr_t>(input) & 3) == 0) {
            Value packed;
            memcpy(&packed, input, sizeof(packed));
            return packed;
        }

        Value packed = 0;
#pragma unroll
        for (uint32_t lane = 0; lane < kValueCount && first + lane < count;
             ++lane) {
            uint32_t bits = 0;
            memcpy(&bits, input + lane, sizeof(T));
            packed |= bits << (lane * kValueBits);
        }
        return packed;
    }

    __device__ __forceinline__ static void store(T* values, uint64_t count,
                                                 uint64_t pack_index,
                                                 Value packed) {
        const uint64_t first = pack_index * kValueCount;
        auto* output = values + first;
        if (count - first >= kValueCount &&
            (reinterpret_cast<uintptr_t>(output) & 3) == 0) {
            memcpy(output, &packed, sizeof(packed));
            return;
        }

#pragma unroll
        for (uint32_t lane = 0; lane < kValueCount && first + lane < count;
             ++lane) {
            const uint32_t bits = packed >> (lane * kValueBits);
            memcpy(output + lane, &bits, sizeof(T));
        }
    }

    template <ReduceOp Op>
    [[nodiscard]] __device__ __forceinline__ static Value reduce(Value left,
                                                                 Value right) {
        Value packed = 0;
#pragma unroll
        for (uint32_t lane = 0; lane < kValueCount; ++lane) {
            const uint32_t left_bits =
                (left >> (lane * kValueBits)) & kValueMask;
            const uint32_t right_bits =
                (right >> (lane * kValueBits)) & kValueMask;
            T a, b;
            memcpy(&a, &left_bits, sizeof(T));
            memcpy(&b, &right_bits, sizeof(T));
            const T result = DeviceReductionTraits<T, Op>::apply(a, b);
            uint32_t bits = 0;
            memcpy(&bits, &result, sizeof(T));
            packed |= bits << (lane * kValueBits);
        }
        return packed;
    }
};

// A 64-bit value occupies two consecutive 32-bit LL payloads.
template <typename T>
struct LLValuePack<T, std::enable_if_t<sizeof(T) == sizeof(uint64_t)>> {
    using Value = uint64_t;
    static constexpr uint32_t kValueCount = 1;
    static constexpr uint32_t kPayloadWords = 2;

    [[nodiscard]] __device__ __forceinline__ static Value load(
        const T* values, uint64_t, uint64_t pack_index) {
        Value bits;
        memcpy(&bits, values + pack_index, sizeof(bits));
        return bits;
    }

    __device__ __forceinline__ static void store(T* values, uint64_t,
                                                 uint64_t pack_index,
                                                 Value bits) {
        memcpy(values + pack_index, &bits, sizeof(bits));
    }

    template <ReduceOp Op>
    [[nodiscard]] __device__ __forceinline__ static Value reduce(Value left,
                                                                 Value right) {
        T a, b;
        memcpy(&a, &left, sizeof(T));
        memcpy(&b, &right, sizeof(T));
        const T result = DeviceReductionTraits<T, Op>::apply(a, b);
        Value bits;
        memcpy(&bits, &result, sizeof(bits));
        return bits;
    }
};

template <>
struct LLValuePack<__half> {
    using Value = uint32_t;
    static constexpr uint32_t kValueCount = 2;
    static constexpr uint32_t kPayloadWords = 1;

    [[nodiscard]] __device__ __forceinline__ static uint32_t load(
        const __half* values, uint64_t count, uint64_t pack_index) {
        const uint64_t element = pack_index * kValueCount;
        const auto* input = values + element;
        const uint64_t remaining = count - element;
        if (remaining >= 2 && (reinterpret_cast<uintptr_t>(input) & 3) == 0) {
            uint32_t bits;
            memcpy(&bits, input, sizeof(bits));
            return bits;
        }
        return uint32_t{__half_as_ushort(input[0])} |
               (remaining >= 2 ? uint32_t{__half_as_ushort(input[1])} << 16
                               : 0);
    }

    __device__ __forceinline__ static void store(__half* values, uint64_t count,
                                                 uint64_t pack_index,
                                                 uint32_t bits) {
        const uint64_t element = pack_index * kValueCount;
        auto* output = values + element;
        const uint64_t remaining = count - element;
        if (remaining >= 2 && (reinterpret_cast<uintptr_t>(output) & 3) == 0) {
            memcpy(output, &bits, sizeof(bits));
        } else {
            output[0] = __ushort_as_half(static_cast<uint16_t>(bits));
            if (remaining >= 2) output[1] = __ushort_as_half(bits >> 16);
        }
    }

    template <ReduceOp Op>
    __device__ __forceinline__ static uint32_t reduce(uint32_t left,
                                                      uint32_t right) {
        const auto a = __halves2half2(__ushort_as_half(left),
                                      __ushort_as_half(left >> 16));
        const auto b = __halves2half2(__ushort_as_half(right),
                                      __ushort_as_half(right >> 16));
        const auto reduced = ll_value_detail::reduce16x2<Op>(a, b);
        return uint32_t{__half_as_ushort(__low2half(reduced))} |
               (uint32_t{__half_as_ushort(__high2half(reduced))} << 16);
    }
};

template <>
struct LLValuePack<__nv_bfloat16> {
    using Value = uint32_t;
    static constexpr uint32_t kValueCount = 2;
    static constexpr uint32_t kPayloadWords = 1;

    [[nodiscard]] __device__ __forceinline__ static uint32_t load(
        const __nv_bfloat16* values, uint64_t count, uint64_t pack_index) {
        const uint64_t element = pack_index * kValueCount;
        const auto* input = values + element;
        const uint64_t remaining = count - element;
        if (remaining >= 2 && (reinterpret_cast<uintptr_t>(input) & 3) == 0) {
            uint32_t bits;
            memcpy(&bits, input, sizeof(bits));
            return bits;
        }
        return uint32_t{__bfloat16_as_ushort(input[0])} |
               (remaining >= 2 ? uint32_t{__bfloat16_as_ushort(input[1])} << 16
                               : 0);
    }

    __device__ __forceinline__ static void store(__nv_bfloat16* values,
                                                 uint64_t count,
                                                 uint64_t pack_index,
                                                 uint32_t bits) {
        const uint64_t element = pack_index * kValueCount;
        auto* output = values + element;
        const uint64_t remaining = count - element;
        if (remaining >= 2 && (reinterpret_cast<uintptr_t>(output) & 3) == 0) {
            memcpy(output, &bits, sizeof(bits));
        } else {
            output[0] = __ushort_as_bfloat16(static_cast<uint16_t>(bits));
            if (remaining >= 2) output[1] = __ushort_as_bfloat16(bits >> 16);
        }
    }

    template <ReduceOp Op>
    __device__ __forceinline__ static uint32_t reduce(uint32_t left,
                                                      uint32_t right) {
        const auto a = __halves2bfloat162(__ushort_as_bfloat16(left),
                                          __ushort_as_bfloat16(left >> 16));
        const auto b = __halves2bfloat162(__ushort_as_bfloat16(right),
                                          __ushort_as_bfloat16(right >> 16));
        const auto reduced = ll_value_detail::reduce16x2<Op>(a, b);
        return uint32_t{__bfloat16_as_ushort(__low2bfloat16(reduced))} |
               (uint32_t{__bfloat16_as_ushort(__high2bfloat16(reduced))} << 16);
    }
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_VALUE_PACK_CUH
