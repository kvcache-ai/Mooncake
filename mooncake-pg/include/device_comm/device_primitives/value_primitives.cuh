#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_PRIMITIVES_VALUE_PRIMITIVES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_PRIMITIVES_VALUE_PRIMITIVES_CUH

#include <cstdint>
#include <math.h>
#include <type_traits>

#include <cooperative_groups.h>
#include <cuda_bf16.h>
#include <cuda_fp16.h>

#include "common_types.h"

namespace mooncake {

namespace device_reduction_detail {

// Integer collective reductions use modulo-2^N arithmetic. Perform the
// operation on the corresponding unsigned type because signed overflow is
// undefined in C++. Explicitly widen sub-32-bit operands to uint32_t so the
// usual integer promotions cannot move their arithmetic into signed int;
// casting back to Unsigned truncates the result to T's original width.
template <typename T>
__device__ __forceinline__ T wrappingAdd(T left, T right) {
    static_assert(std::is_integral_v<T> && !std::is_same_v<T, bool>,
                  "wrappingAdd requires a non-bool integer type");
    using Unsigned = std::make_unsigned_t<T>;
    using WideUnsigned =
        std::conditional_t<(sizeof(Unsigned) < sizeof(uint32_t)), uint32_t,
                           Unsigned>;
    const auto result = static_cast<WideUnsigned>(static_cast<Unsigned>(left)) +
                        static_cast<WideUnsigned>(static_cast<Unsigned>(right));
    return static_cast<T>(static_cast<Unsigned>(result));
}

template <typename T>
__device__ __forceinline__ T wrappingMultiply(T left, T right) {
    static_assert(std::is_integral_v<T> && !std::is_same_v<T, bool>,
                  "wrappingMultiply requires a non-bool integer type");
    using Unsigned = std::make_unsigned_t<T>;
    using WideUnsigned =
        std::conditional_t<(sizeof(Unsigned) < sizeof(uint32_t)), uint32_t,
                           Unsigned>;
    const auto result = static_cast<WideUnsigned>(static_cast<Unsigned>(left)) *
                        static_cast<WideUnsigned>(static_cast<Unsigned>(right));
    return static_cast<T>(static_cast<Unsigned>(result));
}

}  // namespace device_reduction_detail

template <typename T, ReduceOp Op>
struct DeviceReductionTraits;

template <typename T>
struct DeviceReductionTraits<T, ReduceOp::Sum> {
    __device__ __forceinline__ static T apply(T left, T right) {
        if constexpr (std::is_same_v<T, bool>) {
            return left || right;
        } else if constexpr (std::is_integral_v<T>) {
            return device_reduction_detail::wrappingAdd(left, right);
        } else {
            return left + right;
        }
    }
};

template <typename T>
struct DeviceReductionTraits<T, ReduceOp::Product> {
    __device__ __forceinline__ static T apply(T left, T right) {
        if constexpr (std::is_same_v<T, bool>) {
            return left && right;
        } else if constexpr (std::is_integral_v<T>) {
            return device_reduction_detail::wrappingMultiply(left, right);
        } else {
            return left * right;
        }
    }
};

template <typename T>
struct DeviceReductionTraits<T, ReduceOp::Min> {
    __device__ __forceinline__ static T apply(T left, T right) {
        return left < right ? left : right;
    }
};

template <typename T>
struct DeviceReductionTraits<T, ReduceOp::Max> {
    __device__ __forceinline__ static T apply(T left, T right) {
        return right < left ? left : right;
    }
};

template <>
struct DeviceReductionTraits<float, ReduceOp::Min> {
    __device__ __forceinline__ static float apply(float left, float right) {
        return fminf(left, right);
    }
};

template <>
struct DeviceReductionTraits<float, ReduceOp::Max> {
    __device__ __forceinline__ static float apply(float left, float right) {
        return fmaxf(left, right);
    }
};

template <>
struct DeviceReductionTraits<double, ReduceOp::Min> {
    __device__ __forceinline__ static double apply(double left, double right) {
        return fmin(left, right);
    }
};

template <>
struct DeviceReductionTraits<double, ReduceOp::Max> {
    __device__ __forceinline__ static double apply(double left, double right) {
        return fmax(left, right);
    }
};

template <>
struct DeviceReductionTraits<bool, ReduceOp::Min> {
    __device__ __forceinline__ static bool apply(bool left, bool right) {
        return left && right;
    }
};

template <>
struct DeviceReductionTraits<bool, ReduceOp::Max> {
    __device__ __forceinline__ static bool apply(bool left, bool right) {
        return left || right;
    }
};

template <>
struct DeviceReductionTraits<__half, ReduceOp::Sum> {
    __device__ __forceinline__ static __half apply(__half left, __half right) {
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 530 && __CUDA_ARCH__ != 610
        return __hadd(left, right);
#else
        return __float2half_rn(__half2float(left) + __half2float(right));
#endif
    }
};

template <>
struct DeviceReductionTraits<__half, ReduceOp::Product> {
    __device__ __forceinline__ static __half apply(__half left, __half right) {
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 530 && __CUDA_ARCH__ != 610
        return __hmul(left, right);
#else
        return __float2half_rn(__half2float(left) * __half2float(right));
#endif
    }
};

template <>
struct DeviceReductionTraits<__half, ReduceOp::Min> {
    __device__ __forceinline__ static __half apply(__half left, __half right) {
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 800
        return __hmin(left, right);
#else
        return __float2half_rn(fminf(__half2float(left), __half2float(right)));
#endif
    }
};

template <>
struct DeviceReductionTraits<__half, ReduceOp::Max> {
    __device__ __forceinline__ static __half apply(__half left, __half right) {
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 800
        return __hmax(left, right);
#else
        return __float2half_rn(fmaxf(__half2float(left), __half2float(right)));
#endif
    }
};

template <>
struct DeviceReductionTraits<__nv_bfloat16, ReduceOp::Sum> {
    __device__ __forceinline__ static __nv_bfloat16 apply(__nv_bfloat16 left,
                                                          __nv_bfloat16 right) {
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 800
        return __hadd(left, right);
#else
        return __float2bfloat16(__bfloat162float(left) +
                                __bfloat162float(right));
#endif
    }
};

template <>
struct DeviceReductionTraits<__nv_bfloat16, ReduceOp::Product> {
    __device__ __forceinline__ static __nv_bfloat16 apply(__nv_bfloat16 left,
                                                          __nv_bfloat16 right) {
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 800
        return __hmul(left, right);
#else
        return __float2bfloat16(__bfloat162float(left) *
                                __bfloat162float(right));
#endif
    }
};

template <>
struct DeviceReductionTraits<__nv_bfloat16, ReduceOp::Min> {
    __device__ __forceinline__ static __nv_bfloat16 apply(__nv_bfloat16 left,
                                                          __nv_bfloat16 right) {
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 800
        return __hmin(left, right);
#else
        return __float2bfloat16(
            fminf(__bfloat162float(left), __bfloat162float(right)));
#endif
    }
};

template <>
struct DeviceReductionTraits<__nv_bfloat16, ReduceOp::Max> {
    __device__ __forceinline__ static __nv_bfloat16 apply(__nv_bfloat16 left,
                                                          __nv_bfloat16 right) {
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 800
        return __hmax(left, right);
#else
        return __float2bfloat16(
            fmaxf(__bfloat162float(left), __bfloat162float(right)));
#endif
    }
};

namespace device_value_detail {

__device__ __forceinline__ uint32_t loadPackedWord(const void* address) {
    uint32_t word;
    asm volatile("ld.global.b32 %0, [%1];" : "=r"(word) : "l"(address));
    return word;
}

__device__ __forceinline__ void storePackedWord(void* address, uint32_t word) {
    asm volatile("st.global.b32 [%0], %1;"
                 :
                 : "l"(address), "r"(word)
                 : "memory");
}

template <ReduceOp Op, typename Pair>
__device__ __forceinline__ Pair reducePair(Pair left, Pair right) {
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

}  // namespace device_value_detail

// Load/store one 32-bit word of values, independently of the operation that
// consumes it. count is the number of values remaining and must be positive.
// Half-width specializations also handle unaligned pairs and an odd tail.
template <typename T>
struct ValueWord {
    static_assert(sizeof(T) == sizeof(uint32_t));
    static constexpr uint32_t kValueCount = 1;

    __device__ __forceinline__ static uint32_t load(const T* input, uint64_t) {
        return device_value_detail::loadPackedWord(input);
    }
    __device__ __forceinline__ static void store(T* output, uint64_t,
                                                 uint32_t bits) {
        device_value_detail::storePackedWord(output, bits);
    }
};

template <>
struct ValueWord<__half> {
    static constexpr uint32_t kValueCount = 2;

    __device__ __forceinline__ static uint32_t load(const __half* input,
                                                    uint64_t count) {
        if (count >= 2 && (reinterpret_cast<uintptr_t>(input) & 3) == 0) {
            return device_value_detail::loadPackedWord(input);
        }
        return uint32_t{__half_as_ushort(input[0])} |
               (count >= 2 ? uint32_t{__half_as_ushort(input[1])} << 16 : 0);
    }
    __device__ __forceinline__ static void store(__half* output, uint64_t count,
                                                 uint32_t bits) {
        if (count >= 2 && (reinterpret_cast<uintptr_t>(output) & 3) == 0) {
            device_value_detail::storePackedWord(output, bits);
        } else {
            output[0] = __ushort_as_half(static_cast<uint16_t>(bits));
            if (count >= 2) output[1] = __ushort_as_half(bits >> 16);
        }
    }
};

template <>
struct ValueWord<__nv_bfloat16> {
    static constexpr uint32_t kValueCount = 2;

    __device__ __forceinline__ static uint32_t load(const __nv_bfloat16* input,
                                                    uint64_t count) {
        if (count >= 2 && (reinterpret_cast<uintptr_t>(input) & 3) == 0) {
            return device_value_detail::loadPackedWord(input);
        }
        return uint32_t{__bfloat16_as_ushort(input[0])} |
               (count >= 2 ? uint32_t{__bfloat16_as_ushort(input[1])} << 16
                           : 0);
    }
    __device__ __forceinline__ static void store(__nv_bfloat16* output,
                                                 uint64_t count,
                                                 uint32_t bits) {
        if (count >= 2 && (reinterpret_cast<uintptr_t>(output) & 3) == 0) {
            device_value_detail::storePackedWord(output, bits);
        } else {
            output[0] = __ushort_as_bfloat16(static_cast<uint16_t>(bits));
            if (count >= 2) output[1] = __ushort_as_bfloat16(bits >> 16);
        }
    }
};

// A word-indexed source for communication primitives. The source owns the
// element type and tail handling; the consumer only sees 32-bit words.
template <typename T>
struct ValueWordSource {
    const T* values;
    uint64_t count;

    [[nodiscard]] __device__ __forceinline__ uint32_t
    operator()(uint64_t word) const {
        const uint64_t element = word * ValueWord<T>::kValueCount;
        return ValueWord<T>::load(values + element, count - element);
    }
};

// Reduce the values packed into a word. Packing, communication and output
// placement do not depend on the operation.
template <typename T, ReduceOp Op>
struct PackedReduction;

template <ReduceOp Op>
struct PackedReduction<float, Op> {
    __device__ __forceinline__ uint32_t operator()(uint32_t left,
                                                   uint32_t right) const {
        return __float_as_uint(DeviceReductionTraits<float, Op>::apply(
            __uint_as_float(left), __uint_as_float(right)));
    }
};

template <ReduceOp Op>
struct PackedReduction<int32_t, Op> {
    __device__ __forceinline__ uint32_t operator()(uint32_t left,
                                                   uint32_t right) const {
        return static_cast<uint32_t>(DeviceReductionTraits<int32_t, Op>::apply(
            static_cast<int32_t>(left), static_cast<int32_t>(right)));
    }
};

template <ReduceOp Op>
struct PackedReduction<__half, Op> {
    __device__ __forceinline__ uint32_t operator()(uint32_t left,
                                                   uint32_t right) const {
        const auto a = __halves2half2(__ushort_as_half(left),
                                      __ushort_as_half(left >> 16));
        const auto b = __halves2half2(__ushort_as_half(right),
                                      __ushort_as_half(right >> 16));
        const auto reduced = device_value_detail::reducePair<Op>(a, b);
        return uint32_t{__half_as_ushort(__low2half(reduced))} |
               (uint32_t{__half_as_ushort(__high2half(reduced))} << 16);
    }
};

template <ReduceOp Op>
struct PackedReduction<__nv_bfloat16, Op> {
    __device__ __forceinline__ uint32_t operator()(uint32_t left,
                                                   uint32_t right) const {
        const auto a = __halves2bfloat162(__ushort_as_bfloat16(left),
                                          __ushort_as_bfloat16(left >> 16));
        const auto b = __halves2bfloat162(__ushort_as_bfloat16(right),
                                          __ushort_as_bfloat16(right >> 16));
        const auto reduced = device_value_detail::reducePair<Op>(a, b);
        return uint32_t{__bfloat16_as_ushort(__low2bfloat16(reduced))} |
               (uint32_t{__bfloat16_as_ushort(__high2bfloat16(reduced))} << 16);
    }
};

// A 16-byte-aligned unit used by the CTA copy/reduction loops. Each pack holds
// as many complete T values as fit in 16 bytes.
template <typename T>
struct alignas(16) ValuePack {
    static_assert(16 % sizeof(T) == 0);
    static constexpr uint64_t kValueCount = 16 / sizeof(T);

    [[nodiscard]] __device__ __forceinline__ static ValuePack* fromValues(
        T* values) {
        return reinterpret_cast<ValuePack*>(values);
    }

    [[nodiscard]] __device__ __forceinline__ static const ValuePack* fromValues(
        const T* values) {
        return reinterpret_cast<const ValuePack*>(values);
    }

    T values[kValueCount];
};

// Return the number of complete packs only when every participating buffer is
// 16-byte aligned. OR-ing the addresses preserves any non-zero alignment bit,
// so one mask test checks all pointers. For a power-of-two alignment A, an
// address is A-byte aligned exactly when address & (A - 1) is zero.
template <typename T, typename... Remaining>
[[nodiscard]] __device__ __forceinline__ uint64_t packCountIfAligned(
    uint64_t value_count, const T* first, Remaining... remaining) {
    static_assert(sizeof(ValuePack<T>) == 16);
    const uintptr_t combined = (reinterpret_cast<uintptr_t>(first) | ... |
                                reinterpret_cast<uintptr_t>(remaining));
    if ((combined & (alignof(ValuePack<T>) - 1)) != 0) {
        return 0;
    }
    return value_count / ValuePack<T>::kValueCount;
}

template <typename T, ReduceOp Op>
struct DeviceValuePackReductionTraits {
    [[nodiscard]] __device__ __forceinline__ static ValuePack<T> apply(
        ValuePack<T> left, ValuePack<T> right) {
        ValuePack<T> result;
#pragma unroll
        for (uint64_t item = 0; item < ValuePack<T>::kValueCount; ++item) {
            result.values[item] = DeviceReductionTraits<T, Op>::apply(
                left.values[item], right.values[item]);
        }
        return result;
    }
};

// Scope determines which threads share the supplied range. Both scopes use
// the same vectorized copy and scalar tail; the caller owns synchronization.
// By default, threads in the calling CTA cooperatively copy the supplied range.
template <typename Scope = SingleCta, typename T, typename... Destinations>
__device__ __forceinline__ void copyValuesTo(
    const T* source, uint64_t count, cooperative_groups::thread_block block,
    Destinations... destinations) {
    static_assert(std::is_same_v<Scope, SingleCta> ||
                  std::is_same_v<Scope, MultiCta>);
    static_assert(sizeof...(Destinations) != 0);

    uint64_t thread_index = block.thread_rank();
    uint64_t thread_count = block.size();
    if constexpr (std::is_same_v<Scope, MultiCta>) {
        thread_index += uint64_t{blockIdx.x} * thread_count;
        thread_count *= gridDim.x;
    }
    const uint64_t pack_count =
        packCountIfAligned(count, source, destinations...);
    const auto* source_packs = ValuePack<T>::fromValues(source);
    for (uint64_t index = thread_index; index < pack_count;
         index += thread_count) {
        const ValuePack<T> value = source_packs[index];
        ((ValuePack<T>::fromValues(destinations)[index] = value), ...);
    }

    const uint64_t tail_begin = pack_count * ValuePack<T>::kValueCount;
    for (uint64_t index = tail_begin + thread_index; index < count;
         index += thread_count) {
        const T value = source[index];
        ((destinations[index] = value), ...);
    }
}

template <typename T, ReduceOp Op, typename... Destinations>
__device__ __forceinline__ void reduceValuesTo(
    const T* local_values, const T* received_values, uint64_t count,
    cooperative_groups::thread_block block, Destinations... destinations) {
    static_assert(sizeof...(Destinations) != 0);

    const uint64_t pack_count = packCountIfAligned(
        count, local_values, received_values, destinations...);
    const auto* local_packs = ValuePack<T>::fromValues(local_values);
    const auto* received_packs = ValuePack<T>::fromValues(received_values);
    for (uint64_t index = block.thread_rank(); index < pack_count;
         index += block.size()) {
        const ValuePack<T> local = local_packs[index];
        const ValuePack<T> received = received_packs[index];
        const ValuePack<T> result =
            DeviceValuePackReductionTraits<T, Op>::apply(local, received);
        ((ValuePack<T>::fromValues(destinations)[index] = result), ...);
    }

    const uint64_t tail_begin = pack_count * ValuePack<T>::kValueCount;
    for (uint64_t index = tail_begin + block.thread_rank(); index < count;
         index += block.size()) {
        const T result = DeviceReductionTraits<T, Op>::apply(
            local_values[index], received_values[index]);
        ((destinations[index] = result), ...);
    }
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_PRIMITIVES_VALUE_PRIMITIVES_CUH
