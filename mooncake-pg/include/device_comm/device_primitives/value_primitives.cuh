#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_PRIMITIVES_VALUE_PRIMITIVES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_PRIMITIVES_VALUE_PRIMITIVES_CUH

#include <cstdint>
#include <cstring>
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

// Apply a scalar transform to each T stored in Value, retaining Value's
// storage type.
template <typename T, typename Value, typename Transform>
[[nodiscard]] __device__ __forceinline__ Value
transformValue(Value value, Transform transform) {
    static_assert(sizeof(Value) % sizeof(T) == 0);
    T values[sizeof(Value) / sizeof(T)];
    memcpy(values, &value, sizeof(value));
#pragma unroll
    for (uint32_t item = 0; item < sizeof(Value) / sizeof(T); ++item) {
        values[item] = transform(values[item]);
    }
    memcpy(&value, values, sizeof(value));
    return value;
}

template <typename T, typename Value, typename Transform>
[[nodiscard]] __device__ __forceinline__ Value
transformValue(Value left, Value right, Transform transform) {
    static_assert(sizeof(Value) % sizeof(T) == 0);
    T left_values[sizeof(Value) / sizeof(T)];
    T right_values[sizeof(Value) / sizeof(T)];
    memcpy(left_values, &left, sizeof(left));
    memcpy(right_values, &right, sizeof(right));
#pragma unroll
    for (uint32_t item = 0; item < sizeof(Value) / sizeof(T); ++item) {
        left_values[item] = transform(left_values[item], right_values[item]);
    }
    memcpy(&left, left_values, sizeof(left));
    return left;
}

}  // namespace device_reduction_detail

template <typename T, ReduceOp Op>
struct DeviceReductionTraits {
    static constexpr bool kSupported = false;
};

template <typename T>
struct DeviceReductionTraits<T, ReduceOp::Sum> {
    static constexpr bool kSupported = true;

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
    static constexpr bool kSupported = true;

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
    static constexpr bool kSupported = true;

    __device__ __forceinline__ static T apply(T left, T right) {
        return left < right ? left : right;
    }
};

template <typename T>
struct DeviceReductionTraits<T, ReduceOp::Max> {
    static constexpr bool kSupported = true;

    __device__ __forceinline__ static T apply(T left, T right) {
        return right < left ? left : right;
    }
};

template <>
struct DeviceReductionTraits<float, ReduceOp::Min> {
    static constexpr bool kSupported = true;

    __device__ __forceinline__ static float apply(float left, float right) {
        return fminf(left, right);
    }
};

template <>
struct DeviceReductionTraits<float, ReduceOp::Max> {
    static constexpr bool kSupported = true;

    __device__ __forceinline__ static float apply(float left, float right) {
        return fmaxf(left, right);
    }
};

template <>
struct DeviceReductionTraits<double, ReduceOp::Min> {
    static constexpr bool kSupported = true;

    __device__ __forceinline__ static double apply(double left, double right) {
        return fmin(left, right);
    }
};

template <>
struct DeviceReductionTraits<double, ReduceOp::Max> {
    static constexpr bool kSupported = true;

    __device__ __forceinline__ static double apply(double left, double right) {
        return fmax(left, right);
    }
};

template <>
struct DeviceReductionTraits<bool, ReduceOp::Min> {
    static constexpr bool kSupported = true;

    __device__ __forceinline__ static bool apply(bool left, bool right) {
        return left && right;
    }
};

template <>
struct DeviceReductionTraits<bool, ReduceOp::Max> {
    static constexpr bool kSupported = true;

    __device__ __forceinline__ static bool apply(bool left, bool right) {
        return left || right;
    }
};

template <>
struct DeviceReductionTraits<__half, ReduceOp::Sum> {
    static constexpr bool kSupported = true;

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
    static constexpr bool kSupported = true;

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
    static constexpr bool kSupported = true;

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
    static constexpr bool kSupported = true;

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
    static constexpr bool kSupported = true;

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
    static constexpr bool kSupported = true;

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
    static constexpr bool kSupported = true;

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
    static constexpr bool kSupported = true;

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

// Return 1 / divisor rounded to the floating-point type T.
template <typename T>
[[nodiscard]] __device__ __forceinline__ T reciprocal(uint32_t divisor) {
    if constexpr (std::is_same_v<T, __half>) {
        return __float2half_rn(1.0f / divisor);
    } else if constexpr (std::is_same_v<T, __nv_bfloat16>) {
        return __float2bfloat16(1.0f / divisor);
    } else {
        return static_cast<T>(1.0 / divisor);
    }
}

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

// These operations accept scalar T values and packs of T, including
// ValuePack<T> and LL payload words.

// Preserve every element.
struct Identity {
    template <typename Value>
    [[nodiscard]] __device__ __forceinline__ Value
    operator()(Value value) const {
        return value;
    }
};

// Multiply each T element by factor.
template <typename T>
struct Mul {
    T factor;

    template <typename Value>
    [[nodiscard]] __device__ __forceinline__ Value
    operator()(Value value) const {
        return device_reduction_detail::transformValue<T>(
            value, [factor = factor](T element) {
                return DeviceReductionTraits<T, ReduceOp::Product>::apply(
                    element, factor);
            });
    }
};

// Divide each integral T element toward zero. The divisor must be in
// [1, INT32_MAX]; keep it at least 32 bits wide so small T cannot truncate it.
template <typename T>
struct Div {
    uint32_t divisor;

    template <typename Value>
    [[nodiscard]] __device__ __forceinline__ Value
    operator()(Value value) const {
        static_assert(std::is_integral_v<T> && !std::is_same_v<T, bool>);
        using Divisor =
            std::conditional_t<std::is_signed_v<T>, int32_t, uint32_t>;
        const auto typed_divisor = static_cast<Divisor>(divisor);
        return device_reduction_detail::transformValue<T>(
            value, [typed_divisor](T element) {
                return static_cast<T>(element / typed_divisor);
            });
    }
};

// Apply Op to corresponding T elements.
template <typename T, ReduceOp Op>
struct Reduce {
    template <typename Value>
    [[nodiscard]] __device__ __forceinline__ Value
    operator()(Value left, Value right) const {
        return device_reduction_detail::transformValue<T>(
            left, right,
            [](T a, T b) { return DeviceReductionTraits<T, Op>::apply(a, b); });
    }
};

// Multiply each left T by factor, then add the corresponding right T.
template <typename T>
struct MulAdd {
    T factor;

    template <typename Value>
    [[nodiscard]] __device__ __forceinline__ Value
    operator()(Value left, Value right) const {
        return Reduce<T, ReduceOp::Sum>{}(Mul<T>{factor}(left), right);
    }
};

// Sum corresponding integral T values with modulo arithmetic, then divide
// toward zero. The divisor must be in [1, INT32_MAX].
template <typename T>
struct SumDiv {
    uint32_t divisor;

    template <typename Value>
    [[nodiscard]] __device__ __forceinline__ Value
    operator()(Value left, Value right) const {
        return Div<T>{divisor}(Reduce<T, ReduceOp::Sum>{}(left, right));
    }
};

// Apply transform to source values and write each result to every destination.
// Aligned data uses ValuePack<T>; unaligned data and tails use T. Transform
// must accept and return both forms. Identity is skipped when every destination
// equals source.
template <typename T, typename Transform, typename... Destinations>
__device__ __forceinline__ void transformValuesTo(
    const T* source, uint64_t count, cooperative_groups::thread_block block,
    Transform transform, Destinations... destinations) {
    static_assert(sizeof...(Destinations) != 0);

    if constexpr (std::is_same_v<Transform, Identity>) {
        if (((source == destinations) && ...)) return;
    }

    const uint64_t pack_count =
        packCountIfAligned(count, source, destinations...);
    const auto* source_packs = ValuePack<T>::fromValues(source);
    for (uint64_t index = block.thread_rank(); index < pack_count;
         index += block.size()) {
        const ValuePack<T> value = transform(source_packs[index]);
        ((ValuePack<T>::fromValues(destinations)[index] = value), ...);
    }

    const uint64_t tail_begin = pack_count * ValuePack<T>::kValueCount;
    for (uint64_t index = tail_begin + block.thread_rank(); index < count;
         index += block.size()) {
        const T value = transform(source[index]);
        ((destinations[index] = value), ...);
    }
}

// Apply transform(left, right) to corresponding values and write each result
// to every destination, using the same scalar and packed forms as above.
template <typename T, typename Transform, typename... Destinations>
__device__ __forceinline__ void transformValuesTo(
    const T* left_values, const T* right_values, uint64_t count,
    cooperative_groups::thread_block block, Transform transform,
    Destinations... destinations) {
    static_assert(sizeof...(Destinations) != 0);

    const uint64_t pack_count =
        packCountIfAligned(count, left_values, right_values, destinations...);
    const auto* left_packs = ValuePack<T>::fromValues(left_values);
    const auto* right_packs = ValuePack<T>::fromValues(right_values);
    for (uint64_t index = block.thread_rank(); index < pack_count;
         index += block.size()) {
        const ValuePack<T> result =
            transform(left_packs[index], right_packs[index]);
        ((ValuePack<T>::fromValues(destinations)[index] = result), ...);
    }

    const uint64_t tail_begin = pack_count * ValuePack<T>::kValueCount;
    for (uint64_t index = tail_begin + block.thread_rank(); index < count;
         index += block.size()) {
        const T result = transform(left_values[index], right_values[index]);
        ((destinations[index] = result), ...);
    }
}

template <typename T, typename... Destinations>
__device__ __forceinline__ void copyValuesTo(
    const T* source, uint64_t count, cooperative_groups::thread_block block,
    Destinations... destinations) {
    transformValuesTo(source, count, block, Identity{}, destinations...);
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_PRIMITIVES_VALUE_PRIMITIVES_CUH
