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

    [[nodiscard]] __device__ __forceinline__ static const ValuePack*
    fromValues(const T* values) {
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
    const uintptr_t combined =
        (reinterpret_cast<uintptr_t>(first) | ... |
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

template <typename T, typename... Destinations>
__device__ __forceinline__ void copyValuesTo(
    const T* source, uint64_t count, cooperative_groups::thread_block block,
    Destinations... destinations) {
    static_assert(sizeof...(Destinations) != 0);

    const uint64_t pack_count =
        packCountIfAligned(count, source, destinations...);
    const auto* source_packs = ValuePack<T>::fromValues(source);
    for (uint64_t index = block.thread_rank(); index < pack_count;
         index += block.size()) {
        const ValuePack<T> value = source_packs[index];
        ((ValuePack<T>::fromValues(destinations)[index] = value), ...);
    }

    const uint64_t tail_begin = pack_count * ValuePack<T>::kValueCount;
    for (uint64_t index = tail_begin + block.thread_rank(); index < count;
         index += block.size()) {
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
