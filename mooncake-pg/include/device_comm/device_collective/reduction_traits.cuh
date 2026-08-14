#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_REDUCTION_TRAITS_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_REDUCTION_TRAITS_CUH

#include <cstdint>
#include <math.h>
#include <type_traits>

#include <cuda_bf16.h>
#include <cuda_fp16.h>

#include "common_types.h"

namespace mooncake {

namespace device_reduction_detail {

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

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_REDUCTION_TRAITS_CUH
