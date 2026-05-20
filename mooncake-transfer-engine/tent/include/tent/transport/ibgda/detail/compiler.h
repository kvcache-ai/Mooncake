#ifndef _MXA_COMPILER_H
#define _MXA_COMPILER_H

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __GNUC__
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif

#define mxa_assert(x) assert(x)

#ifdef __cplusplus
#define MXA_BEGIN_C_DECLS extern "C" {
#define MXA_END_C_DECLS }
#else
#define MXA_BEGIN_C_DECLS
#define MXA_END_C_DECLS
#endif

#endif