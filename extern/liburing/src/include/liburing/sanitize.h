/* SPDX-License-Identifier: MIT */
#ifndef LIBURING_SANITIZE_H
#define LIBURING_SANITIZE_H

#ifdef __cplusplus
#define LIBURING_NOEXCEPT noexcept
#else
#define LIBURING_NOEXCEPT
#endif

#ifdef __cplusplus
extern "C" {
#endif

struct io_uring;
struct iovec;

#if defined(CONFIG_USE_SANITIZER)
void liburing_sanitize_ring(struct io_uring *ring) LIBURING_NOEXCEPT;
void liburing_sanitize_address(const void *addr) LIBURING_NOEXCEPT;
void liburing_sanitize_region(const void *addr, unsigned int len)
	LIBURING_NOEXCEPT;
void liburing_sanitize_iovecs(const struct iovec *iovecs, unsigned nr)
	LIBURING_NOEXCEPT;
#else
#define __maybe_unused	__attribute__((__unused__))
static inline void liburing_sanitize_ring(struct io_uring __maybe_unused *ring)
   LIBURING_NOEXCEPT
{
}
static inline void liburing_sanitize_address(const void __maybe_unused *addr)
   LIBURING_NOEXCEPT
{
}
static inline void liburing_sanitize_region(const void __maybe_unused *addr,
					    unsigned int __maybe_unused len)
   LIBURING_NOEXCEPT
{
}
static inline void liburing_sanitize_iovecs(const struct iovec __maybe_unused *iovecs,
					    unsigned __maybe_unused nr)
   LIBURING_NOEXCEPT
{
}
#endif

#ifdef __cplusplus
}
#endif

#endif
