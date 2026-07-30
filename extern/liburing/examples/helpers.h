/* SPDX-License-Identifier: MIT */
#ifndef LIBURING_EX_HELPERS_H
#define LIBURING_EX_HELPERS_H

#include <stddef.h>

#define T_ALIGN_UP(v, align) (((v) + (align) - 1) & ~((align) - 1))

int setup_listening_socket(int port, int ipv6);

/*
 * Some Android versions lack aligned_alloc in stdlib.h.
 * To avoid making large changes in tests, define a helper
 * function that wraps posix_memalign as our own aligned_alloc.
 */
void *t_aligned_alloc(size_t alignment, size_t size);

void t_error(int status, int errnum, const char *format, ...);

#ifndef CONFIG_HAVE_MEMFD_CREATE
#include <linux/memfd.h>
#endif
int memfd_create(const char *name, unsigned int flags);

#endif
