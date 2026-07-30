/* SPDX-License-Identifier: MIT */
/*
 * Description: Helpers for tests.
 */
#ifndef LIBURING_HELPERS_H
#define LIBURING_HELPERS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "liburing.h"
#include "../src/setup.h"
#include <arpa/inet.h>
#include <sys/time.h>
#include <stdlib.h>

enum t_setup_ret {
	T_SETUP_OK	= 0,
	T_SETUP_SKIP,
};

enum t_test_result {
	T_EXIT_PASS   = 0,
	T_EXIT_FAIL   = 1,
	T_EXIT_SKIP   = 77,
};

/*
 * Some Android versions lack aligned_alloc in stdlib.h.
 * To avoid making large changes in tests, define a helper
 * function that wraps posix_memalign as our own aligned_alloc.
 */
void *t_aligned_alloc(size_t alignment, size_t size);

/*
 * Helper for binding socket to an ephemeral port.
 * The port number to be bound is returned in @addr->sin_port.
 */
int t_bind_ephemeral_port(int fd, struct sockaddr_in *addr);


/*
 * Helper for allocating memory in tests.
 */
void *t_malloc(size_t size);


/*
 * Helper for allocating size bytes aligned on a boundary.
 */
void t_posix_memalign(void **memptr, size_t alignment, size_t size);


/*
 * Helper for allocating space for an array of nmemb elements
 * with size bytes for each element.
 */
void *t_calloc(size_t nmemb, size_t size);


/*
 * Helper for creating file and write @size byte buf with 0xaa value in the file.
 */
void t_create_file(const char *file, size_t size);

/*
 * Helper for creating file and write @size byte buf with @pattern value in
 * the file.
 */
void t_create_file_pattern(const char *file, size_t size, char pattern);

/*
 * Helper for creating @buf_num number of iovec
 * with @buf_size bytes buffer of each iovec.
 */
struct iovec *t_create_buffers(size_t buf_num, size_t buf_size);

/*
 * Helper for creating connected socket pairs
 */
int t_create_socket_pair(int fd[2], bool stream);

int t_create_socketpair_ip(struct sockaddr_storage *addr,
				int *sock_client, int *sock_server,
				bool ipv6, bool client_connect,
				bool msg_zc, bool tcp, const char *name);

/*
 * Helper for setting up a ring and checking for user privs
 */
enum t_setup_ret t_create_ring_params(int depth, struct io_uring *ring,
				      struct io_uring_params *p);
enum t_setup_ret t_create_ring(int depth, struct io_uring *ring,
			       unsigned int flags);

enum t_setup_ret t_register_buffers(struct io_uring *ring,
				    const struct iovec *iovecs,
				    unsigned nr_iovecs);

bool t_probe_defer_taskrun(void);
void t_set_nonblock(int fd);
void t_clear_nonblock(int fd);

unsigned __io_uring_flush_sq(struct io_uring *ring);

static inline int t_io_uring_init_sqarray(unsigned entries, struct io_uring *ring,
					struct io_uring_params *p)
{
	int ret;

	ret = __io_uring_queue_init_params(entries, ring, p, NULL, 0);
	return ret >= 0 ? 0 : ret;
}

#define ARRAY_SIZE(x) (sizeof(x) / sizeof((x)[0]))

void t_error(int status, int errnum, const char *format, ...);

unsigned long long mtime_since(const struct timeval *s, const struct timeval *e);
unsigned long long mtime_since_now(struct timeval *tv);
unsigned long long utime_since(const struct timeval *s, const struct timeval *e);
unsigned long long utime_since_now(struct timeval *tv);

int t_submit_and_wait_single(struct io_uring *ring, struct io_uring_cqe **cqe);

size_t t_iovec_data_length(struct iovec *iov, unsigned iov_len);

unsigned long t_compare_data_iovec(struct iovec *iov_src, unsigned nr_src,
				   struct iovec *iov_dst, unsigned nr_dst);

#ifdef __cplusplus
}
#endif

#endif
