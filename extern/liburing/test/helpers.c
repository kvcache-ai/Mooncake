/* SPDX-License-Identifier: MIT */
/*
 * Description: Helpers for tests.
 */
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <sys/types.h>

#include <arpa/inet.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>

#include "helpers.h"
#include "liburing.h"

/*
 * Helper for allocating memory in tests.
 */
void *t_malloc(size_t size)
{
	void *ret;
	ret = malloc(size);
	assert(ret);
	return ret;
}

/*
 * Helper for binding socket to an ephemeral port.
 * The port number to be bound is returned in @addr->sin_port.
 */
int t_bind_ephemeral_port(int fd, struct sockaddr_in *addr)
{
	socklen_t addrlen;
	int ret;

	addr->sin_port = 0;
	if (bind(fd, (struct sockaddr *)addr, sizeof(*addr)))
		return -errno;

	addrlen = sizeof(*addr);
	ret = getsockname(fd, (struct sockaddr *)addr, &addrlen);
	assert(!ret);
	assert(addr->sin_port != 0);
	return 0;
}

/*
 * Helper for allocating size bytes aligned on a boundary.
 */
void t_posix_memalign(void **memptr, size_t alignment, size_t size)
{
	int ret;
	ret = posix_memalign(memptr, alignment, size);
	assert(!ret);
}

/*
 * Helper for allocating space for an array of nmemb elements
 * with size bytes for each element.
 */
void *t_calloc(size_t nmemb, size_t size)
{
	void *ret;
	ret = calloc(nmemb, size);
	assert(ret);
	return ret;
}

/*
 * Helper for creating file and write @size byte buf with 0xaa value in the file.
 */
static void __t_create_file(const char *file, size_t size, char pattern)
{
	ssize_t ret;
	char *buf;
	int fd;

	buf = t_malloc(size);
	memset(buf, pattern, size);

	fd = open(file, O_WRONLY | O_CREAT, 0644);
	assert(fd >= 0);

	ret = write(fd, buf, size);
	fsync(fd);
	close(fd);
	free(buf);
	assert(ret == size);
}

void t_create_file(const char *file, size_t size)
{
	__t_create_file(file, size, 0xaa);
}

void t_create_file_pattern(const char *file, size_t size, char pattern)
{
	__t_create_file(file, size, pattern);
}

/*
 * Helper for creating @buf_num number of iovec
 * with @buf_size bytes buffer of each iovec.
 */
struct iovec *t_create_buffers(size_t buf_num, size_t buf_size)
{
	struct iovec *vecs;
	int i;

	vecs = t_malloc(buf_num * sizeof(struct iovec));
	for (i = 0; i < buf_num; i++) {
		t_posix_memalign(&vecs[i].iov_base, buf_size, buf_size);
		vecs[i].iov_len = buf_size;
	}
	return vecs;
}

/*
 * Helper for setting up an io_uring instance, skipping if the given user isn't
 * allowed to.
 */
enum t_setup_ret t_create_ring_params(int depth, struct io_uring *ring,
				      struct io_uring_params *p)
{
	int ret;

	ret = io_uring_queue_init_params(depth, ring, p);
	if (!ret)
		return T_SETUP_OK;
	if ((p->flags & IORING_SETUP_SQPOLL) && ret == -EPERM && geteuid()) {
		fprintf(stdout, "SQPOLL skipped for regular user\n");
		return T_SETUP_SKIP;
	}
	if (ret == -EINVAL)
		return T_SETUP_SKIP;

	fprintf(stderr, "queue_init: %s\n", strerror(-ret));
	return ret;
}

enum t_setup_ret t_create_ring(int depth, struct io_uring *ring,
			       unsigned int flags)
{
	struct io_uring_params p = { };

	p.flags = flags;
	return t_create_ring_params(depth, ring, &p);
}

enum t_setup_ret t_register_buffers(struct io_uring *ring,
				    const struct iovec *iovecs,
				    unsigned nr_iovecs)
{
	int ret;

	ret = io_uring_register_buffers(ring, iovecs, nr_iovecs);
	if (!ret)
		return T_SETUP_OK;

	if ((ret == -EPERM || ret == -ENOMEM) && geteuid()) {
		fprintf(stdout, "too large non-root buffer registration, skip\n");
		return T_SETUP_SKIP;
	}

	fprintf(stderr, "buffer register failed: %s\n", strerror(-ret));
	return ret;
}

int t_create_socket_pair(int fd[2], bool stream)
{
	int ret;
	int type = stream ? SOCK_STREAM : SOCK_DGRAM;
	int val;
	struct sockaddr_in serv_addr;
	struct sockaddr *paddr;
	socklen_t paddrlen;

	type |= SOCK_CLOEXEC;
	fd[0] = socket(AF_INET, type, 0);
	if (fd[0] < 0)
		return errno;
	fd[1] = socket(AF_INET, type, 0);
	if (fd[1] < 0) {
		ret = errno;
		close(fd[0]);
		return ret;
	}

	val = 1;
	if (setsockopt(fd[0], SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)))
		goto errno_cleanup;

	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = 0;
	inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

	paddr = (struct sockaddr *)&serv_addr;
	paddrlen = sizeof(serv_addr);

	if (bind(fd[0], paddr, paddrlen)) {
		fprintf(stderr, "bind failed\n");
		goto errno_cleanup;
	}

	if (stream && listen(fd[0], 16)) {
		fprintf(stderr, "listen failed\n");
		goto errno_cleanup;
	}

	if (getsockname(fd[0], (struct sockaddr *)&serv_addr,
			(socklen_t *)&paddrlen)) {
		fprintf(stderr, "getsockname failed\n");
		goto errno_cleanup;
	}
	inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

	if (connect(fd[1], (struct sockaddr *)&serv_addr, paddrlen)) {
		fprintf(stderr, "connect failed\n");
		goto errno_cleanup;
	}

	if (!stream) {
		/* connect the other udp side */
		if (getsockname(fd[1], (struct sockaddr *)&serv_addr,
				(socklen_t *)&paddrlen)) {
			fprintf(stderr, "getsockname failed\n");
			goto errno_cleanup;
		}
		inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

		if (connect(fd[0], (struct sockaddr *)&serv_addr, paddrlen)) {
			fprintf(stderr, "connect failed\n");
			goto errno_cleanup;
		}
		return 0;
	}

	/* for stream case we must accept and cleanup the listen socket */

	ret = accept(fd[0], NULL, NULL);
	if (ret < 0)
		goto errno_cleanup;

	close(fd[0]);
	fd[0] = ret;

	return 0;

errno_cleanup:
	ret = errno;
	close(fd[0]);
	close(fd[1]);
	return ret;
}

bool t_probe_defer_taskrun(void)
{
	struct io_uring ring;
	int ret;

	ret = io_uring_queue_init(1, &ring, IORING_SETUP_SINGLE_ISSUER |
					    IORING_SETUP_DEFER_TASKRUN);
	if (ret < 0)
		return false;
	io_uring_queue_exit(&ring);
	return true;
}

/*
 * Sync internal state with kernel ring state on the SQ side. Returns the
 * number of pending items in the SQ ring, for the shared ring.
 */
unsigned __io_uring_flush_sq(struct io_uring *ring)
{
	struct io_uring_sq *sq = &ring->sq;
	unsigned tail = sq->sqe_tail;

	if (sq->sqe_head != tail) {
		sq->sqe_head = tail;
		/*
		 * Ensure kernel sees the SQE updates before the tail update.
		 */
		if (!(ring->flags & IORING_SETUP_SQPOLL))
			*sq->ktail = tail;
		else
			io_uring_smp_store_release(sq->ktail, tail);
	}
	/*
	* This load needs to be atomic, since sq->khead is written concurrently
	* by the kernel, but it doesn't need to be load_acquire, since the
	* kernel doesn't store to the submission queue; it advances khead just
	* to indicate that it's finished reading the submission queue entries
	* so they're available for us to write to.
	*/
	return tail - IO_URING_READ_ONCE(*sq->khead);
}

/*
 * Implementation of error(3), prints an error message and exits.
 */
void t_error(int status, int errnum, const char *format, ...)
{
	va_list args;
    	va_start(args, format);

	vfprintf(stderr, format, args);
    	if (errnum)
        	fprintf(stderr, ": %s", strerror(errnum));

	fprintf(stderr, "\n");
	va_end(args);
    	exit(status);
}

unsigned long long mtime_since(const struct timeval *s, const struct timeval *e)
{
	long long sec, usec;

	sec = e->tv_sec - s->tv_sec;
	usec = (e->tv_usec - s->tv_usec);
	if (sec > 0 && usec < 0) {
		sec--;
		usec += 1000000;
	}

	sec *= 1000;
	usec /= 1000;
	return sec + usec;
}

unsigned long long mtime_since_now(struct timeval *tv)
{
	struct timeval end;

	gettimeofday(&end, NULL);
	return mtime_since(tv, &end);
}

unsigned long long utime_since(const struct timeval *s, const struct timeval *e)
{
	long long sec, usec;

	sec = e->tv_sec - s->tv_sec;
	usec = (e->tv_usec - s->tv_usec);
	if (sec > 0 && usec < 0) {
		sec--;
		usec += 1000000;
	}

	sec *= 1000000;
	return sec + usec;
}

unsigned long long utime_since_now(struct timeval *tv)
{
	struct timeval end;

	gettimeofday(&end, NULL);
	return utime_since(tv, &end);
}

void *t_aligned_alloc(size_t alignment, size_t size)
{
	void *ret;

	if (posix_memalign(&ret, alignment, size))
		return NULL;

	return ret;
}

int t_create_socketpair_ip(struct sockaddr_storage *addr,
				int *sock_client, int *sock_server,
				bool ipv6, bool client_connect,
				bool msg_zc, bool tcp, const char *name)
{
	socklen_t addr_size;
	int family, sock, listen_sock = -1;
	int ret;

	memset(addr, 0, sizeof(*addr));
	if (ipv6) {
		struct sockaddr_in6 *saddr = (struct sockaddr_in6 *)addr;

		family = AF_INET6;
		saddr->sin6_family = family;
		saddr->sin6_port = htons(0);
		addr_size = sizeof(*saddr);
	} else {
		struct sockaddr_in *saddr = (struct sockaddr_in *)addr;

		family = AF_INET;
		saddr->sin_family = family;
		saddr->sin_port = htons(0);
		saddr->sin_addr.s_addr = htonl(INADDR_ANY);
		addr_size = sizeof(*saddr);
	}

	/* server sock setup */
	if (tcp) {
		sock = listen_sock = socket(family, SOCK_STREAM, IPPROTO_TCP);
	} else {
		sock = *sock_server = socket(family, SOCK_DGRAM, 0);
	}
	if (sock < 0) {
		perror("socket");
		return 1;
	}

	ret = bind(sock, (struct sockaddr *)addr, addr_size);
	if (ret < 0) {
		perror("bind");
		return 1;
	}

	ret = getsockname(sock, (struct sockaddr *)addr, &addr_size);
	if (ret < 0) {
		fprintf(stderr, "getsockname failed %i\n", errno);
		return 1;
	}

	if (tcp) {
		ret = listen(sock, 128);
		assert(ret != -1);
	}

	if (ipv6) {
		struct sockaddr_in6 *saddr = (struct sockaddr_in6 *)addr;

		inet_pton(AF_INET6, name, &(saddr->sin6_addr));
	} else {
		struct sockaddr_in *saddr = (struct sockaddr_in *)addr;

		inet_pton(AF_INET, name, &saddr->sin_addr);
	}

	/* client sock setup */
	if (tcp) {
		*sock_client = socket(family, SOCK_STREAM, IPPROTO_TCP);
		assert(client_connect);
	} else {
		*sock_client = socket(family, SOCK_DGRAM, 0);
	}
	if (*sock_client < 0) {
		perror("socket");
		return 1;
	}
	if (client_connect) {
		ret = connect(*sock_client, (struct sockaddr *)addr, addr_size);
		if (ret < 0) {
			perror("connect");
			return 1;
		}
	}
	if (msg_zc) {
#ifdef SO_ZEROCOPY
		int val = 1;

		/*
		 * NOTE: apps must not set SO_ZEROCOPY when using io_uring zc.
		 * It's only here to test interactions with MSG_ZEROCOPY.
		 */
		if (setsockopt(*sock_client, SOL_SOCKET, SO_ZEROCOPY, &val, sizeof(val))) {
			perror("setsockopt zc");
			return 1;
		}
#else
		fprintf(stderr, "no SO_ZEROCOPY\n");
		return 1;
#endif
	}
	if (tcp) {
		*sock_server = accept(listen_sock, NULL, NULL);
		if (!*sock_server) {
			fprintf(stderr, "can't accept\n");
			return 1;
		}
		close(listen_sock);
	}
	return 0;
}

static void __t_toggle_nonblock(int fd, int set)
{
	int flags;

	flags = fcntl(fd, F_GETFL, 0);
	if (flags < 0)
		t_error(1, errno, "fcntl F_GETFL");
	if (set)
		flags |= O_NONBLOCK;
	else
		flags &= ~O_NONBLOCK;
	if (fcntl(fd, F_SETFL, flags) < 0)
		t_error(1, errno, "fcntl F_SETFL");
}

void t_set_nonblock(int fd)
{
	__t_toggle_nonblock(fd, 1);
}

void t_clear_nonblock(int fd)
{
	__t_toggle_nonblock(fd, 0);
}

int t_submit_and_wait_single(struct io_uring *ring, struct io_uring_cqe **cqe)
{
	int ret;

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		return -1;
	}
	ret = io_uring_wait_cqe(ring, cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
		return ret;
	}
	return 0;
}

size_t t_iovec_data_length(struct iovec *iov, unsigned iov_len)
{
	size_t sz = 0;
	int i;

	for (i = 0; i < iov_len; i++)
		sz += iov[i].iov_len;
	return sz;
}

#define t_min(a, b) ((a) < (b) ? (a) : (b))

unsigned long t_compare_data_iovec(struct iovec *iov_src, unsigned nr_src,
				   struct iovec *iov_dst, unsigned nr_dst)
{
	size_t src_len = t_iovec_data_length(iov_src, nr_src);
	size_t dst_len = t_iovec_data_length(iov_dst, nr_dst);
	size_t len_left = t_min(src_len, dst_len);
	unsigned long src_off = 0, dst_off = 0;
	unsigned long offset = 0;

	while (offset != len_left) {
		size_t len = len_left - offset;
		unsigned long i;

		len = t_min(len, iov_src->iov_len - src_off);
		len = t_min(len, iov_dst->iov_len - dst_off);

		for (i = 0; i < len; i++) {
			char csrc = ((char *)iov_src->iov_base)[src_off + i];
			char cdst = ((char *)iov_dst->iov_base)[dst_off + i];

			if (csrc != cdst) {
				fprintf(stderr, "data mismatch, %i vs %i\n",
					csrc, cdst);
				return -EINVAL;
			}
		}

		src_off += len;
		dst_off += len;
		if (src_off == iov_src->iov_len) {
			src_off = 0;
			iov_src++;
		}
		if (dst_off == iov_dst->iov_len) {
			dst_off = 0;
			iov_dst++;
		}
		offset += len;
	}
	return 0;
}
