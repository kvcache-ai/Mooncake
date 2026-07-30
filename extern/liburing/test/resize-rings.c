/* SPDX-License-Identifier: MIT */
/*
 * Description: test sq/cq ring resizing
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

static bool only_defer, no_defer;

#define NVECS	128

#define min(a, b)	((a) < (b) ? (a) : (b))

struct data {
	pthread_t thread;
	int fd;
	int nr_writes;
	int failed;
};

static void *thread_fn(void *__data)
{
	struct data *d = __data;
	char buffer[8];
	int to_write = d->nr_writes;

	memset(buffer, 0x5a, sizeof(buffer));
	usleep(10000);
	while (to_write) {
		int ret = write(d->fd, buffer, sizeof(buffer));

		if (ret < 0) {
			perror("write");
			d->failed = 1;
			break;
		} else if (ret != sizeof(buffer)) {
			printf("short write %d\n", ret);
		}
		to_write--;
		usleep(5);
	}
	return NULL;
}

static int test_pipes(struct io_uring *ring, int async)
{
	struct io_uring_params p = { };
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	unsigned long ud = 0;
	struct data d = { };
	int ret, i, fds[2], to_read;
	char buffer[8];
	void *tret;

	p.sq_entries = 128;
	p.cq_entries = 128;
	ret = io_uring_resize_rings(ring, &p);
	if (ret < 0) {
		fprintf(stderr, "Failed to resize ring: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (pipe(fds) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	/*
	 * Put NVECS inflight, then resize while waiting. Repeat until
	 * 'to_read' has been read.
	 */
	d.nr_writes = 4096;
	d.fd = fds[1];
	p.sq_entries = 64;
	p.cq_entries = 256;

	pthread_create(&d.thread, NULL, thread_fn, &d);

	to_read = d.nr_writes - 128;
	while (to_read && !d.failed) {
		unsigned long start_ud = -1UL, end_ud;
		int to_wait;

		to_wait = NVECS;
		if (to_wait > to_read)
			to_wait = to_read;

		for (i = 0; i < to_wait; i++) {
			sqe = io_uring_get_sqe(ring);
			/* resized smaller */
			if (!sqe)
				break;
			io_uring_prep_read(sqe, fds[0], buffer, sizeof(buffer), 0);
			if (async)
				sqe->flags |= IOSQE_ASYNC;
			if (start_ud == -1UL)
				start_ud = ud;
			sqe->user_data = ++ud;
			to_read--;
		}
		end_ud = ud;
		ret = io_uring_submit(ring);
		if (ret != i) {
			fprintf(stderr, "submitted; %d\n", ret);
			return T_EXIT_FAIL;
		}

		to_wait = i;
		for (i = 0; i < to_wait; i++) {
			if (i == 0) {
				ret = io_uring_resize_rings(ring, &p);
				if (ret < 0) {
					if (ret != -EOVERFLOW) {
						fprintf(stderr, "resize failed: %d\n", ret);
						return T_EXIT_FAIL;
					}
				}
				p.sq_entries = 32;
				p.cq_entries = 128;
			}
			if (d.failed)
				break;
			ret = io_uring_wait_cqe(ring, &cqe);
			if (ret) {
				fprintf(stderr, "wait cqe: %d\n", ret);
				return T_EXIT_FAIL;
			}
			if (cqe->res < 0) {
				fprintf(stderr, "cqe res %d\n", cqe->res);
				return T_EXIT_FAIL;
			}
			if (cqe->user_data < start_ud ||
			    cqe->user_data > end_ud) {
				fprintf(stderr, "use_data out-of-range: <%lu-%lu>: %lu\n",
					start_ud, end_ud, (long) cqe->user_data);
				return T_EXIT_FAIL;
			}
			io_uring_cqe_seen(ring, cqe);
			if (!(i % 17)) {
				ret = io_uring_resize_rings(ring, &p);
				if (ret < 0) {
					if (ret == -EOVERFLOW)
						continue;
					fprintf(stderr, "resize failed: %d\n", ret);
					return T_EXIT_FAIL;
				}
				if (p.sq_entries == 32)
					p.sq_entries = 64;
				else if (p.sq_entries == 64)
					p.sq_entries = 16;
				else
					p.sq_entries = 32;
				if (p.cq_entries == 128)
					p.cq_entries = 256;
				else
					p.cq_entries = 128;
			}
		}
	}

	pthread_join(d.thread, &tret);
	close(fds[0]);
	close(fds[0]);
	return 0;
}

static int test_reads(struct io_uring *ring, int fd, int async)
{
	struct io_uring_params p = { };
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct iovec vecs[NVECS];
	unsigned long to_read;
	unsigned long ud = 0;
	unsigned long offset;
	int ret, i;

	if (fd == -1)
		return T_EXIT_SKIP;

	p.sq_entries = 128;
	p.cq_entries = 128;
	ret = io_uring_resize_rings(ring, &p);
	if (ret < 0) {
		fprintf(stderr, "Failed to resize ring: %d\n", ret);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < NVECS; i++) {
		if (posix_memalign(&vecs[i].iov_base, 4096, 4096))
			return T_EXIT_FAIL;
		vecs[i].iov_len = 4096;
	}

	/*
	 * Put NVECS inflight, then resize while waiting. Repeat until
	 * 'to_read' has been read.
	 */
	to_read = 64*1024*1024;
	p.sq_entries = 64;
	p.cq_entries = 256;
	offset = 0;
	while (to_read) {
		unsigned long start_ud = -1UL, end_ud;
		int to_wait;

		for (i = 0; i < NVECS; i++) {
			sqe = io_uring_get_sqe(ring);
			/* resized smaller */
			if (!sqe)
				break;
			io_uring_prep_read(sqe, fd, vecs[i].iov_base,
						vecs[i].iov_len, offset);
			if (async)
				sqe->flags |= IOSQE_ASYNC;
			offset += 8192;
			if (start_ud == -1UL)
				start_ud = ud;
			sqe->user_data = ++ud;
		}
		end_ud = ud;
		ret = io_uring_submit(ring);
		if (ret != i) {
			fprintf(stderr, "submitted; %d\n", ret);
			return T_EXIT_FAIL;
		}

		to_wait = i;
		for (i = 0; i < to_wait; i++) {
			if (i == 0) {
				ret = io_uring_resize_rings(ring, &p);
				if (ret < 0) {
					if (ret != -EOVERFLOW) {
						fprintf(stderr, "resize failed: %d\n", ret);
						return T_EXIT_FAIL;
					}
				}
				p.sq_entries = 32;
				p.cq_entries = 128;
			}
			ret = io_uring_wait_cqe(ring, &cqe);
			if (ret) {
				fprintf(stderr, "wait cqe: %d\n", ret);
				return T_EXIT_FAIL;
			}
			if (cqe->res < 0) {
				fprintf(stderr, "cqe res %d\n", cqe->res);
				return T_EXIT_FAIL;
			}
			if (cqe->user_data < start_ud ||
			    cqe->user_data > end_ud) {
				fprintf(stderr, "use_data out-of-range: <%lu-%lu>: %lu\n",
					start_ud, end_ud, (long) cqe->user_data);
				return T_EXIT_FAIL;
			}
			io_uring_cqe_seen(ring, cqe);
			if (to_read)
				to_read -= min(to_read, 4096);
			if (!(i % 17)) {
				ret = io_uring_resize_rings(ring, &p);
				if (ret < 0) {
					if (ret == -EOVERFLOW)
						continue;
					fprintf(stderr, "resize failed: %d\n", ret);
					return T_EXIT_FAIL;
				}
				if (p.sq_entries == 32)
					p.sq_entries = 64;
				else if (p.sq_entries == 64)
					p.sq_entries = 16;
				else
					p.sq_entries = 32;
				if (p.cq_entries == 128)
					p.cq_entries = 256;
				else
					p.cq_entries = 128;
			}
		}
	}

	for (i = 0; i < NVECS; i++)
		free(vecs[i].iov_base);

	return 0;
}

static int test_basic(struct io_uring *ring, int async)
{
	struct io_uring_params p = { };
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int i, ret;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	if (async)
		sqe->flags |= IOSQE_ASYNC;
	sqe->user_data = 1;
	io_uring_submit(ring);

	p.sq_entries = 32;
	p.cq_entries = 64;
	ret = io_uring_resize_rings(ring, &p);
	if (ret == -EINVAL || ret == -ENOMEM) {
		return T_EXIT_SKIP;
	} else if (ret < 0) {
		fprintf(stderr, "resize=%d\n", ret);
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	if (async)
		sqe->flags |= IOSQE_ASYNC;
	sqe->user_data = 2;
	io_uring_submit(ring);

	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait cqe %d\n", ret);
			return T_EXIT_FAIL;
		}
		if (cqe->user_data != i + 1) {
			fprintf(stderr, "bad user_data %ld\n", (long) cqe->user_data);
			return T_EXIT_FAIL;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	return T_EXIT_PASS;
}

static int test_all_copy(struct io_uring *ring)
{
	struct io_uring_params p = { };
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	unsigned head;
	int i, ret;

	p.sq_entries = 32;
	p.cq_entries = 64;
	ret = io_uring_resize_rings(ring, &p);
	if (ret) {
		fprintf(stderr, "resize failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < 32; i++) {
		sqe = io_uring_get_sqe(ring);
		io_uring_prep_nop(sqe);
		sqe->user_data = i + 1;
	}

	io_uring_submit(ring);

	memset(&p, 0, sizeof(p));
	p.sq_entries = 64;
	p.cq_entries = 128;
	ret = io_uring_resize_rings(ring, &p);
	if (ret) {
		fprintf(stderr, "resize failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	i = 1;
	io_uring_for_each_cqe(ring, head, cqe) {
		if (cqe->user_data != i) {
			fprintf(stderr, "Found cqe at wrong offset\n");
			return T_EXIT_FAIL;
		}
		i++;
	}
	io_uring_cq_advance(ring, 32);
	return T_EXIT_PASS;
}

static int test_overflow(struct io_uring *ring)
{
	struct io_uring_params p = { };
	struct io_uring_sqe *sqe;
	int i, ret;

	p.sq_entries = 32;
	p.cq_entries = 64;
	ret = io_uring_resize_rings(ring, &p);
	if (ret) {
		fprintf(stderr, "resize failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < 32; i++) {
		sqe = io_uring_get_sqe(ring);
		io_uring_prep_nop(sqe);
		sqe->user_data = i + 1;
	}

	io_uring_submit(ring);

	/* have 32 CQEs pending, resize to CQ size 32 which should work */
	memset(&p, 0, sizeof(p));
	p.sq_entries = 32;
	p.cq_entries = 32;
	ret = io_uring_resize_rings(ring, &p);
	if (ret) {
		fprintf(stderr, "resize failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* now resize to CQ size 16, which should fail with -EOVERFLOW */
	memset(&p, 0, sizeof(p));
	p.sq_entries = 8;
	p.cq_entries = 16;
	ret = io_uring_resize_rings(ring, &p);
	if (ret != -EOVERFLOW) {
		fprintf(stderr, "Expected overflow, got %d\n", ret);
		return T_EXIT_FAIL;
	}

	io_uring_cq_advance(ring, 32);
	return T_EXIT_PASS;
}

static int test_same_resize(int flags)
{
	struct io_uring_params p = { };
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int i, ret;

	ret = io_uring_queue_init(32, &ring, flags);
	if (ret)
		return T_EXIT_FAIL;

	p.sq_entries = 32;
	p.cq_entries = 64;
	ret = io_uring_resize_rings(&ring, &p);
	if (ret) {
		fprintf(stderr, "resize failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < 32; i++) {
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_nop(sqe);
		sqe->user_data = i + 1;
	}

	io_uring_submit(&ring);

	for (i = 0; i < 32; i++) {
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe: %d\n", ret);
			return T_EXIT_FAIL;
		}
		if (cqe->user_data != i + 1) {
			fprintf(stderr, "Found cqe at wrong offset\n");
			return T_EXIT_FAIL;
		}
		io_uring_cqe_seen(&ring, cqe);
	}

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}

static int mmap_child(struct io_uring *__ring, struct io_uring_params *__p)
{
	struct io_uring ring = *__ring;
	struct timeval tv;
	int ret;

	gettimeofday(&tv, NULL);
	do {
		struct io_uring_params p = *__p;
		void *sq_ptr, *cq_ptr;

		ret = io_uring_queue_mmap(__ring->ring_fd, &p, &ring);
		if (ret)
			continue;

		sq_ptr = ring.sq.ring_ptr + 2 * sizeof(__u32);
		cq_ptr = ring.cq.ring_ptr + 2 * sizeof(__u32);
		memset(sq_ptr, 0x5a, ring.sq.ring_sz - 2 * sizeof(__u32));
		memset(cq_ptr, 0xa5, ring.cq.ring_sz - 2 * sizeof(__u32));
		io_uring_unmap_rings(&ring.sq, &ring.cq);
	} while (mtime_since_now(&tv) < 2500);

	exit(T_EXIT_PASS);
}

static int test_mmap_race(struct io_uring *ring, struct io_uring_params *__p)
{
	unsigned long useless_sum;
	int i, w, nr_children;
	struct timeval tv;
	pid_t pid;

	nr_children = sysconf(_SC_NPROCESSORS_ONLN);
	if (nr_children < 0)
		nr_children = 4;

	for (i = 0; i < nr_children; i++) {
		pid = fork();
		if (!pid) {
			mmap_child(ring, __p);
			return T_EXIT_PASS;
		}
	}

	useless_sum = 0;
	gettimeofday(&tv, NULL);
	do {
		struct io_uring_params p = { .sq_entries = 32, };
		void *ptr;

		io_uring_resize_rings(ring, &p);

		ptr = memchr(ring->sq.ring_ptr, 0x5a, ring->sq.ring_sz);
		if (ptr)
			useless_sum += ptr - ring->sq.ring_ptr;

		ptr = memchr(ring->cq.ring_ptr, 0xa5, ring->cq.ring_sz);
		if (ptr)
			useless_sum += ptr - ring->cq.ring_ptr;

		p.sq_entries = 128;
		io_uring_resize_rings(ring, &p);
	} while (mtime_since_now(&tv) < 2500);

	for (i = 0; i < nr_children; i++)
		wait(&w);

	if (useless_sum)
		return T_EXIT_PASS;
	return T_EXIT_PASS;
}

static int test(int flags, int fd, int async)
{
	struct io_uring_params p = {
		.flags = flags,
	};
	struct io_uring ring;
	int ret;

	if (no_defer)
		return T_EXIT_SKIP;
	if (!(flags & IORING_SETUP_DEFER_TASKRUN) && only_defer)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init_params(8, &ring, &p);
	if (ret == -EINVAL)
		return T_EXIT_SKIP;
	if (ret < 0) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = test_basic(&ring, async);
	if (ret == T_EXIT_SKIP) {
		if (!(flags & IORING_SETUP_DEFER_TASKRUN)) {
			io_uring_queue_exit(&ring);
			only_defer = true;
		} else {
			no_defer = true;
		}
		return T_EXIT_SKIP;
	} else if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_basic %x failed\n", flags);
		return T_EXIT_FAIL;
	}

	ret = test_reads(&ring, fd, async);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_reads %x failed\n", flags);
		return T_EXIT_FAIL;
	}

	ret = test_pipes(&ring, async);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_pipes %x failed\n", flags);
		return T_EXIT_FAIL;
	}

	if (async)
		return T_EXIT_PASS;

	ret = test_all_copy(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_all_copy %x failed\n", flags);
		return T_EXIT_FAIL;
	}

	ret = test_overflow(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_overflow %x failed\n", flags);
		return T_EXIT_FAIL;
	}

	ret = test_same_resize(flags);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_same_resize %x failed\n", flags);
		return T_EXIT_FAIL;
	}

	/* must go at the end, insert more tests above this one */
	ret = test_mmap_race(&ring, &p);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_mmap_race %x failed\n", flags);
		return T_EXIT_FAIL;
	}

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	int ret, fd = -1;

	if (argc > 1)
		fd = open("/dev/nvme0n1", O_RDONLY | O_DIRECT);

	ret = test(0, fd, 0);
	if (ret == T_EXIT_SKIP)
		goto try_defer;
	else if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	ret = test(0, fd, 1);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	ret = test(IORING_SETUP_SQPOLL, fd, 0);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	ret = test(IORING_SETUP_SQPOLL, fd, 1);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

try_defer:
	ret = test(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN, fd, 0);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;
	else if (ret == T_EXIT_SKIP)
		return T_EXIT_SKIP;

	ret = test(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN, fd, 1);
	if (ret == T_EXIT_FAIL)
		return T_EXIT_FAIL;

	return T_EXIT_PASS;
}
