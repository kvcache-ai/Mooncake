/* SPDX-License-Identifier: MIT */
/*
 * Description: read /proc/kallsyms. Mostly just here so that fops->read() can
 *		get exercised, with and without registered buffers
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/resource.h>

#include "helpers.h"
#include "liburing.h"

#define FILE_SIZE	(8 * 1024)
#define BS		8192
#define BUFFERS		(FILE_SIZE / BS)

static struct iovec *vecs;
static int warned;

static int __test_io(const char *file, struct io_uring *ring, int fixed, int nonvec)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int open_flags;
	int i, fd = -1, ret;
	off_t offset;

	open_flags = O_RDONLY;
	if (fixed) {
		ret = t_register_buffers(ring, vecs, BUFFERS);
		if (ret == T_SETUP_SKIP)
			return 0;
		if (ret != T_SETUP_OK) {
			fprintf(stderr, "buffer reg failed: %d\n", ret);
			goto err;
		}
	}

	fd = open(file, open_flags);
	if (fd < 0) {
		if (errno == EINVAL || errno == EPERM || errno == ENOENT)
			return 0;
		perror("file open");
		goto err;
	}

	offset = 0;
	for (i = 0; i < BUFFERS; i++) {
		int do_fixed = fixed;

		sqe = io_uring_get_sqe(ring);
		if (!sqe) {
			fprintf(stderr, "sqe get failed\n");
			goto err;
		}
		if (fixed && (i & 1))
			do_fixed = 0;
		if (do_fixed) {
			io_uring_prep_read_fixed(sqe, fd, vecs[i].iov_base,
						vecs[i].iov_len, offset, i);
		} else if (nonvec) {
			io_uring_prep_read(sqe, fd, vecs[i].iov_base,
							vecs[i].iov_len, offset);
		} else {
			io_uring_prep_readv(sqe, fd, &vecs[i], 1, offset);
		}
		sqe->user_data = i;
		offset += BS;
	}

	ret = io_uring_submit(ring);
	if (ret != BUFFERS) {
		fprintf(stderr, "submit got %d, wanted %d\n", ret, BUFFERS);
		goto err;
	}

	for (i = 0; i < BUFFERS; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe=%d\n", ret);
			goto err;
		}
		if (cqe->res == -EINVAL && nonvec) {
			if (!warned) {
				fprintf(stdout, "Non-vectored IO not "
					"supported, skipping\n");
				warned = 1;
			}
		}
		io_uring_cqe_seen(ring, cqe);
	}

	if (fixed) {
		ret = io_uring_unregister_buffers(ring);
		if (ret) {
			fprintf(stderr, "buffer unreg failed: %d\n", ret);
			goto err;
		}
	}

	close(fd);
	return 0;
err:
	if (fd != -1)
		close(fd);
	return 1;
}
static int test_io(const char *file, int fixed, int nonvec)
{
	struct io_uring ring;
	int ret, ring_flags = 0;

	ret = t_create_ring(64, &ring, ring_flags);
	if (ret == T_SETUP_SKIP)
		return 0;
	if (ret != T_SETUP_OK) {
		fprintf(stderr, "ring create failed: %d\n", ret);
		return 1;
	}

	ret = __test_io(file, &ring, fixed, nonvec);
	io_uring_queue_exit(&ring);
	return ret;
}

static int has_nonvec_read(void)
{
	struct io_uring_probe *p;
	struct io_uring ring;
	int ret;

	ret = io_uring_queue_init(1, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue init failed: %d\n", ret);
		exit(ret);
	}

	p = t_calloc(1, sizeof(*p) + 256 * sizeof(struct io_uring_probe_op));
	ret = io_uring_register_probe(&ring, p, 256);
	/* if we don't have PROBE_REGISTER, we don't have OP_READ/WRITE */
	if (ret == -EINVAL) {
out:
		io_uring_queue_exit(&ring);
		free(p);
		return 0;
	} else if (ret) {
		fprintf(stderr, "register_probe: %d\n", ret);
		goto out;
	}

	if (p->ops_len <= IORING_OP_READ)
		goto out;
	if (!(p->ops[IORING_OP_READ].flags & IO_URING_OP_SUPPORTED))
		goto out;
	io_uring_queue_exit(&ring);
	free(p);
	return 1;
}

int main(int argc, char *argv[])
{
	int ret, nonvec;

	if (argc > 1)
		return T_EXIT_SKIP;

	vecs = t_create_buffers(BUFFERS, BS);

	/* if we don't have nonvec read, skip testing that */
	nonvec = has_nonvec_read();

	if (nonvec) {
		ret = test_io("/proc/kallsyms", 0, 0);
		if (ret)
			goto err;
	}

	ret = test_io("/proc/kallsyms", 0, 1);
	if (ret)
		goto err;

	if (nonvec) {
		ret = test_io("/proc/kallsyms", 1, 0);
		if (ret)
			goto err;
	}

	ret = test_io("/proc/kallsyms", 1, 1);
	if (ret)
		goto err;

	return 0;
err:
	fprintf(stderr, "Reading kallsyms failed\n");
	return 1;
}
