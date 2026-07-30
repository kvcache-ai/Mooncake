// SPDX-License-Identifier: MIT
/*
 * Description: test pipe creation through io_uring
 *
 */
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "helpers.h"
#include "liburing.h"

static int no_pipe;

struct params {
	int fixed;
	int async;
	int too_small;
};

static int pipe_comms(struct io_uring *ring, int *fds, struct params *p)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	char src[32], dst[32];
	int i, ret;

	memset(src, 0x5a, sizeof(src));
	memset(dst, 0, sizeof(dst));

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_write(sqe, fds[1], src, sizeof(src), 0);
	if (p->fixed)
		sqe->flags |= IOSQE_FIXED_FILE;
	sqe->user_data = 1;
	io_uring_submit(ring);

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_read(sqe, fds[0], dst, sizeof(dst), 0);
	if (p->fixed)
		sqe->flags |= IOSQE_FIXED_FILE;
	sqe->user_data = 2;
	io_uring_submit(ring);

	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait cqe %d\n", ret);
			return 1;
		}
		if (cqe->res != 32) {
			printf("ud=%d, res=%d\n", (int) cqe->user_data, cqe->res);
			return 1;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	return memcmp(dst, src, sizeof(src));
}

static int pipe_test(int init_flags, struct params *p)
{
	struct io_uring ring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, fds[2];

	ret = io_uring_queue_init(8, &ring, init_flags);
	/* can hit -ENOMEM due to repeated ring creation and teardowns */
	if (ret == -ENOMEM) {
		usleep(1000);
		return 0;
	} else if (ret) {
		fprintf(stderr, "ring_init: %d\n", ret);
		return 1;
	}

	if (p->fixed) {
		int sz;

		if (p->too_small)
			sz = 1;
		else
			sz = 100;
		ret = io_uring_register_files_sparse(&ring, sz);
		if (ret) {
			if (ret == -EINVAL) {
				no_pipe = 1;
				return 0;
			}
			fprintf(stderr, "Failed to register sparse table %d\n", ret);
			return 1;
		}
	}

	fds[0] = fds[1] = -1;
	sqe = io_uring_get_sqe(&ring);
	if (!p->fixed)
		io_uring_prep_pipe(sqe, fds, 0);
	else
		io_uring_prep_pipe_direct(sqe, fds, 0, IORING_FILE_INDEX_ALLOC);
	if (p->async)
		sqe->flags |= IOSQE_ASYNC;

	io_uring_submit(&ring);
	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait: %d\n", ret);
		return 1;
	}
	if (cqe->res) {
		if (cqe->res == -EINVAL) {
			no_pipe = 1;
			return 0;
		}
		if (p->fixed && p->too_small && cqe->res == -ENFILE)
			goto done;
		fprintf(stderr, "Bad cqe res %d\n", cqe->res);
		return 1;
	}

	io_uring_cqe_seen(&ring, cqe);

	ret = pipe_comms(&ring, fds, p);
	if (!p->fixed) {
		close(fds[0]);
		close(fds[1]);
	}
done:
	io_uring_queue_exit(&ring);
	return ret;
}

int main(int argc, char *argv[])
{
	int init_flags[] = { 0, IORING_SETUP_SQPOLL, IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_SINGLE_ISSUER };
	struct params ps[] = {
		{ 0, 0, 0 },
		{ 0, 1, 0 },
		{ 1, 0, 0 },
		{ 1, 1, 0 },
		{ 0, 0, 1 },
		{ 0, 1, 1 },
		{ 1, 0, 1 },
		{ 1, 1, 1 } };

	int i, j;

	if (argc > 1)
		return T_EXIT_SKIP;

	for (i = 0; i < ARRAY_SIZE(init_flags); i++) {
		for (j = 0; j < ARRAY_SIZE(ps); j++) {
			if (pipe_test(init_flags[i], &ps[j])) {
				fprintf(stderr, "pipe %x %d/%d/%d failed\n",
					init_flags[i], ps[j].fixed, ps[j].async,
					ps[j].too_small);
				return T_EXIT_FAIL;
			}
			if (no_pipe)
				return T_EXIT_SKIP;
		}
	}

	return T_EXIT_PASS;
}
