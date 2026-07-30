/* SPDX-License-Identifier: MIT */
/*
 * Description: test multishot read on stdin. Not that this REQUIRES input
 *		to be received on stdin, and hence if invoked with no
 *		arguments, or without the single argument being 'stdin',
 *		the test will just return SKIPPED. Can't be run from the
 *		standard test harness, as it's interactive.
 *
 * To run, do run "test/read-mshot-stdin.t stdin" and then input text on
 * the console, followed by enter / line feed. If it works as it should,
 * it'll output the received CQE data. If an error is detected, it'll
 * abort with an error.
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "liburing.h"
#include "helpers.h"

#define BUF_SIZE	32
#define NR_BUFS		64
#define BUF_BGID	1

#define BR_MASK		(NR_BUFS - 1)

static int test_stdin(void)
{
	struct io_uring_buf_ring *br;
	struct io_uring_params p = { };
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int ret, i, last_bid;
	char *buf, *ptr;

	p.flags = IORING_SETUP_CQSIZE;
	p.cq_entries = NR_BUFS;
	ret = io_uring_queue_init_params(1, &ring, &p);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (posix_memalign((void **) &buf, 4096, NR_BUFS * BUF_SIZE))
		return T_EXIT_FAIL;

	br = io_uring_setup_buf_ring(&ring, NR_BUFS, BUF_BGID, 0, &ret);
	if (!br) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "Buffer ring register failed %d\n", ret);
		return T_EXIT_FAIL;
	}

	ptr = buf;
	for (i = 0; i < NR_BUFS; i++) {
		io_uring_buf_ring_add(br, ptr, BUF_SIZE, i + 1, BR_MASK, i);
		ptr += BUF_SIZE;
	}
	io_uring_buf_ring_advance(br, NR_BUFS);

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_read_multishot(sqe, STDIN_FILENO, 0, 0, BUF_BGID);

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		return T_EXIT_FAIL;
	}

	last_bid = -1;
	do {
		int bid;

		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait cqe failed %d\n", ret);
			return T_EXIT_FAIL;
		}
		if (cqe->res && !(cqe->flags & IORING_CQE_F_BUFFER)) {
			fprintf(stderr, "BUF flag not set %x\n", cqe->flags);
			return T_EXIT_FAIL;
		}
		bid = cqe->flags >> 16;
		printf("CQE res %d, bid %d, flags %x\n", cqe->res, bid, cqe->flags);
		if (cqe->res > 0 && last_bid != -1 && last_bid + 1 != bid) {
			fprintf(stderr, "Got bid %d, wanted %d\n", bid, last_bid + 1);
			return T_EXIT_FAIL;
		}
		if (!(cqe->flags & IORING_CQE_F_MORE)) {
			io_uring_cqe_seen(&ring, cqe);
			break;
		}

		last_bid = bid;
		io_uring_cqe_seen(&ring, cqe);
	}while (1);

	io_uring_free_buf_ring(&ring, br, NR_BUFS, BUF_BGID);
	io_uring_queue_exit(&ring);
	free(buf);
	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	if (argc == 1)
		return T_EXIT_SKIP;
	else if (argc > 2)
		return T_EXIT_SKIP;
	if (!strcmp(argv[1], "stdin"))
		return test_stdin();
	return T_EXIT_SKIP;
}
