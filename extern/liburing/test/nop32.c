/* SPDX-License-Identifier: MIT */
/*
 * Description: run various nop tests
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#include "liburing.h"
#include "helpers.h"
#include "test.h"

static int seq;
static int extra1 = 100, extra2 = 200;
static int want_extra1 = 100, want_extra2 = 200;

static int test_single_nop(struct io_uring *ring, unsigned req_flags, bool cqe32)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		goto err;
	}

	io_uring_prep_nop(sqe);
	sqe->user_data = ++seq;
	sqe->flags |= req_flags;
	if (cqe32) {
		sqe->nop_flags = IORING_NOP_CQE32;
		sqe->off = extra1++;
		sqe->addr = extra2++;
	}

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
		goto err;
	}
	if (!cqe->user_data) {
		fprintf(stderr, "Unexpected 0 user_data: %ld\n", (long) cqe->user_data);
		goto err;
	}
	if (cqe32) {
		if (!(cqe->flags & IORING_CQE_F_32)) {
			fprintf(stderr, "CQE_F_32 not set\n");
			goto err;
		}
		if (cqe->big_cqe[0] != want_extra1) {
			fprintf(stderr, "Unexpected extra1: %ld, want %d\n", (long) cqe->big_cqe[0], want_extra1);
			goto err;

		}
		want_extra1++;
		if (cqe->big_cqe[1] != want_extra2) {
			fprintf(stderr, "Unexpected extra2: %ld, want %d\n", (long) cqe->big_cqe[1], want_extra2);
			goto err;
		}
		want_extra2++;
	}
	io_uring_cqe_seen(ring, cqe);
	return T_EXIT_PASS;
err:
	return T_EXIT_FAIL;
}

static int test_ring(unsigned flags)
{
	struct io_uring ring;
	struct io_uring_params p = { };
	int ret, i;

	p.flags = flags;
	ret = io_uring_queue_init_params(8, &ring, &p);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	test_single_nop(&ring, 0, 0);

	for (i = 0; i < 16; i++) {
		ret = test_single_nop(&ring, 0, 1);
		if (ret) {
			printf("fail off %d\n", i);
			fprintf(stderr, "test_single_nop failed\n");
			goto err;
		}
	}
err:
	io_uring_queue_exit(&ring);
	return ret;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_ring(IORING_SETUP_CQE_MIXED);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "Mixed ring test failed\n");
		return ret;
	}

	return T_EXIT_PASS;
}
