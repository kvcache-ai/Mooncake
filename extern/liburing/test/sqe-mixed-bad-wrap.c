/* SPDX-License-Identifier: MIT */
/*
 * Description: run various nop tests
 *
 */
#include <stdio.h>

#include "liburing.h"
#include "helpers.h"
#include "test.h"

static int seq;

static int test_single_nop(struct io_uring *ring, bool should_fail)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		return T_EXIT_FAIL;
	}

	if (should_fail)
		io_uring_prep_nop128(sqe);
	else
		io_uring_prep_nop(sqe);
	sqe->user_data = ++seq;

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
	} else if (should_fail && cqe->res == 0) {
		fprintf(stderr, "Unexpected success\n");
	} else if (!should_fail && cqe->res != 0) {
		fprintf(stderr, "Completion error:%d\n", cqe->res);
	} else if (cqe->res == 0 && cqe->user_data != seq) {
		fprintf(stderr, "Unexpected user_data: %ld\n", (long) cqe->user_data);
	} else {
		io_uring_cqe_seen(ring, cqe);
		return T_EXIT_PASS;
	}
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	int ret, i;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(8, &ring, IORING_SETUP_SQE_MIXED);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* prime the sq to the last entry before wrapping */
	for (i = 0; i < 7; i++) {
		ret = test_single_nop(&ring, false);
		if (ret != T_EXIT_PASS)
			goto done;
	}

	/* inserting a 128b sqe in the last entry should fail */
	ret = test_single_nop(&ring, true);
	if (ret != T_EXIT_PASS)
		goto done;

	/* proceeding from the bad wrap should succeed */
	ret = test_single_nop(&ring, false);
done:
	io_uring_queue_exit(&ring);
	return ret;
}
