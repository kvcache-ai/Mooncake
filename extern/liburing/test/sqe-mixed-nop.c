/* SPDX-License-Identifier: MIT */
/*
 * Description: run various nop tests
 *
 */
#include <stdio.h>

#include "liburing.h"
#include "helpers.h"
#include "test.h"

#define NENTRIES	8

static int seq;

static int test_single_nop(struct io_uring *ring, bool sqe128, int offset)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret;

	if (sqe128)
		sqe = io_uring_get_sqe128(ring);
	else
		sqe = io_uring_get_sqe(ring);

	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		return T_EXIT_FAIL;
	}

	if (sqe128)
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
	} else if (cqe->res != 0) {
		fprintf(stderr, "Completion error:%d\n", cqe->res);
	} else if (cqe->user_data != seq) {
		fprintf(stderr, "Unexpected user_data: %ld\n", (long) cqe->user_data);
	} else {
		io_uring_cqe_seen(ring, cqe);
		return T_EXIT_PASS;
	}
	return T_EXIT_FAIL;
}

static int check_sq(struct io_uring *ring, unsigned int ready,
		    unsigned int left, const char *msg)
{
	unsigned int pspace, pready;

	pready = io_uring_sq_ready(ring);
	if (pready != ready) {
		fprintf(stderr, "%s: %d ready, should be %d\n", msg, pready, ready);
		return 1;
	}
	pspace = io_uring_sq_space_left(ring);
	if (pspace != left) {
		fprintf(stderr, "%s: %d entries, should be %d\n", msg, pspace, left);
		return 1;
	}

	return 0;
}

static void submit_and_reap(struct io_uring *ring, int to_reap, const char *msg)
{
	struct io_uring_cqe *cqe;
	unsigned long ud = 1000;
	int i;

	io_uring_submit(ring);

	for (i = 0; i < to_reap; i++) {
		io_uring_wait_cqe(ring, &cqe);
		if (cqe->res < 0) {
			fprintf(stderr, "cqe res %d\n", cqe->res);
			io_uring_cqe_seen(ring, cqe);
			continue;
		}
		if (cqe->user_data != ud)
			fprintf(stderr, "%s: cqe=%d, %ld != %ld\n", msg, i, (long) cqe->user_data, ud);
		ud++;
		io_uring_cqe_seen(ring, cqe);
	}
}

static int test_sq_space(struct io_uring *ring, unsigned int flags)
{
	struct io_uring_sqe *sqe;
	int nop_adj;

	/*
	 * For SQ_REWIND, sqe_tail resets to 0 after each submit, so we always
	 * start at position 0 and never hit wrap cases. For non-SQ_REWIND,
	 * after 32 test_single_nop iterations, sqe_tail ends up at position 7
	 * (modulo 8), so step5's sqe128 triggers the wrap case with a NOP.
	 */
	nop_adj = 0;
	if (!(flags & IORING_SETUP_SQ_REWIND))
		nop_adj = 1;

	if (check_sq(ring, 0, NENTRIES, "initial"))
		return 1;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	sqe->user_data = 1000;

	if (check_sq(ring, 1, NENTRIES - 1, "step1"))
		return 1;

	sqe = io_uring_get_sqe128(ring);
	io_uring_prep_nop128(sqe);
	sqe->user_data = 1001;

	if (check_sq(ring, 3, NENTRIES - 3, "step2"))
		return 1;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	sqe->user_data = 1002;

	if (check_sq(ring, 4, NENTRIES - 4, "step3"))
		return 1;

	submit_and_reap(ring, 3, "round1");

	if (check_sq(ring, 0, NENTRIES, "step4"))
		return 1;

	sqe = io_uring_get_sqe128(ring);
	io_uring_prep_nop128(sqe);
	sqe->user_data = 1000;

	/* SQ_REWIND: no wrap at position 0, just 2 slots. Non-rewind: wrap at 7, 3 slots */
	if (check_sq(ring, 2 + nop_adj, NENTRIES - 2 - nop_adj, "step5"))
		return 1;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	sqe->user_data = 1001;

	if (check_sq(ring, 3 + nop_adj, NENTRIES - 3 - nop_adj, "step6"))
		return 1;

	sqe = io_uring_get_sqe128(ring);
	io_uring_prep_nop128(sqe);
	sqe->user_data = 1002;

	if (check_sq(ring, 5 + nop_adj, NENTRIES - 5 - nop_adj, "step7"))
		return 1;

	submit_and_reap(ring, 3, "round2");
	return 0;
}

static int test_flags(unsigned int flags)
{
	struct io_uring ring;
	int ret, i;

	ret = io_uring_queue_init(NENTRIES, &ring, IORING_SETUP_SQE_MIXED | flags);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* alternate big and little sqe's */
	for (i = 0; i < 32; i++) {
		ret = test_single_nop(&ring, i & 1, i);
		if (ret != T_EXIT_PASS)
			break;
	}

	if (ret) {
		fprintf(stderr, "Single nop test failed\n");
		io_uring_queue_exit(&ring);
		return ret;
	}

	ret = test_sq_space(&ring, flags);
	io_uring_queue_exit(&ring);
	return ret;

}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_flags(0);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret) {
		fprintf(stderr, "test flags 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_flags(IORING_SETUP_SQ_REWIND);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret) {
		fprintf(stderr, "test flags REWIND failed\n");
		return T_EXIT_FAIL;
	}

	return ret;
}
