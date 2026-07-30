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

static int fail_cqe(struct io_uring_cqe *cqe, int off, const char *msg)
{
	fprintf(stderr, "Bad cqe at off %d: %s\n", off, msg);
	fprintf(stderr, "CQE ud=%lu, res=%u, flags=%x\n", (long) cqe->user_data, cqe->res, cqe->flags);
	return T_EXIT_FAIL;
}

static int test_ring(unsigned flags)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	struct io_uring_params p = { };
	unsigned head;
	int ret, i, cqe_nr;

	p.flags = flags | IORING_SETUP_CQSIZE;
	p.cq_entries = 8;
	ret = io_uring_queue_init_params(8, &ring, &p);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/*
	 * Prep 8 NOP requests, with the first being a 16b one to
	 * unalign the ring. The 4 latter ones will hit overflow,
	 * and after the first 4 we should see a SKIP cqe.
	 */
	for (i = 0; i < 8; i++) {
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_nop(sqe);
		if (i)
			sqe->nop_flags = IORING_NOP_CQE32;
		sqe->user_data = ++seq;
	}

	io_uring_submit(&ring);

	/*
	 * Should find the following CQEs:
	 *
	 * 1: 16b with ud 1
	 * 2: 32b with ud 2
	 * 3: 32b with ud 3
	 * 4: 32b with ud 4
	 * 5: 16b skip CQE
	 *
	 * And then we should have overflow pending to flush the rest
	 */
	i = cqe_nr = 0;
	io_uring_for_each_cqe(&ring, head, cqe) {
		switch (i) {
		case 0:
			if (cqe->user_data != 1)
				return fail_cqe(cqe, i, "Non-overflow UD");
			if (cqe->flags & IORING_CQE_F_32)
				return fail_cqe(cqe, i, "Non-overflow flags");
			break;
		case 1:
		case 2:
		case 3:
			if (cqe->user_data != i + 1)
				return fail_cqe(cqe, i, "Non-overflow 32b UD");
			if (!(cqe->flags & IORING_CQE_F_32))
				return fail_cqe(cqe, i, "Non-overflow 32b flags");
			break;
		case 4:
			if (!(cqe->flags & IORING_CQE_F_SKIP))
				return fail_cqe(cqe, i, "Non-overflow skip");
			break;
		default:
			return fail_cqe(cqe, i, "Bogus CQE");
		}
		i++;
		cqe_nr += io_uring_cqe_nr(cqe);
	}
	io_uring_cq_advance(&ring, cqe_nr);

	/*
	 * This should flush overflow, and we should see:
	 *
	 * 1: 32b with ud 5
	 * 2: 32b with ud 6
	 * 3: 32b with ud 7
	 * 4: 32b with ud 8
	 */
	for (i = 0; i < 4; i++) {
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait ret %d\n", ret);
			return T_EXIT_FAIL;
		}
		switch (i) {
		case 0:
		case 1:
		case 2:
		case 3:
			if (cqe->user_data != i + 5)
				return fail_cqe(cqe, i + 5, "Overflow UD");
			if (!(cqe->flags & IORING_CQE_F_32))
				return fail_cqe(cqe, i + 5, "Overflow flags");
			break;
		default:
			return fail_cqe(cqe, i + 5, "Bogus CQE");
		}
		io_uring_cqe_seen(&ring, cqe);
	}

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
