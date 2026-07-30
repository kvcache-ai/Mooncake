/* SPDX-License-Identifier: MIT */
/*
 * Test io_uring_setup with IORING_SETUP_REGISTERED_FD_ONLY
 *
 */
#include <stdio.h>

#include "helpers.h"

#define NORMAL_PAGE_ENTRIES	8
#define HUGE_PAGE_ENTRIES	512

static int no_mmap;

static int test_nops(struct io_uring *ring, int sq_size, int nr_nops)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int i, ret;

	do {
		int todo = nr_nops;

		if (todo > sq_size)
			todo = sq_size;

		for (i = 0; i < todo; i++) {
			sqe = io_uring_get_sqe(ring);
			io_uring_prep_nop(sqe);
		}

		ret = io_uring_submit(ring);
		if (ret != todo) {
			fprintf(stderr, "short submit %d\n", ret);
			return T_EXIT_FAIL;
		}

		for (i = 0; i < todo; i++) {
			ret = io_uring_wait_cqe(ring, &cqe);
			if (ret) {
				fprintf(stderr, "wait err %d\n", ret);
				return T_EXIT_FAIL;
			}
			io_uring_cqe_seen(ring, cqe);
		}
		nr_nops -= todo;
	} while (nr_nops);

	return T_EXIT_PASS;
}

static int test(int nentries, int ring_flags)
{
	struct io_uring ring;
	unsigned values[2];
	int ret;

	ret = io_uring_queue_init(nentries, &ring,
			IORING_SETUP_REGISTERED_FD_ONLY | IORING_SETUP_NO_MMAP |
			ring_flags);
	if (ret == -EINVAL || ret == -ENOENT) {
		no_mmap = 1;
		return T_EXIT_SKIP;
	} else if (ret == -ENOMEM) {
		fprintf(stdout, "Enable huge pages to test big rings\n");
		return T_EXIT_SKIP;
	} else if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_ring_fd(&ring);
	if (ret != -EEXIST) {
		fprintf(stderr, "registering already-registered ring fd should fail\n");
		goto err;
	}

	ret = io_uring_close_ring_fd(&ring);
	if (ret != -EBADF) {
		fprintf(stderr, "closing already-closed ring fd should fail\n");
		goto err;
	}

	/* Test a simple io_uring_register operation expected to work.
	 * io_uring_register_iowq_max_workers is arbitrary.
	 */
	values[0] = values[1] = 0;
	ret = io_uring_register_iowq_max_workers(&ring, values);
	if (ret || (values[0] == 0 && values[1] == 0)) {
		fprintf(stderr, "io_uring_register operation failed after closing ring fd\n");
		goto err;
	}

	ret = test_nops(&ring, nentries, nentries * 4);
	if (ret)
		goto err;

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;

err:
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	/* test single normal page */
	ret = test(NORMAL_PAGE_ENTRIES, 0);
	if (ret == T_EXIT_SKIP || no_mmap) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test 8 failed\n");
		return T_EXIT_FAIL;
	}

	/* test single normal page */
	ret = test(NORMAL_PAGE_ENTRIES, IORING_SETUP_SQPOLL);
	if (ret == T_EXIT_SKIP || no_mmap) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test 8 failed\n");
		return T_EXIT_FAIL;
	}

	/* test with entries requiring a huge page */
	ret = test(HUGE_PAGE_ENTRIES, 0);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test 512 failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}
