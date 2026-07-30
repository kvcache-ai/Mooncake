/* SPDX-License-Identifier: MIT */
/*
 * Description: Test FUTEX2_PRIVATE with an active waiter being killed
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <linux/futex.h>

#include "liburing.h"
#include "helpers.h"

#ifndef FUTEX2_SIZE_U32
#define FUTEX2_SIZE_U32		0x02
#endif
#ifndef FUTEX2_PRIVATE
#define FUTEX2_PRIVATE		128
#endif

static int do_child(int ring_flags, int async, int vectored)
{
	struct io_uring_params p = { .flags = ring_flags, };
	struct io_uring_sqe *sqe;
	struct io_uring ring;
	struct futex_waitv fw = { };
	unsigned int *futex;
	int ret;

	ret = t_create_ring_params(16, &ring, &p);
	if (ret) {
		if (ret == T_SETUP_SKIP)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring setup failed\n");
		return T_EXIT_FAIL;
	}

	futex = malloc(sizeof(*futex));
	*futex = 0;
	fw.uaddr = (unsigned long) futex;
	fw.flags = FUTEX2_SIZE_U32|FUTEX2_PRIVATE;

	sqe = io_uring_get_sqe(&ring);
	if (!vectored)
		io_uring_prep_futex_wait(sqe, futex, 0, FUTEX_BITSET_MATCH_ANY,
					FUTEX2_SIZE_U32|FUTEX2_PRIVATE, 0);
	else
		io_uring_prep_futex_waitv(sqe, &fw, 1, 0);
	if (async)
		sqe->flags |= IOSQE_ASYNC;
	io_uring_submit(&ring);
	return T_EXIT_PASS;
}

static int test(int sqpoll, int async, int vectored)
{
	int status, ring_flags = 0;
	pid_t pid;

	if (sqpoll)
		ring_flags |= IORING_SETUP_SQPOLL;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return T_EXIT_FAIL;
	} else if (!pid) {
		exit(do_child(ring_flags, async, vectored));
	}

	usleep(10000);
	kill(pid, 9);
	waitpid(pid, &status, 0);
	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	int ret;

	ret = test(0, 0, 0);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test 0 0 0 failed\n");
		return T_EXIT_FAIL;
	} else if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	}

	ret = test(0, 1, 0);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test 0 1 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(0, 0, 1);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test 0 0 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(0, 1, 1);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test 0 1 1 failed\n");
		return T_EXIT_FAIL;
	}


	ret = test(IORING_SETUP_SQPOLL, 0, 0);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test SQPOLL 0 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(IORING_SETUP_SQPOLL, 1, 0);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test SQPOLL 1 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(IORING_SETUP_SQPOLL, 0, 1);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test SQPOLL 0 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(IORING_SETUP_SQPOLL, 1, 1);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test SQPOLL 1 1 failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}
