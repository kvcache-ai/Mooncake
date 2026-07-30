/* SPDX-License-Identifier: MIT */
/*
 * Test that the sqthread goes to sleep around the specified time, and that
 * the NEED_WAKEUP flag is then set.
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include "liburing.h"
#include "helpers.h"

int main(int argc, char *argv[])
{
	struct io_uring_params p = {};
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct timeval tv;
	struct io_uring ring;
	unsigned long elapsed;
	bool seen_wakeup;
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	p.flags = IORING_SETUP_SQPOLL;
	p.sq_thread_idle = 100;

	ret = io_uring_queue_init_params(1, &ring, &p);
	if (ret) {
		if (geteuid()) {
			printf("%s: skipped, not root\n", argv[0]);
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "queue_init=%d\n", ret);
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_nop(sqe);
	io_uring_submit(&ring);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe: %d\n", ret);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(&ring, cqe);

	elapsed = 0;
	seen_wakeup = false;
	gettimeofday(&tv, NULL);
	do {
		usleep(100);
		if (IO_URING_READ_ONCE(*ring.sq.kflags) & IORING_SQ_NEED_WAKEUP) {
			seen_wakeup = true;
			break;
		}
		elapsed = mtime_since_now(&tv);
	} while (elapsed < 1000);

	if (!seen_wakeup) {
		fprintf(stderr, "SQPOLL didn't flag wakeup\n");
		return T_EXIT_FAIL;
	}

	/* should be around 100 msec */
	if (elapsed < 90 || elapsed > 110) {
		fprintf(stderr, "SQPOLL wakeup timing off %lu\n", elapsed);
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}
