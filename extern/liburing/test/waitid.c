/* SPDX-License-Identifier: MIT */
/*
 * Description: test waitid functionality
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "liburing.h"
#include "helpers.h"

static bool no_waitid;

static void child(long usleep_time)
{
	if (usleep_time)
		usleep(usleep_time);
	exit(0);
}

static int test_invalid_infop(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	siginfo_t *si = (siginfo_t *) (uintptr_t) 0x1234;
	int ret, w;
	pid_t pid;

	pid = fork();
	if (!pid) {
		child(200000);
		exit(0);
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_waitid(sqe, P_PID, pid, si, WEXITED, 0);
	sqe->user_data = 1;
	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "cqe wait: %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res != -EFAULT) {
		fprintf(stderr, "Bad return on invalid infop: %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(ring, cqe);
	wait(&w);
	return T_EXIT_PASS;
}

/*
 * Test linked timeout with child not exiting in time
 */
static int test_noexit(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct __kernel_timespec ts;
	int ret, i, w;
	siginfo_t si;
	pid_t pid;

	pid = fork();
	if (!pid) {
		child(200000);
		exit(0);
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_waitid(sqe, P_PID, pid, &si, WEXITED, 0);
	sqe->flags |= IOSQE_IO_LINK;
	sqe->user_data = 1;

	ts.tv_sec = 0;
	ts.tv_nsec = 100 * 1000 * 1000ULL;
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_link_timeout(sqe, &ts, 0);
	sqe->user_data = 2;

	io_uring_submit(ring);

	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "cqe wait: %d\n", ret);
			return T_EXIT_FAIL;
		}
		if (cqe->user_data == 2 && cqe->res != 1) {
			fprintf(stderr, "timeout res: %d\n", cqe->res);
			return T_EXIT_FAIL;
		}
		if (cqe->user_data == 1 && cqe->res != -ECANCELED) {
			fprintf(stderr, "waitid res: %d\n", cqe->res);
			return T_EXIT_FAIL;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	wait(&w);
	return T_EXIT_PASS;
}

/*
 * Test one child exiting, but not the one we were looking for
 */
static int test_double(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	siginfo_t si;
	pid_t p1, p2;
	int ret, w;

	/* p1 will exit shortly */
	p1 = fork();
	if (!p1) {
		child(100000);
		exit(0);
	}

	/* p2 will linger */
	p2 = fork();
	if (!p2) {
		child(200000);
		exit(0);
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_waitid(sqe, P_PID, p2, &si, WEXITED, 0);

	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "cqe wait: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (cqe->res < 0) {
		fprintf(stderr, "cqe res: %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	if (si.si_pid != p2) {
		fprintf(stderr, "expected pid %d, got %d\n", p2, si.si_pid);
		return T_EXIT_FAIL;
	}

	io_uring_cqe_seen(ring, cqe);
	wait(&w);
	return T_EXIT_PASS;
}

/*
 * Test reaping of an already exited task
 */
static int test_ready(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	siginfo_t si;
	pid_t pid;
	int ret;

	pid = fork();
	if (!pid) {
		child(0);
		exit(0);
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_waitid(sqe, P_PID, pid, &si, WEXITED, 0);

	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "cqe wait: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (cqe->res < 0) {
		fprintf(stderr, "cqe res: %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	if (si.si_pid != pid) {
		fprintf(stderr, "expected pid %d, got %d\n", pid, si.si_pid);
		return T_EXIT_FAIL;
	}

	io_uring_cqe_seen(ring, cqe);
	return T_EXIT_PASS;
}

/*
 * Test cancelation of pending waitid
 */
static int test_cancel(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, i, w;
	pid_t pid;

	pid = fork();
	if (!pid) {
		child(20000);
		exit(0);
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_waitid(sqe, P_PID, pid, NULL, WEXITED, 0);
	sqe->user_data = 1;

	io_uring_submit(ring);

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_cancel64(sqe, 1, 0);
	sqe->user_data = 2;

	io_uring_submit(ring);

	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "cqe wait: %d\n", ret);
			return T_EXIT_FAIL;
		}
		if (cqe->user_data == 1 && cqe->res != -ECANCELED) {
			fprintf(stderr, "cqe res: %d\n", cqe->res);
			return T_EXIT_FAIL;
		}
		if (cqe->user_data == 2 && cqe->res != 1) {
			fprintf(stderr, "cqe res: %d\n", cqe->res);
			return T_EXIT_FAIL;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	wait(&w);
	return T_EXIT_PASS;
}

/*
 * Test cancelation of pending waitid, with expected races that either
 * waitid trigger or cancelation will win.
 */
static int test_cancel_race(struct io_uring *ring, int async)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, i, to_wait, total_forks;
	pid_t pid;

	total_forks = 0;
	for (i = 0; i < 10; i++) {
		total_forks++;
		pid = fork();
		if (!pid) {
			child(getpid() & 1);
			exit(0);
		}
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_waitid(sqe, P_ALL, -1, NULL, WEXITED, 0);
	if (async)
		sqe->flags |= IOSQE_ASYNC;
	sqe->user_data = 1;

	io_uring_submit(ring);

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_cancel64(sqe, 1, 0);
	sqe->user_data = 2;

	usleep(1);

	io_uring_submit(ring);

	to_wait = total_forks;
	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "cqe wait: %d\n", ret);
			return T_EXIT_FAIL;
		}
		if (cqe->user_data == 1) {
			if (!cqe->res)
				to_wait--;
			if (!(cqe->res == -ECANCELED || cqe->res == 0)) {
				fprintf(stderr, "cqe1 res: %d\n", cqe->res);
				return T_EXIT_FAIL;
			}
		}
		if (cqe->user_data == 2 &&
		    !(cqe->res == 1 || cqe->res == 0 || cqe->res == -ENOENT ||
		      cqe->res == -EALREADY)) {
			fprintf(stderr, "cqe2 res: %d\n", cqe->res);
			return T_EXIT_FAIL;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	for (i = 0; i < to_wait; i++) {
		int w;

		wait(&w);
	}

	return T_EXIT_PASS;
}

/*
 * Test basic reap of child exit
 */
static int test(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	siginfo_t si;
	pid_t pid;
	int ret;

	pid = fork();
	if (!pid) {
		child(100);
		exit(0);
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_waitid(sqe, P_PID, pid, &si, WEXITED, 0);

	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "cqe wait: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* no waitid support */
	if (cqe->res == -EINVAL) {
		no_waitid = true;
		return T_EXIT_SKIP;
	}
	if (cqe->res < 0) {
		fprintf(stderr, "cqe res: %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	if (si.si_pid != pid) {
		fprintf(stderr, "expected pid %d, got %d\n", pid, si.si_pid);
		return T_EXIT_FAIL;
	}

	io_uring_cqe_seen(ring, cqe);
	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	int ret, i;

	if (argc > 1)
		return T_EXIT_SKIP;

	io_uring_queue_init(8, &ring, 0);

	ret = test(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test failed\n");
		return T_EXIT_FAIL;
	}
	if (no_waitid)
		return T_EXIT_SKIP;

	ret = test_noexit(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_noexit failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_noexit(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_noexit failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_double(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_double failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_ready(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_ready failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_cancel(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_cancel failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_invalid_infop(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_invalid_infop failed\n");
		return T_EXIT_FAIL;
	}

	for (i = 0; i < 1000; i++) {
		ret = test_cancel_race(&ring, i & 1);
		if (ret == T_EXIT_FAIL) {
			fprintf(stderr, "test_cancel_race failed\n");
			return T_EXIT_FAIL;
		}
	}

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}
