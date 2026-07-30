/* SPDX-License-Identifier: MIT */
/*
 * Description: test pollfree wakeups
 */
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/signalfd.h>
#include <unistd.h>
#include <stdlib.h>

#include "liburing.h"
#include "helpers.h"

static int no_signalfd;

static int child(int flags)
{
	struct io_uring_sqe *sqe;
	struct io_uring ring;
	struct signalfd_siginfo si;
	static unsigned long index;
	sigset_t mask;
	int ret, fd;

	ret = io_uring_queue_init(4, &ring, flags);
	if (ret) {
		if (ret == -EINVAL)
			return 0;
		fprintf(stderr, "queue init failed %d\n", ret);
		return ret;
	}

	sigemptyset(&mask);
	sigaddset(&mask, SIGINT);

	fd = signalfd(-1, &mask, SFD_NONBLOCK);
	if (fd < 0) {
		no_signalfd = 1;
		perror("signalfd");
		return 1;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_read(sqe, fd, &si, sizeof(si), 0);
	sqe->user_data = 1;
	io_uring_submit(&ring);

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_read(sqe, fd, &si, sizeof(si), 0);
	sqe->user_data = 2;
	sqe->flags |= IOSQE_ASYNC;
	io_uring_submit(&ring);

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_read(sqe, fd, &si, sizeof(si), 0);
	sqe->user_data = 3;
	io_uring_submit(&ring);

	if (!(++index & 7))
		usleep(100);

	return 0;
}

static int run_test(int flags)
{
	pid_t pid;
	int ret;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	} else if (!pid) {
		ret = child(flags);
		_exit(ret);
	} else {
		int wstatus;
		pid_t childpid;

		do {
			childpid = waitpid(pid, &wstatus, 0);
		} while (childpid == (pid_t) -1 && (errno == EINTR));

		if (errno == ECHILD)
			wstatus = 0;
		return wstatus;
	}
}

static int test(int flags)
{
	struct timeval start;
	int ret;

	gettimeofday(&start, NULL);
	do {
		ret = run_test(flags);
		if (ret) {
			fprintf(stderr, "test failed with flags %x\n", flags);
			return 1;
		}
		if (no_signalfd)
			break;
	} while (mtime_since_now(&start) < 2500);

	return 0;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test(0);
	if (ret) {
		fprintf(stderr, "test 0 failed: %d\n", ret);
		return ret;
	}

	if (no_signalfd)
		return T_EXIT_SKIP;

	ret = test(IORING_SETUP_SQPOLL);
	if (ret) {
		fprintf(stderr, "test SQPOLL failed: %d\n", ret);
		return ret;
	}

	ret = test(IORING_SETUP_COOP_TASKRUN);
	if (ret) {
		fprintf(stderr, "test COOP failed: %d\n", ret);
		return ret;
	}

	ret = test(IORING_SETUP_DEFER_TASKRUN|IORING_SETUP_SINGLE_ISSUER);
	if (ret) {
		fprintf(stderr, "test DEFER failed: %d\n", ret);
		return ret;
	}

	return T_EXIT_PASS;
}
