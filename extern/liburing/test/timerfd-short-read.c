/* SPDX-License-Identifier: MIT */
/*
 * Description: Check that trying to read two eventfd events will still
 *		return one when generated. There was a kernel commit that
 *		all of a sudden pretended that anonymous inodes were
 *		regular files, which broke the io_uring short read/write
 *		handling logic. See:
 *
 *		https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=cfd86ef7e8e7
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/timerfd.h>

#include "liburing.h"
#include "helpers.h"

static void sig_alrm(int sig)
{
	fprintf(stderr, "Test failed due to timeout\n");
	exit(T_EXIT_FAIL);
}

int main(int argc, char *argv[])
{
	struct io_uring_params p = {};
	struct sigaction act = { };
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	struct itimerspec is = { };
	unsigned long tmp[2];
	int ret, fd;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init_params(8, &ring, &p);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	fd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);
	if (fd < 0) {
		perror("timerfd_create");
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_read(sqe, fd, tmp, sizeof(tmp), 0);
	sqe->user_data = 1;

	io_uring_submit(&ring);

	is.it_value.tv_sec = 0;
	is.it_value.tv_nsec = 10000000;
	ret = timerfd_settime(fd, 0, &is, NULL);
	if (ret < 0) {
		perror("timerfd_settime");
		return T_EXIT_FAIL;
	}

	act.sa_handler = sig_alrm;
	sigaction(SIGALRM, &act, NULL);
	alarm(1);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait: %d\n", ret);
		return T_EXIT_FAIL;
	}

	close(fd);
	io_uring_cqe_seen(&ring, cqe);
	return T_EXIT_PASS;
}
