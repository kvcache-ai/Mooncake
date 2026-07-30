/* SPDX-License-Identifier: MIT */
/*
 * Description: Test O_NONBLOCK reading from fifo, should result in proper
 *		retry and a positive read results. Buggy result would be
 *		-EAGAIN being returned to the user.
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "liburing.h"
#include "helpers.h"

int main(int argc, char *argv[])
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	char buf[32];
	int fds[2];
	int ret;

	io_uring_queue_init(1, &ring, 0);

	if (pipe(fds) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	t_set_nonblock(fds[0]);

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_read(sqe, fds[0], buf, sizeof(buf), 0);
	io_uring_submit(&ring);

	usleep(10000);

	ret = write(fds[1], "Hello\n", 6);
	if (ret < 0) {
		perror("pipe write");
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait=%d\n", ret);
		return T_EXIT_FAIL;
	}

	if (cqe->res < 0) {
		fprintf(stderr, "cqe res %d\n", cqe->res);
		return T_EXIT_FAIL;
	}

	io_uring_cqe_seen(&ring, cqe);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}
