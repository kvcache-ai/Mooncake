/* SPDX-License-Identifier: MIT */
/*
 * Description: Test that out-of-order file updates with inflight requests
 *		work as expected.
 *
 */
#include <stdio.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>

#include "liburing.h"
#include "helpers.h"

int main(int argc, char *argv[])
{
	struct io_uring_sqe *sqe;
	int res, fds[2], sockid;
	struct io_uring ring;

	if (argc > 1)
		return T_EXIT_SKIP;

	res = io_uring_queue_init(1, &ring, 0);
	if (res) {
		fprintf(stderr, "queue_init: %d\n", res);
		return T_EXIT_FAIL;
	}

	res = io_uring_register_files_sparse(&ring, 2);
	if (res) {
		if (res == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "sparse reg: %d\n", res);
		return T_EXIT_FAIL;
	}

	fds[0] = socket(AF_INET, SOCK_DGRAM, 0);
	if (fds[0] < 0) {
		perror("socket");
		return T_EXIT_FAIL;
	}
	fds[1] = socket(AF_INET, SOCK_DGRAM, 0);
	if (fds[1] < 0) {
		perror("socket");
		return T_EXIT_FAIL;
	}

	res = io_uring_register_files_update(&ring, 0, fds, 2);
	if (res != 2) {
		fprintf(stderr, "files updates; %d\n", res);
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_poll_add(sqe, 0, POLLIN);
	sqe->flags = IOSQE_FIXED_FILE;
	io_uring_submit(&ring);

	close(fds[0]);
	close(fds[1]);

	sockid = -1;
	res = io_uring_register_files_update(&ring, 1, &sockid, 1);
	if (res != 1) {
		fprintf(stderr, "files updates; %d\n", res);
		return T_EXIT_FAIL;
	}

	sockid = -1;
	res = io_uring_register_files_update(&ring, 0, &sockid, 1);
	if (res != 1) {
		fprintf(stderr, "files updates; %d\n", res);
		return T_EXIT_FAIL;
	}

	sleep(1);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}
