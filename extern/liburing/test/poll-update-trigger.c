/* SPDX-License-Identifier: MIT */
/*
 * Description: test POLL_ADD being added with a mask that doesn't make it
 *		trigger, but then updated with POLL_REMOVE to a mask that does
 *		trigger it.
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <poll.h>

#include "liburing.h"
#include "helpers.h"

int main(int argc, char *argv[])
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int ret, fd[2], i;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(4, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (pipe(fd) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	/*
	 * Attach POLLIN request to output side of pipe, won't complete as
	 * that will never be readabable.
	 */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_poll_add(sqe, fd[1], POLLIN);
	sqe->user_data = 1;
	io_uring_submit(&ring);

	/*
	 * Now update the previously submitted poll request, adding POLLOUT.
	 * This will cause it to trigger immediately, as the pipe is indeed
	 * writeable.
	 */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_poll_remove(sqe, 1);
	sqe->len = IORING_POLL_UPDATE_EVENTS;
	sqe->poll32_events = POLLOUT;
	sqe->user_data = 2;
	io_uring_submit(&ring);

	/*
	 * Expect both requests to complete - the update with 0 for success,
	 * and the original poll_add with POLLOUT as the result.
	 */
	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe: %d\n", ret);
			return T_EXIT_FAIL;
		}
		if (cqe->user_data == 1) {
			if (cqe->res != POLLOUT) {
				fprintf(stderr, "Original poll: %d\n", cqe->res);
				return T_EXIT_FAIL;
			}
		} else if (cqe->user_data == 2) {
			if (cqe->res) {
				fprintf(stderr, "Poll update: %d\n", cqe->res);
				return T_EXIT_FAIL;
			}
		}
		io_uring_cqe_seen(&ring, cqe);
	}

	return T_EXIT_PASS;
}
