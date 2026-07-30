/* SPDX-License-Identifier: MIT */
/*
 * Description: Check to see if the asan checks catch an stack-use-after-free for prep_link_timeout
 */

#include <stdio.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <assert.h>
#include "liburing.h"
#include "helpers.h"

#include <stdio.h>

int main(int argc, char *argv[])
{
	struct io_uring ring;
	struct io_uring_sqe *sqe;
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret < 0) {
		printf("io_uring_queue_init ret %i\n", ret);
		return T_EXIT_PASS; // this test expects an inverted exit code
	}

	// force timespec to go out of scope, test "passes" if asan catches this bug.
	{
		struct __kernel_timespec timespec;
		timespec.tv_sec = 0;
		timespec.tv_nsec = 5000;

		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_timeout(sqe, &timespec, 0, 0);
		io_uring_sqe_set_data(sqe, (void *) 1);
	}

	ret = io_uring_submit_and_wait(&ring, 1);
	printf("submit_and_wait %i\n", ret);

	return T_EXIT_PASS; // this test expects an inverted exit code
}
