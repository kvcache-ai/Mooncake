/* SPDX-License-Identifier: MIT */
/*
 * Description: test that a defer task_work file with tags unregistration
 *		doesn't trigger a lockdep violation
 *
 */
#include <stdio.h>
#include <inttypes.h>
#include <stdlib.h>
#include <unistd.h>

#include "liburing.h"
#include "helpers.h"

int main(int argc, char *argv[])
{
	__u64 tags[2] = { 1, 2 };
	struct io_uring ring;
	int fds[2], ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	if (pipe(fds) < 0) {
		perror("pipe");
		return 1;
	}

	ret = io_uring_queue_init(4, &ring, IORING_SETUP_SINGLE_ISSUER|IORING_SETUP_DEFER_TASKRUN);
	if (ret == -EINVAL) {
		return T_EXIT_SKIP;
	} else if (ret < 0) {
		fprintf(stderr, "queue_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_files_tags(&ring, fds, tags, 2);
	if (ret == -EINVAL) {
		return T_EXIT_SKIP;
	} else if (ret < 0) {
		fprintf(stderr, "file_register_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	io_uring_queue_exit(&ring);
	sleep(1);
	return 0;
}
