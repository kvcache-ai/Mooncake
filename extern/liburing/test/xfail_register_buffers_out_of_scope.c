/* SPDX-License-Identifier: MIT */
/*
 * Description: Check to see if the asan checks catch an stack-use-after-free for io_uring_sqe_set_data
 */

#include <stdio.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <assert.h>
#include <stdlib.h>
#include "liburing.h"
#include "helpers.h"

#include <stdio.h>

#define BUFFERS     8
#define BUFFER_SIZE 128

int main(int argc, char *argv[])
{
	struct io_uring ring;
	struct iovec *iovs;
	int i;
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret < 0) {
		printf("io_uring_queue_init ret %i\n", ret);
		return T_EXIT_PASS; // this test expects an inverted exit code
	}

	iovs = calloc(BUFFERS, sizeof(struct iovec));
	for (i = 0; i < BUFFERS; i++) {
		iovs[i].iov_base = malloc(BUFFER_SIZE);
		iovs[i].iov_len = BUFFER_SIZE;
	}
	// force one iov_base to be freed, test "passes" if asan catches this bug.
	free(iovs[4].iov_base);

	ret = io_uring_register_buffers(&ring, iovs, BUFFERS);
	printf("io_uring_register_buffers %i\n", ret);

	ret = io_uring_submit_and_wait(&ring, 1);
	printf("submit_and_wait %i\n", ret);

	return T_EXIT_PASS; // this test expects an inverted exit code
}
