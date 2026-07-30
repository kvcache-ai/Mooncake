/* SPDX-License-Identifier: MIT */
/*
 * Test fixed buffer merging/skipping
 *
 * Taken from: https://github.com/axboe/liburing/issues/994
 *
 */
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

#include "liburing.h"
#include "helpers.h"

int main(int argc, char *argv[])
{
	int ret, i, fd, initial_offset = 4096, num_requests = 3;
	struct io_uring ring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct iovec iov;
	char *buffer, *to_free;
	unsigned head;
	char filename[64];

	ret = io_uring_queue_init(4, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	sprintf(filename, ".fixed-buf-%d", getpid());
	t_create_file(filename, 4 * 4096);

	fd = open(filename, O_RDONLY | O_DIRECT, 0644);
	if (fd < 0) {
		if (errno == EINVAL) {
			unlink(filename);
			return T_EXIT_SKIP;
		}
		perror("open");
		goto err_unlink;
	}

	to_free = buffer = t_aligned_alloc(4096, 128 * 4096);
	if (!buffer) {
		perror("t_aligned_alloc");
		goto err_unlink;
	}

	/* Register buffer */
	iov.iov_base = buffer;
	iov.iov_len = 128 * 4096;

	ret = io_uring_register_buffers(&ring, &iov, 1);
	if (ret) {
		fprintf(stderr, "buf register: %d\n", ret);
		goto err_unlink;
	}

	/* Prepare read requests */
	buffer += initial_offset;
	for (i = 0; i < num_requests; i++) {
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_read_fixed(sqe, fd, buffer, 4096, 4096 * i, 0);
		buffer += 4096;
	}

	/* Submit requests and reap completions */
	ret = io_uring_submit_and_wait(&ring, num_requests);
	if (ret != num_requests) {
		fprintf(stderr, "Submit and wait: %d\n", ret);
		goto err_unlink;
	}

	i = 0;
	io_uring_for_each_cqe(&ring, head, cqe) {
		if (cqe->res != 4096) {
			fprintf(stderr, "cqe: %d\n", cqe->res);
			goto err_unlink;
		}
		i++;
	}

	if (i != num_requests) {
		fprintf(stderr, "Got %d completions\n", i);
		goto err_unlink;
	}

	io_uring_cq_advance(&ring, i);
	io_uring_queue_exit(&ring);
	close(fd);
	free(to_free);
	unlink(filename);
	return T_EXIT_PASS;
err_unlink:
	unlink(filename);
	return T_EXIT_FAIL;
}
