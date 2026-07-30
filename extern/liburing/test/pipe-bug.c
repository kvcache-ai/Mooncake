// SPDX-License-Identifier: MIT

/*
 * Description: tests bug fixed in
 * "io_uring: don't gate task_work run on TIF_NOTIFY_SIGNAL"
 *
 * See: https://github.com/axboe/liburing/issues/665
 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "helpers.h"
#include "liburing.h"

#define CHECK(x)								\
do {										\
	if (!(x)) {								\
		fprintf(stderr, "%s:%d %s failed\n", __FILE__, __LINE__, #x);	\
		return -1;							\
	}									\
} while (0)

static int pipe_bug(void)
{
	struct io_uring ring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	char buf[1024];
	int ret, fds[2];
	struct __kernel_timespec to = {
		.tv_sec = 1
	};

	ret = io_uring_queue_init(8, &ring, 0);
	/* can hit -ENOMEM due to repeated ring creation and teardowns */
	if (ret == -ENOMEM) {
		usleep(1000);
		return 0;
	} else if (ret) {
		fprintf(stderr, "ring_init: %d\n", ret);
		return 1;
	}

	CHECK(pipe(fds) == 0);

	/* WRITE */
	sqe = io_uring_get_sqe(&ring);
	CHECK(sqe);
	io_uring_prep_write(sqe, fds[1], "foobar", strlen("foobar"), 0); /* or -1 */
	CHECK(io_uring_submit(&ring) == 1);
	CHECK(io_uring_wait_cqe(&ring, &cqe) == 0);

	io_uring_cqe_seen(&ring, cqe);

	/* CLOSE */
	sqe = io_uring_get_sqe(&ring);
	CHECK(sqe);
	io_uring_prep_close(sqe, fds[1]);
	CHECK(io_uring_submit(&ring) == 1);
	CHECK(io_uring_wait_cqe_timeout(&ring, &cqe, &to) == 0);
	io_uring_cqe_seen(&ring, cqe);

	/* READ */
	sqe = io_uring_get_sqe(&ring);
	CHECK(sqe);
	io_uring_prep_read(sqe, fds[0], buf, sizeof(buf), 0); /* or -1 */
	CHECK(io_uring_submit(&ring) == 1);
	CHECK(io_uring_wait_cqe_timeout(&ring, &cqe, &to) == 0);
	io_uring_cqe_seen(&ring, cqe);
	memset(buf, 0, sizeof(buf));

	/* READ */
	sqe = io_uring_get_sqe(&ring);
	CHECK(sqe);
	io_uring_prep_read(sqe, fds[0], buf, sizeof(buf), 0); /* or -1 */
	CHECK(io_uring_submit(&ring) == 1);
	CHECK(io_uring_wait_cqe_timeout(&ring, &cqe, &to) == 0);
	io_uring_cqe_seen(&ring, cqe);

	close(fds[0]);
	io_uring_queue_exit(&ring);

	return 0;
}

int main(int argc, char *argv[])
{
	int i;

	if (argc > 1)
		return T_EXIT_SKIP;

	for (i = 0; i < 10000; i++) {
		if (pipe_bug())
			return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}
