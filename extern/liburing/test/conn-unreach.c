/* SPDX-License-Identifier: MIT */
/*
 * Check that IORING_OP_CONNECT properly returns -ECONNRESET when
 * attempting to connect to an unreachable address. See:
 *
 * https://github.com/axboe/liburing/discussions/1335
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "liburing.h"
#include "helpers.h"

static int check_cqe(struct io_uring *ring, struct io_uring_cqe *cqe)
{
	if (cqe->res == -EINVAL)
		return T_EXIT_SKIP;

	switch (cqe->user_data) {
	case 1:
		if (cqe->res != -ECONNRESET && cqe->res != -ENETUNREACH) {
			fprintf(stderr, "Unexpected connect: %d\n", cqe->res);
			return T_EXIT_FAIL;
		}
		break;
	case 2:
		if (cqe->res) {
			fprintf(stderr, "Unexpected shutdown: %d\n", cqe->res);
			return T_EXIT_FAIL;
		}
		break;
	}
	io_uring_cqe_seen(ring, cqe);
	return T_EXIT_PASS;
}

static int test(struct io_uring *ring, struct sockaddr_in *saddr, int p_fd)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe1, *sqe2;
	socklen_t val_len;
	int ret, val;

	sqe1 = io_uring_get_sqe(ring);
	io_uring_prep_connect(sqe1, p_fd, (struct sockaddr *)saddr, sizeof(*saddr));
	sqe1->user_data = 1;

	ret = io_uring_submit(ring);
	assert(ret != -1);

	usleep(200000); // 200ms
	sqe2 = io_uring_get_sqe(ring);
	io_uring_prep_shutdown(sqe2, p_fd, SHUT_RDWR);
	sqe2->user_data = 2;

	ret = io_uring_submit(ring);
	assert(ret != -1);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait: %s\n", strerror(-ret));
		return T_EXIT_FAIL;
	}

	ret = check_cqe(ring, cqe);
	if (ret != T_EXIT_PASS)
		return ret;

	val = 0;
	val_len = sizeof(val);
	ret = getsockopt(p_fd, SOL_SOCKET, SO_ERROR, &val, &val_len);
	assert(ret != -1);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait: %s\n", strerror(-ret));
		return T_EXIT_FAIL;
	}

	return check_cqe(ring, cqe);
}

int main(int argc, char *argv[])
{
	struct sockaddr_in addr = { };
	struct io_uring ring;
	int val, p_fd, ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	p_fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);

	val = 1;
	ret = setsockopt(p_fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
	assert(ret != -1);

	// NB. these are to make things faster
	val = 2;
	ret = setsockopt(p_fd, IPPROTO_TCP, TCP_SYNCNT, &val, sizeof(val));
	assert(ret != -1);

	val = 500; // 500ms
	ret = setsockopt(p_fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &val, sizeof(val));
	assert(ret != -1);

	t_set_nonblock(p_fd);

	addr.sin_family = AF_INET;
	/* any unreachable address */
	addr.sin_addr.s_addr = inet_addr("172.31.5.5");
	addr.sin_port = htons(12345);

	ret = io_uring_queue_init(2, &ring, 0);
	assert(ret >= 0);

	ret = test(&ring, &addr, p_fd);
	io_uring_queue_exit(&ring);
	return ret;
}
