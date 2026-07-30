/* SPDX-License-Identifier: MIT */
/*
 * Check that recv on an empty socket will bubble back -EAGAIN if
 * MSG_DONTWAIT is set, regardless of whether or not O_NONBLOCK is set
 * on the socket itself.
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
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

static int test(int o_nonblock, int msg_dontwait)
{
	int p_fd[2], ret, flags, recv_s0, val;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct sockaddr_in addr;
	struct io_uring ring;
	char recv_buff[128];

	recv_s0 = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);

	val = 1;
	ret = setsockopt(recv_s0, SOL_SOCKET, SO_REUSEPORT, &val, sizeof(val));
	assert(ret != -1);
	ret = setsockopt(recv_s0, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
	assert(ret != -1);

	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = inet_addr("127.0.0.1");
	ret = t_bind_ephemeral_port(recv_s0, &addr);
	assert(!ret);
	ret = listen(recv_s0, 128);
	assert(ret != -1);

	p_fd[1] = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);

	val = 1;
	ret = setsockopt(p_fd[1], IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
	assert(ret != -1);

	t_set_nonblock(p_fd[1]);

	ret = connect(p_fd[1], (struct sockaddr *) &addr, sizeof(addr));
	assert(ret == -1);

	p_fd[0] = accept(recv_s0, NULL, NULL);
	assert(p_fd[0] != -1);

	if (o_nonblock)
		t_set_nonblock(p_fd[0]);

	while (1) {
		int32_t code;
		socklen_t code_len = sizeof(code);

		ret = getsockopt(p_fd[1], SOL_SOCKET, SO_ERROR, &code, &code_len);
		assert(ret != -1);

		if (!code)
			break;
	}

	ret = io_uring_queue_init(32, &ring, 0);
	assert(ret >= 0);

	flags = msg_dontwait ? MSG_DONTWAIT : 0;
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_recv(sqe, p_fd[0], recv_buff, sizeof(recv_buff), flags);
	sqe->user_data = 1;
	io_uring_submit(&ring);

	ret = io_uring_peek_cqe(&ring, &cqe);
	if (ret) {
		if (ret != -EAGAIN) {
			fprintf(stderr, "bad peek: %d\n", ret);
			goto err;
		}
		if (msg_dontwait) {
			fprintf(stderr, "Got -EAGAIN without MSG_DONTWAIT\n");
			goto err;
		}
	} else {
		if (!msg_dontwait) {
			fprintf(stderr, "Unexpected completion\n");
			goto err;
		}
	}

	io_uring_queue_exit(&ring);
	close(p_fd[0]);
	close(p_fd[1]);
	close(recv_s0);
	return T_EXIT_PASS;
err:
	io_uring_queue_exit(&ring);
	close(p_fd[0]);
	close(p_fd[1]);
	close(recv_s0);
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test(0, 0);
	if (ret) {
		fprintf(stderr, "test 0 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(0, 1);
	if (ret) {
		fprintf(stderr, "test 0 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(1, 0);
	if (ret) {
		fprintf(stderr, "test 1 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(1, 1);
	if (ret) {
		fprintf(stderr, "test 1 1 failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}
