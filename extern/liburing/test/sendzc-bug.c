/* SPDX-License-Identifier: MIT */
/*
 * Description: test that importing a buffer node ties the lifetime to the
 *		notification io_kiocb, not the normal request.
 *
 * Based on a test case from Google Big Sleep.
 *
 */
#include <arpa/inet.h>
#include <err.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "liburing.h"
#include "helpers.h"

#define BUF_SIZE (1024 * 1024)
static uint8_t read_buffer[BUF_SIZE] = { 0 };

/* Creates a pair of connected TCP sockets. */
static void tcp_socketpair(int sv[2])
{
	struct sockaddr_in addr_in = {
		.sin_family = AF_INET,
		.sin_addr.s_addr = htonl(INADDR_LOOPBACK),
		.sin_port = 0,
	};
	struct sockaddr *addr = (struct sockaddr *) &addr_in;
	socklen_t addr_len = sizeof(addr_in);
	int listener_fd;

	listener_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (listener_fd < 0)
		err(EXIT_FAILURE, "listener socket");

	if (bind(listener_fd, addr, addr_len) < 0)
		err(EXIT_FAILURE, "bind");

	if (listen(listener_fd, 1) < 0)
		err(EXIT_FAILURE, "listen");

	if (getsockname(listener_fd, addr, &addr_len) < 0)
		err(EXIT_FAILURE, "getsockname");

	sv[0] = socket(AF_INET, SOCK_STREAM, 0);
	if (sv[0] < 0)
		err(EXIT_FAILURE, "socket(AF_INET, SOCK_STREAM, 0)");

	if (connect(sv[0], addr, addr_len) < 0)
		err(EXIT_FAILURE, "connect");

	sv[1] = accept(listener_fd, NULL, NULL);
	if (sv[1] == -1)
		err(EXIT_FAILURE, "accept");

	close(listener_fd);
}

int main(int argc, char *argv[])
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe* cqe;
	struct iovec send_iov;
	struct io_uring ring;
	struct iovec iov_reg;
	struct msghdr msg = {
		.msg_iov = &send_iov,
		.msg_iovlen = 1,
	};
	void *send_buffer;
	int sockets[2];
	int val, ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	tcp_socketpair(sockets);

	val = 1;
	if (setsockopt(sockets[0], SOL_SOCKET, SO_ZEROCOPY, &val, sizeof(val)))
		err(EXIT_FAILURE, "setsockopt(SO_ZEROCOPY)");

	ret = io_uring_queue_init(1, &ring, 0);
	if (ret < 0)
		errx(EXIT_FAILURE, "io_uring_queue_init: %s", strerror(-ret));

	/* mmap send_buffer so we can unmap it later. */
	send_buffer = mmap(NULL, BUF_SIZE, PROT_READ | PROT_WRITE,
			  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (send_buffer == MAP_FAILED)
		err(EXIT_FAILURE, "mapping iouring buffer");

	iov_reg.iov_base = send_buffer;
	iov_reg.iov_len = BUF_SIZE;
	ret = io_uring_register_buffers(&ring, &iov_reg, 1);
	if (ret < 0)
		errx(EXIT_FAILURE, "io_uring_register_buffers: %s", strerror(-ret));

	sqe = io_uring_get_sqe(&ring);

	send_iov.iov_base = send_buffer;
	send_iov.iov_len = BUF_SIZE;

	io_uring_prep_sendmsg_zc(sqe, sockets[0], &msg, 0);
	sqe->ioprio = IORING_RECVSEND_FIXED_BUF;
	sqe->buf_index = 0;

	memset(send_buffer, 'B', BUF_SIZE);

	io_uring_submit(&ring);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret < 0)
		errx(EXIT_FAILURE, "io_uring_wait_cqe: %s", strerror(-ret));

	if (cqe->res < 0) {
		if (cqe->res == -EINVAL)
			return T_EXIT_SKIP;
		errx(EXIT_FAILURE, "readv failed: %s", strerror(-cqe->res));
	}

	io_uring_cqe_seen(&ring, cqe);

	io_uring_unregister_buffers(&ring);
	munmap(send_buffer, BUF_SIZE);

	ret = read(sockets[1], read_buffer, BUF_SIZE);
	if (ret < 0)
		err(EXIT_FAILURE, "read");

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}
