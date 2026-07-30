/* SPDX-License-Identifier: MIT */
/*
 * Description: verify that timestamp retrieval that needs retrying with
 *		a current list of SKBs works.
 *
 * Based on a test case from Google Big Sleep.
 *
 */
#include <arpa/inet.h>
#include <err.h>
#include <fcntl.h>
#include <sys/time.h>
#include <linux/errqueue.h>
#include <linux/net_tstamp.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include "liburing.h"
#include "helpers.h"

/* Compatibility with slightly older kernel headers */
#ifndef SOCKET_URING_OP_TX_TIMESTAMP
#define SOCKET_URING_OP_TX_TIMESTAMP 4
#endif

/*
 * Create a socket whose error queue contains both timestamp information and
 * actual errors.
 */
static int create_sock_with_timestamps_and_errors(void)
{
	struct sockaddr_in recv_addr = { };
	struct sockaddr_in closed = { };
	int recv_sock, sock, val;
	char data = 'A';

	sock = socket(AF_INET, SOCK_DGRAM, 0);
	if (sock < 0)
		err(1, "socket");

	/* Enable TX timestamps */
	val = SOF_TIMESTAMPING_SOFTWARE | SOF_TIMESTAMPING_TX_SOFTWARE |
		SOF_TIMESTAMPING_OPT_ID | SOF_TIMESTAMPING_OPT_TSONLY;
	if (setsockopt(sock, SOL_SOCKET, SO_TIMESTAMPING, &val, sizeof(val)) < 0)
		err(1, "setsockopt(SO_TIMESTAMPING)");

	/* Enable IP_RECVERR */
	val = 1;
	if (setsockopt(sock, IPPROTO_IP, IP_RECVERR, &val, sizeof(val)) < 0)
		err(1, "setsockopt(IP_RECVERR)");

	recv_sock = socket(AF_INET, SOCK_DGRAM, 0);
	if (recv_sock < 0)
		err(1, "socket");

	recv_addr.sin_family = AF_INET;
	recv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
	if (bind(recv_sock, (struct sockaddr*)&recv_addr, sizeof(recv_addr)) < 0)
		err(1, "bind receiver");

	socklen_t recv_len = sizeof(recv_addr);
	if (getsockname(recv_sock, (struct sockaddr*)&recv_addr, &recv_len) < 0)
		err(1, "getsockname");

	/* Send packets to generate TX timestamps. */
	for (int i = 0; i < 2; i++) {
		if (sendto(sock, &data, sizeof(data), 0, (struct sockaddr*)&recv_addr, sizeof(recv_addr)) < 0)
			err(1, "sendto (%d)", i);
	}

	closed.sin_family = AF_INET;
	closed.sin_addr.s_addr = inet_addr("127.0.0.1");
	closed.sin_port = htons(9); /* assuming no one is listening on port 9 */
	if (sendto(sock, &data, sizeof(data), 0, (struct sockaddr*) &closed, sizeof(closed)) < 0)
		warn("sendto");

	return sock;
}

int main(int argc, char *argv[])
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	struct io_uring_params params = {
		.flags = IORING_SETUP_CQE32, /* required for timestamps */
	};
	char buf[1024], ctrl_recv[1024];
	struct iovec iov_recv = { .iov_base = buf, .iov_len = sizeof(buf) };
	struct msghdr msg_recv = {
		.msg_iov = &iov_recv,
		.msg_iovlen = 1,
		.msg_control = ctrl_recv,
		.msg_controllen = sizeof(ctrl_recv),
	};
	int sock, ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	sock = create_sock_with_timestamps_and_errors();

	ret = io_uring_queue_init_params(1, &ring, &params);
	if (ret < 0) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		err(1, "io_uring_queue_init_params: %d", ret);
	}

	sqe = io_uring_get_sqe(&ring);
	*sqe = (struct io_uring_sqe) {
		.opcode = IORING_OP_URING_CMD,
		.fd = sock,
		.cmd_op = SOCKET_URING_OP_TX_TIMESTAMP,
	};

	io_uring_submit(&ring);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret < 0)
		errx(1, "io_uring_wait_cqe: %d", ret);
	ret = cqe->res;
	io_uring_cqe_seen(&ring, cqe);
	/* kernel doesn't support retrieving timestamps */
	if (ret == -EOPNOTSUPP)
		return T_EXIT_SKIP;

	recvmsg(sock, &msg_recv, MSG_ERRQUEUE | MSG_DONTWAIT);
	return T_EXIT_PASS;
}
