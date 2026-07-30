/* SPDX-License-Identifier: MIT */
/*
 * Description: run NAPI receive test. Meant to be run from the associated
 *		script, napi-test.sh. That will invoke this test program
 *		as either a sender or receiver, with the queue flags passed
 *		in for testing.
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>
#include <linux/if_packet.h>
#include <linux/socket.h>
#include <sys/socket.h>
#include <sys/stat.h>

#include "liburing.h"
#include "helpers.h"

static const char receiver_address[] = "10.10.10.20";
static const int port = 9999;
#define BUF_SIZE 4096

static char buffer[BUF_SIZE];
static unsigned current_byte = 0;

static void do_setsockopt(int fd, int level, int optname, int val)
{
	int ret = setsockopt(fd, level, optname, &val, sizeof(val));

	assert(ret == 0);
}

static int sender(int queue_flags)
{
	unsigned long long written = 0;
	struct sockaddr_in addr;
	struct io_uring ring;
	int i, ret, fd;

	/*
	 * Sender doesn't use the ring, but try and set one up with the same
	 * flags that the receiver will use. If that fails, we know the
	 * receiver will have failed too - just skip the test in that case.
	 */
	ret = io_uring_queue_init(1, &ring, queue_flags);
	if (ret)
		return T_EXIT_SKIP;
	io_uring_queue_exit(&ring);

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	ret = inet_pton(AF_INET, receiver_address, &addr.sin_addr);
	assert(ret == 1);

	fd = socket(PF_INET, SOCK_STREAM, 0);
	assert(fd >= 0);

	/* don't race with receiver, give it 1 second to connect */
	i = 0;
	do {
		ret = connect(fd, (void *)&addr, sizeof(addr));
		if (!ret)
			break;
		if (ret == -1 && errno == ECONNREFUSED) {
			if (i >= 10000) {
				fprintf(stderr, "Gave up trying to connect\n");
				return 1;
			}
			usleep(100);
			continue;
		}
		i++;
	} while (1);

	while (written < 8 * 1024 * 1024) {
		for (i = 0; i < BUF_SIZE; i++)
			buffer[i] = current_byte + i;

		ret = write(fd, buffer, BUF_SIZE);
		if (ret <= 0) {
			if (!ret || errno == ECONNRESET)
				break;
			fprintf(stderr, "write failed %i %i\n", ret, errno);
			return 1;
		}
		written += ret;
		current_byte += ret;
	}

	close(fd);
	return 0;
}

static int receiver(int queue_flags)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	struct io_uring_napi napi = { };
	struct sockaddr_in addr;
	int fd, listen_fd;
	int i, ret;

	ret = io_uring_queue_init(8, &ring, queue_flags);
	if (ret < 0) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "queue_init: %s\n", strerror(-ret));
		return 1;
	}

	napi.prefer_busy_poll = 1;
	napi.busy_poll_to = 50;
	io_uring_register_napi(&ring, &napi);

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr.s_addr = INADDR_ANY;

	listen_fd = socket(AF_INET, SOCK_STREAM, 0);
	assert(listen_fd >= 0);

	do_setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, 1);
	ret = bind(listen_fd, (void *)&addr, sizeof(addr));
	if (ret) {
		fprintf(stderr, "bind failed %i %i\n", ret, errno);
		return 1;
	}

	ret = listen(listen_fd, 8);
	assert(ret == 0);

	fd = accept(listen_fd, NULL, NULL);
	assert(fd >= 0);

	while (1) {
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_recv(sqe, fd, buffer, BUF_SIZE, 0);

		ret = io_uring_submit(&ring);
		if (ret < 0) {
			fprintf(stderr, "io_uring_submit: %i\n", ret);
			return 1;
		}

		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret < 0) {
			fprintf(stderr, "io_uring_wait_cqe: %i\n", ret);
			return 1;
		}

		ret = cqe->res;
		if (ret <= 0) {
			if (!ret)
				break;
			fprintf(stderr, "recv failed %i %i\n", ret, errno);
			return 1;
		}

		for (i = 0; i < ret; i++) {
			char expected = current_byte + i;

			if (buffer[i] != expected) {
				fprintf(stderr, "data mismatch: idx %i, %c vs %c\n",
					i, buffer[i], expected);
				return 1;
			}
		}

		current_byte += ret;
		io_uring_cqe_seen(&ring, cqe);
	}

	close(fd);
	io_uring_queue_exit(&ring);
	return 0;
}

int main(int argc, char **argv)
{
	int queue_flags;
	int is_rx;

	if (geteuid()) {
		fprintf(stdout, "NAPI test requires root\n");
		return T_EXIT_SKIP;
	}

	if (argc == 1) {
		struct stat sb;

		if (!stat("napi-test.sh", &sb)) {
			return system("bash napi-test.sh");
		} else if (!stat("test/napi-test.sh", &sb)) {
			return system("bash test/napi-test.sh");
		} else {
			fprintf(stderr, "Can't find napi-test.sh\n");
			return T_EXIT_SKIP;
		}
	} else if (argc == 2) {
		return T_EXIT_SKIP;
	} else if (argc != 3) {
		return T_EXIT_SKIP;
	}

	if (!strcmp(argv[1], "receive"))
		is_rx = 1;
	else if (!strcmp(argv[1], "send"))
		is_rx = 0;
	else
		return T_EXIT_FAIL;

	queue_flags = strtoul(argv[2], NULL, 16);

	if (is_rx)
		return receiver(queue_flags);

	return sender(queue_flags);
}
