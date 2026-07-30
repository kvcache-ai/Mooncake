/* SPDX-License-Identifier: MIT */
/*
 * Description: Test non-immediate sendmsg completion with non-static iovec
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include "liburing.h"
#include "helpers.h"

/* anything > 1 should be fine, do bigger than FAST_IOV to be sure */
#define IOVS		16
#define	INFLIGHT	256

enum {
	IS_ACCEPT = 0x89,
	IS_SENDMSG = 0x91,
};

struct thread_data {
	pthread_t thread;
	pthread_barrier_t barrier;
	int parent_pid;
};

static void *thread_fn(void *__data)
{
	struct thread_data *data = __data;
	struct sockaddr_in saddr;
	int sockfd, ret;
	char msg[64];

	memset(&saddr, 0, sizeof(saddr));
	saddr.sin_family = AF_INET;
	saddr.sin_port = htons(9999);
	inet_pton(AF_INET, "127.0.0.1", &saddr.sin_addr);

	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		perror("socket");
		goto done;
	}

	ret = connect(sockfd, (struct sockaddr *) &saddr, sizeof(saddr));
	if (ret < 0) {
		perror("connect");
		close(sockfd);
		goto done;
	}

	pthread_barrier_wait(&data->barrier);
	do {
		usleep(100);
		memset(msg, 0, sizeof(msg));
		ret = recv(sockfd, msg, sizeof(msg), 0);
	} while (ret > 0);

	close(sockfd);
done:
	kill(data->parent_pid, SIGUSR1);
	return NULL;
}

static int queue_sends(struct io_uring *ring, int send_fd, struct thread_data *td, struct msghdr *msghdr)
{
	struct io_uring_sqe *sqe;
	char buf[64];
	int i, ret, sbuf;
	struct iovec *iovs = msghdr->msg_iov;

	sbuf = 8 * 1024;
	ret = setsockopt(send_fd, SOL_SOCKET, SO_SNDBUF, &sbuf, sizeof(sbuf));
	if (ret < 0) {
		perror("setsockopt");
		return 1;
	}

	memset(buf, 0xaa, sizeof(buf));
	for (i = 0; i < IOVS; i++) {
		iovs[i].iov_base = buf;
		iovs[i].iov_len = sizeof(buf);
	}

	/* fill send buffer */
	for (i = 0;; i++) {
		ret = sendmsg(send_fd, msghdr, MSG_DONTWAIT);
		if (ret == -1) {
			if (errno == EAGAIN)
				break;
			perror("sendmsg");
			return 1;
		}
	}

	/* kick receiver, start sendmsg */
	msghdr->msg_iovlen = IOVS;
	pthread_barrier_wait(&td->barrier);
	for (i = 0; i < INFLIGHT; i++) {
		sqe = io_uring_get_sqe(ring);
		io_uring_prep_sendmsg(sqe, send_fd, msghdr, 0);
		sqe->user_data = IS_SENDMSG;
	}

	return 0;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct sockaddr_in saddr;
	int val, send_fd, ret, sockfd, seen_sends = 0;
	struct thread_data td;
	struct iovec iovs[IOVS];
	struct msghdr msghdr = {
		.msg_iov = iovs,
		.msg_iovlen = 1,
	};

	if (argc > 1)
		return T_EXIT_SKIP;

	memset(&saddr, 0, sizeof(saddr));
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = htonl(INADDR_ANY);
	saddr.sin_port = htons(9999);

	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		perror("socket");
		return T_EXIT_FAIL;
	}

	val = 1;
	setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &val, sizeof(val));
	setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

	ret = bind(sockfd, (struct sockaddr *)&saddr, sizeof(saddr));
	if (ret < 0) {
		perror("bind");
		close(sockfd);
		return T_EXIT_FAIL;
	}

	ret = listen(sockfd, 1);
	if (ret < 0) {
		perror("listen");
		close(sockfd);
		return T_EXIT_FAIL;
	}

	ret = io_uring_queue_init(INFLIGHT, &ring, IORING_SETUP_SINGLE_ISSUER |
					    IORING_SETUP_DEFER_TASKRUN);
	if (ret == -EINVAL) {
		close(sockfd);
		return T_EXIT_SKIP;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_multishot_accept(sqe, sockfd, NULL, NULL, 0);
	sqe->user_data = IS_ACCEPT;
	io_uring_submit(&ring);

	/* check for no multishot accept */
	ret = io_uring_peek_cqe(&ring, &cqe);
	if (!ret && cqe->res == -EINVAL) {
		close(sockfd);
		return T_EXIT_SKIP;
	}

	/* start receiver */
	td.parent_pid = getpid();
	pthread_barrier_init(&td.barrier, NULL, 2);
	pthread_create(&td.thread, NULL, thread_fn, &td);

	do {
		ret = io_uring_submit_and_wait(&ring, 1);
		if (ret < 0) {
			fprintf(stderr, "submit: %d\n", ret);
			break;
		}
		ret = io_uring_peek_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "peek: %d\n", ret);
			break;
		}

		switch (cqe->user_data) {
		case IS_ACCEPT:
			send_fd = cqe->res;
			io_uring_cqe_seen(&ring, cqe);

			ret = queue_sends(&ring, send_fd, &td, &msghdr);
			if (ret)
				exit(T_EXIT_FAIL);
			break;
		case IS_SENDMSG:
			io_uring_cqe_seen(&ring, cqe);
			seen_sends++;
			if (seen_sends == INFLIGHT)
				exit(0);
			break;
		default:
			fprintf(stderr, "got unknown cqe\n");
			return T_EXIT_FAIL;
		}
	} while (1);

	return T_EXIT_FAIL;
}
