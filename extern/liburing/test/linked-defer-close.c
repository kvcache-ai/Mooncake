/* SPDX-License-Identifier: MIT */
/*
 * Test that the final close of a file does indeed get it closed, if the
 * ring is setup with DEFER_TASKRUN and the task is waiting in cqring_wait()
 * during. Also see:
 *
 * https://github.com/axboe/liburing/issues/1235
 *
 * for a bug report, and the zig code on which this test program is based.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <signal.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

enum {
	IS_ACCEPT = 0,
	IS_SEND = 0x100,
	IS_SEND2 = 0x101,
	IS_SEND3 = 0x102,
	IS_CLOSE = 0x200,
};

struct thread_data {
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

	do {
		memset(msg, 0, sizeof(msg));
		ret = recv(sockfd, msg, sizeof(msg), 0);
	} while (ret > 0);

	close(sockfd);
done:
	kill(data->parent_pid, SIGUSR1);
	return NULL;
}

/* we got SIGUSR1, exit normally */
static void sig_usr1(int sig)
{
	exit(T_EXIT_PASS);
}

/* timed out, failure */
static void sig_timeout(int sig)
{
	exit(T_EXIT_FAIL);
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct sockaddr_in saddr;
	char *msg1 = "message number 1\n";
	char *msg2 = "message number 2\n";
	char *msg3 = "message number 3\n";
	int val, send_fd, ret, sockfd;
	struct sigaction act[2] = { };
	struct thread_data td;
	pthread_t thread;

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

	ret = io_uring_queue_init(8, &ring, IORING_SETUP_SINGLE_ISSUER |
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

	/* expected exit */
	act[0].sa_handler = sig_usr1;
	sigaction(SIGUSR1, &act[0], NULL);

	/* if this hits, we have failed */
	act[1].sa_handler = sig_timeout;
	sigaction(SIGALRM, &act[1], NULL);
	alarm(5);
	
	/* start receiver */
	td.parent_pid = getpid();
	pthread_create(&thread, NULL, thread_fn, &td);

	do {
		ret = io_uring_submit_and_wait(&ring, 1);
		if (ret < 0) {
			fprintf(stderr, "submit: %d\n", ret);
			return T_EXIT_FAIL;
		}
		ret = io_uring_peek_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "peek: %d\n", ret);
			return T_EXIT_FAIL;
		}

		switch (cqe->user_data) {
		case IS_ACCEPT:
			send_fd = cqe->res;
			io_uring_cqe_seen(&ring, cqe);

			/*
			 * prep two sends, with the 2nd linked to a close
			 * operation. Once the close has been completed, that
			 * will terminate the receiving thread and that will
			 * in turn send this task a SIGUSR1 signal. If the
			 * kernel is buggy, then we never get SIGUSR1 and we
			 * will sit forever waiting and be timed out.
			 */
			sqe = io_uring_get_sqe(&ring);
			io_uring_prep_send(sqe, send_fd, msg1, strlen(msg1), 0);
			sqe->user_data = IS_SEND;
			sqe->flags = IOSQE_CQE_SKIP_SUCCESS | IOSQE_IO_LINK;

			sqe = io_uring_get_sqe(&ring);
			io_uring_prep_send(sqe, send_fd, msg2, strlen(msg2), 0);
			sqe->user_data = IS_SEND2;
			sqe->flags = IOSQE_CQE_SKIP_SUCCESS | IOSQE_IO_LINK;

			sqe = io_uring_get_sqe(&ring);
			io_uring_prep_send(sqe, send_fd, msg3, strlen(msg3), 0);
			sqe->user_data = IS_SEND3;
			sqe->flags = IOSQE_CQE_SKIP_SUCCESS | IOSQE_IO_LINK;

			sqe = io_uring_get_sqe(&ring);
			io_uring_prep_close(sqe, send_fd);
			sqe->user_data = IS_CLOSE;
			sqe->flags = IOSQE_CQE_SKIP_SUCCESS;
			break;
		case IS_SEND:
		case IS_SEND2:
			fprintf(stderr, "Should not see send response\n");
			io_uring_cqe_seen(&ring, cqe);
			return T_EXIT_FAIL;
		case IS_CLOSE:
			fprintf(stderr, "Should not see close response\n");
			io_uring_cqe_seen(&ring, cqe);
			return T_EXIT_FAIL;
		default:
			fprintf(stderr, "got unknown cqe\n");
			return T_EXIT_FAIL;
		}
	} while (1);

	/* will never get here */
	return T_EXIT_FAIL;
}
