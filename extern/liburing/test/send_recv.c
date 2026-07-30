/* SPDX-License-Identifier: MIT */
/*
 * Simple test case showing using send and recv through io_uring
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

#define MAX_MSG	4096

static uint64_t str[MAX_MSG / sizeof(uint64_t)];

#define PORT	10202
#define HOST	"127.0.0.1"

static int no_send_vec;

static int recv_prep(struct io_uring *ring, struct iovec *iov, int *sock,
		     int registerfiles, int async, int provide)
{
	struct sockaddr_in saddr;
	struct io_uring_sqe *sqe;
	int sockfd, ret, val, use_fd;

	memset(&saddr, 0, sizeof(saddr));
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = htonl(INADDR_ANY);
	saddr.sin_port = htons(PORT);

	sockfd = socket(AF_INET, SOCK_DGRAM, 0);
	if (sockfd < 0) {
		perror("socket");
		return 1;
	}

	val = 1;
	setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

	ret = bind(sockfd, (struct sockaddr *)&saddr, sizeof(saddr));
	if (ret < 0) {
		perror("bind");
		goto err;
	}

	if (registerfiles) {
		ret = io_uring_register_files(ring, &sockfd, 1);
		if (ret) {
			fprintf(stderr, "file reg failed\n");
			goto err;
		}
		use_fd = 0;
	} else {
		use_fd = sockfd;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_recv(sqe, use_fd, iov->iov_base, iov->iov_len, 0);
	if (registerfiles)
		sqe->flags |= IOSQE_FIXED_FILE;
	if (async)
		sqe->flags |= IOSQE_ASYNC;
	if (provide)
		sqe->flags |= IOSQE_BUFFER_SELECT;
	sqe->user_data = 2;

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "submit failed: %d\n", ret);
		goto err;
	}

	*sock = sockfd;
	return 0;
err:
	close(sockfd);
	return 1;
}

static int do_recv(struct io_uring *ring, struct iovec *iov, int enobufs)
{
	struct io_uring_cqe *cqe;
	uint64_t *ptr;
	int i, ret;

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stdout, "wait_cqe: %d\n", ret);
		return 1;
	}
	if (cqe->res == -EINVAL) {
		fprintf(stdout, "recv not supported, skipping\n");
		goto out;
	}
	if (cqe->res == -ENOBUFS && enobufs) {
		if (cqe->flags & IORING_CQE_F_SOCK_NONEMPTY) {
			fprintf(stdout, "NONEMPTY set on -ENOBUFS\n");
			goto err;
		}
		goto out;
	}
	if (cqe->res < 0) {
		fprintf(stderr, "failed cqe: %d\n", cqe->res);
		goto err;
	}

	if (cqe->res != MAX_MSG) {
		fprintf(stderr, "got wrong length: %d/%d\n", cqe->res, MAX_MSG);
		goto err;
	}

	ptr = iov->iov_base;
	for (i = 0; i < MAX_MSG / sizeof(uint64_t); i++) {
		if (ptr[i] == str[i])
			continue;
		fprintf(stderr, "data mismatch at %d: %llu\n", i, (unsigned long long) ptr[i]);
		goto err;
	}

out:
	io_uring_cqe_seen(ring, cqe);
	return 0;
err:
	io_uring_cqe_seen(ring, cqe);
	return 1;
}

struct recv_data {
	pthread_mutex_t mutex;
	int use_sqthread;
	int registerfiles;
	int async;
	int provide;
};

static void *recv_fn(void *data)
{
	struct recv_data *rd = data;
	char buf[MAX_MSG + 1];
	struct iovec iov = {
		.iov_base = buf,
		.iov_len = sizeof(buf) - 1,
	};
	struct io_uring_params p = { };
	struct io_uring ring;
	int ret, sock;

	if (rd->use_sqthread)
		p.flags = IORING_SETUP_SQPOLL;
	ret = t_create_ring_params(1, &ring, &p);
	if (ret == T_SETUP_SKIP) {
		pthread_mutex_unlock(&rd->mutex);
		ret = 0;
		goto err;
	} else if (ret < 0) {
		pthread_mutex_unlock(&rd->mutex);
		goto err;
	}

	if (rd->use_sqthread && !rd->registerfiles) {
		if (!(p.features & IORING_FEAT_SQPOLL_NONFIXED)) {
			fprintf(stdout, "Non-registered SQPOLL not available, skipping\n");
			pthread_mutex_unlock(&rd->mutex);
			goto err;
		}
	}

	ret = recv_prep(&ring, &iov, &sock, rd->registerfiles, rd->async,
				rd->provide);
	if (ret) {
		fprintf(stderr, "recv_prep failed: %d\n", ret);
		goto err;
	}
	pthread_mutex_unlock(&rd->mutex);
	ret = do_recv(&ring, &iov, rd->provide);

	close(sock);
	io_uring_queue_exit(&ring);
err:
	return (void *)(intptr_t)ret;
}

static int do_send(int async, int vec, int big_vec)
{
	struct sockaddr_in saddr;
	struct iovec vecs[32];
	struct io_uring ring;
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int sockfd, ret;

	ret = io_uring_queue_init(1, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue init failed: %d\n", ret);
		return 1;
	}

	memset(&saddr, 0, sizeof(saddr));
	saddr.sin_family = AF_INET;
	saddr.sin_port = htons(PORT);
	inet_pton(AF_INET, HOST, &saddr.sin_addr);

	sockfd = socket(AF_INET, SOCK_DGRAM, 0);
	if (sockfd < 0) {
		perror("socket");
		goto err2;
	}

	ret = connect(sockfd, (struct sockaddr *)&saddr, sizeof(saddr));
	if (ret < 0) {
		perror("connect");
		goto err;
	}

retry:
	sqe = io_uring_get_sqe(&ring);
	if (vec) {
		size_t total = MAX_MSG;
		uint64_t *ptr = str;
		int i, nvecs;

		if (!big_vec) {
			vecs[0].iov_base = str;
			vecs[0].iov_len = MAX_MSG / 2;
			vecs[1].iov_base = &str[256];
			vecs[1].iov_len = MAX_MSG / 2;
			nvecs = 2;
		} else {
			total /= 32;

			for (i = 0; i < 32; i++) {
				vecs[i].iov_base = ptr;
				vecs[i].iov_len = total;
				ptr += total / sizeof(uint64_t);
			}
			nvecs = 32;
		}

		io_uring_prep_send(sqe, sockfd, vecs, nvecs, 0);
		sqe->ioprio = (1U << 5);
	} else {
		io_uring_prep_send(sqe, sockfd, str, sizeof(str), 0);
	}
	if (async)
		sqe->flags |= IOSQE_ASYNC;
	sqe->user_data = 1;

	ret = io_uring_submit(&ring);
	if (ret <= 0) {
		fprintf(stderr, "submit failed: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (cqe->res == -EINVAL) {
		if (vec) {
			vec = 0;
			no_send_vec = 1;
			io_uring_cqe_seen(&ring, cqe);
			goto retry;
		}
		fprintf(stdout, "send not supported, skipping\n");
		goto err;
	}
	if (cqe->res != sizeof(str)) {
		fprintf(stderr, "failed cqe: %d\n", cqe->res);
		goto err;
	}

	close(sockfd);
	io_uring_queue_exit(&ring);
	return 0;

err:
	close(sockfd);
err2:
	io_uring_queue_exit(&ring);
	return 1;
}

static int test(int use_sqthread, int regfiles, int async, int provide, int vec,
		int big_vec)
{
	pthread_mutexattr_t attr;
	pthread_t recv_thread;
	struct recv_data rd;
	int ret;
	void *retval;

	if (vec && no_send_vec)
		return T_EXIT_SKIP;

	pthread_mutexattr_init(&attr);
	pthread_mutexattr_setpshared(&attr, 1);
	pthread_mutex_init(&rd.mutex, &attr);
	pthread_mutex_lock(&rd.mutex);
	rd.use_sqthread = use_sqthread;
	rd.registerfiles = regfiles;
	rd.async = async;
	rd.provide = provide;

	ret = pthread_create(&recv_thread, NULL, recv_fn, &rd);
	if (ret) {
		fprintf(stderr, "Thread create failed: %d\n", ret);
		pthread_mutex_unlock(&rd.mutex);
		return 1;
	}

	pthread_mutex_lock(&rd.mutex);
	do_send(async, vec, big_vec);
	pthread_join(recv_thread, &retval);
	return (intptr_t)retval;
}

static int test_invalid(void)
{
	struct io_uring ring;
	int ret, i;
	int fds[2];
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;

	ret = t_create_ring(8, &ring, IORING_SETUP_SUBMIT_ALL);
	if (ret) {
		if (ret == -EINVAL)
			return 0;
		return ret;
	}

	ret = t_create_socket_pair(fds, true);
	if (ret)
		return ret;

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_sendmsg(sqe, fds[0], NULL, MSG_WAITALL);
	sqe->flags |= IOSQE_ASYNC;

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_recvmsg(sqe, fds[1], NULL, 0);
	sqe->flags |= IOSQE_ASYNC;

	ret = io_uring_submit_and_wait(&ring, 2);
	if (ret != 2)
		return ret;

	for (i = 0; i < 2; i++) {
		ret = io_uring_peek_cqe(&ring, &cqe);
		if (ret || cqe->res != -EFAULT)
			return -1;
		io_uring_cqe_seen(&ring, cqe);
	}

	io_uring_queue_exit(&ring);
	close(fds[0]);
	close(fds[1]);
	return 0;
}

int main(int argc, char *argv[])
{
	int i, ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	for (i = 0; i < MAX_MSG / sizeof(uint64_t); i++)
		str[i] = i + 1;

	ret = test_invalid();
	if (ret) {
		fprintf(stderr, "test_invalid failed\n");
		return ret;
	}

	ret = test(0, 0, 1, 1, 0, 0);
	if (ret) {
		fprintf(stderr, "test sqthread=0 1 1 failed\n");
		return ret;
	}

	ret = test(1, 1, 1, 1, 0, 0);
	if (ret) {
		fprintf(stderr, "test sqthread=1 reg=1 1 1 failed\n");
		return ret;
	}

	ret = test(1, 0, 1, 1, 0, 0);
	if (ret) {
		fprintf(stderr, "test sqthread=1 reg=0 1 1 failed\n");
		return ret;
	}

	ret = test(0, 0, 0, 1, 0, 0);
	if (ret) {
		fprintf(stderr, "test sqthread=0 0 1 failed\n");
		return ret;
	}

	ret = test(1, 1, 0, 1, 0, 0);
	if (ret) {
		fprintf(stderr, "test sqthread=1 reg=1 0 1 failed\n");
		return ret;
	}

	ret = test(1, 0, 0, 1, 0, 0);
	if (ret) {
		fprintf(stderr, "test sqthread=1 reg=0 0 1 failed\n");
		return ret;
	}

	ret = test(0, 0, 1, 0, 0, 0);
	if (ret) {
		fprintf(stderr, "test sqthread=0 0 1 failed\n");
		return ret;
	}

	ret = test(1, 1, 1, 0, 0, 0);
	if (ret) {
		fprintf(stderr, "test sqthread=1 reg=1 1 0 failed\n");
		return ret;
	}

	ret = test(1, 0, 1, 0, 0, 0);
	if (ret) {
		fprintf(stderr, "test sqthread=1 reg=0 1 0 failed\n");
		return ret;
	}

	ret = test(0, 0, 0, 0, 0, 0);
	if (ret) {
		fprintf(stderr, "test sqthread=0 0 0 failed\n");
		return ret;
	}

	ret = test(1, 1, 0, 0, 0, 0);
	if (ret) {
		fprintf(stderr, "test sqthread=1 reg=1 0 0 failed\n");
		return ret;
	}

	ret = test(1, 0, 0, 0, 0, 0);
	if (ret) {
		fprintf(stderr, "test sqthread=1 reg=0 0 0 failed\n");
		return ret;
	}

	ret = test(0, 0, 0, 0, 1, 0);
	if (ret) {
		fprintf(stderr, "test small vec sync failed\n");
		return ret;
	}
	if (no_send_vec)
		return T_EXIT_PASS;

	ret = test(0, 0, 1, 0, 1, 0);
	if (ret) {
		fprintf(stderr, "test small vec async failed\n");
		return ret;
	}

	ret = test(0, 0, 0, 0, 1, 1);
	if (ret) {
		fprintf(stderr, "test big vec sync failed\n");
		return ret;
	}

	ret = test(0, 0, 1, 0, 1, 1);
	if (ret) {
		fprintf(stderr, "test big vec async failed\n");
		return ret;
	}

	return T_EXIT_PASS;
}
