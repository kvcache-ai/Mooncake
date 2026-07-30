/* SPDX-License-Identifier: MIT */
/*
 * Check that kernels that support it will return IORING_CQE_F_SOCK_NONEMPTY
 * on accepts requests where more connections are pending.
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/un.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

static int no_more_accept;

#define MAX_ACCEPTS	8

struct data {
	pthread_t thread;
	pthread_barrier_t barrier;
	pthread_barrier_t conn_barrier;
	int connects;
};

static int start_accept_listen(int port_off, int extra_flags)
{
	struct sockaddr_in addr;
	int32_t val = 1;
	int fd, ret;

	fd = socket(AF_INET, SOCK_STREAM | extra_flags, IPPROTO_TCP);

	ret = setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &val, sizeof(val));
	assert(ret != -1);
	ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
	assert(ret != -1);

	addr.sin_family = AF_INET;
	addr.sin_port = htons(0x1235 + port_off);
	addr.sin_addr.s_addr = inet_addr("127.0.0.1");

	ret = bind(fd, (struct sockaddr *) &addr, sizeof(addr));
	assert(ret != -1);
	ret = listen(fd, 20000);
	assert(ret != -1);

	return fd;
}

static void *connect_fn(void *data)
{
	struct sockaddr_in addr = { };
	struct data *d = data;
	int i;

	pthread_barrier_wait(&d->barrier);

	addr.sin_family = AF_INET;
	addr.sin_port = htons(0x1235);
	addr.sin_addr.s_addr = inet_addr("127.0.0.1");

	for (i = 0; i < d->connects; i++) {
		int s;

		s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (s < 0) {
			perror("socket");
			break;
		}
		if (connect(s, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
			perror("connect");
			break;
		}
	}

	if (i)
		pthread_barrier_wait(&d->conn_barrier);

	return NULL;
}

static void setup_thread(struct data *d, int nconns)
{
	d->connects = nconns;
	pthread_barrier_init(&d->barrier, NULL, 2);
	pthread_barrier_init(&d->conn_barrier, NULL, 2);
	pthread_create(&d->thread, NULL, connect_fn, d);
}

static int test_maccept(struct data *d, int flags, int fixed)
{
	struct io_uring_params p = { };
	struct io_uring ring;
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int err = 0, fd, ret, i, *fds;

	p.flags = flags;
	ret = io_uring_queue_init_params(8, &ring, &p);
	if (ret == -EINVAL) {
		return T_EXIT_SKIP;
	} else if (ret < 0) {
		fprintf(stderr, "ring setup failure: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (!(p.features & IORING_FEAT_RECVSEND_BUNDLE)) {
		no_more_accept = 1;
		return 0;
	}

	setup_thread(d, MAX_ACCEPTS);

	fds = malloc(MAX_ACCEPTS * sizeof(int));
	memset(fds, -1, MAX_ACCEPTS * sizeof(int));

	if (fixed) {
		io_uring_register_ring_fd(&ring);

		ret = io_uring_register_files(&ring, fds, MAX_ACCEPTS);
		if (ret) {
			fprintf(stderr, "file reg %d\n", ret);
			return -1;
		}
	}

	fd = start_accept_listen(0, 0);

	pthread_barrier_wait(&d->barrier);

	if (d->connects > 1)
		pthread_barrier_wait(&d->conn_barrier);

	for (i = 0; i < d->connects; i++) {
		sqe = io_uring_get_sqe(&ring);
		if (fixed)
			io_uring_prep_accept_direct(sqe, fd, NULL, NULL, 0, i);
		else
			io_uring_prep_accept(sqe, fd, NULL, NULL, 0);

		ret = io_uring_submit_and_wait(&ring, 1);
		assert(ret != -1);

		ret = io_uring_wait_cqe(&ring, &cqe);
		assert(!ret);
		if (cqe->res < 0) {
			fprintf(stderr, "res=%d\n", cqe->res);
			break;
		}
		fds[i] = cqe->res;
		if (d->connects == 1) {
			if (cqe->flags & IORING_CQE_F_SOCK_NONEMPTY) {
				fprintf(stderr, "Non-empty sock on single?\n");
				err = 1;
				break;
			}
		} else {
			int last = i + 1 == d->connects;

			if (last && cqe->flags & IORING_CQE_F_SOCK_NONEMPTY) {
				fprintf(stderr, "Non-empty sock on last?\n");
				err = 1;
				break;
			} else if (!last && !(cqe->flags & IORING_CQE_F_SOCK_NONEMPTY)) {
				fprintf(stderr, "Empty on multi connect?\n");
				err = 1;
				break;
			}
		}
		io_uring_cqe_seen(&ring, cqe);
	}

	close(fd);
	if (!fixed) {
		for (i = 0; i < MAX_ACCEPTS; i++)
			if (fds[i] != -1)
				close(fds[i]);
	}
	free(fds);
	io_uring_queue_exit(&ring);
	return err;
}

static int test(int flags, int fixed)
{
	struct data d;
	void *tret;
	int ret;

	ret = test_maccept(&d, flags, fixed);
	if (ret) {
		fprintf(stderr, "test conns=1 failed\n");
		return ret;
	}
	if (no_more_accept)
		return T_EXIT_SKIP;

	pthread_join(d.thread, &tret);

	ret = test_maccept(&d, flags, fixed);
	if (ret) {
		fprintf(stderr, "test conns=MAX failed\n");
		return ret;
	}

	pthread_join(d.thread, &tret);
	return 0;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test(0, 0);
	if (no_more_accept)
		return T_EXIT_SKIP;
	if (ret) {
		fprintf(stderr, "test 0 0 failed\n");
		return ret;
	}

	ret = test(IORING_SETUP_SINGLE_ISSUER|IORING_SETUP_DEFER_TASKRUN, 0);
	if (ret) {
		fprintf(stderr, "test DEFER 0 failed\n");
		return ret;
	}

	ret = test(0, 1);
	if (ret) {
		fprintf(stderr, "test 0 1 failed\n");
		return ret;
	}

	ret = test(IORING_SETUP_SINGLE_ISSUER|IORING_SETUP_DEFER_TASKRUN, 1);
	if (ret) {
		fprintf(stderr, "test DEFER 1 failed\n");
		return ret;
	}

	return 0;
}
