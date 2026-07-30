/* SPDX-License-Identifier: MIT */
/*
 * Description: test fd passing with MSG_RING
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

static int no_msg;
static int no_sparse;
static int no_fd_pass;

struct data {
	pthread_t thread;
	pthread_barrier_t barrier;
	int ring_flags;
	int ring_fd;
	char buf[32];
};

static void *thread_fn(void *__data)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct data *d = __data;
	struct io_uring ring;
	int ret, fd = -1;

	io_uring_queue_init(8, &ring, d->ring_flags);
	ret = io_uring_register_files(&ring, &fd, 1);
	if (ret) {
		if (ret != -EINVAL && ret != -EBADF)
			fprintf(stderr, "thread file register: %d\n", ret);
		no_sparse = 1;
		pthread_barrier_wait(&d->barrier);
		return NULL;
	}

	d->ring_fd = ring.ring_fd;
	pthread_barrier_wait(&d->barrier);

	/* wait for MSG */
	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe dst: %d\n", ret);
		return NULL;
	}
	if (cqe->res < 0) {
		fprintf(stderr, "cqe error dst: %d\n", cqe->res);
		return NULL;
	}

	fd = cqe->res;
	io_uring_cqe_seen(&ring, cqe);
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_read(sqe, fd, d->buf, sizeof(d->buf), 0);
	sqe->flags |= IOSQE_FIXED_FILE;
	io_uring_submit(&ring);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe dst: %d\n", ret);
		return NULL;
	}
	if (cqe->res < 0) {
		fprintf(stderr, "cqe error dst: %d\n", cqe->res);
		return NULL;
	}

	io_uring_queue_exit(&ring);
	return NULL;
}

static int test_remote(struct io_uring *src, int ring_flags)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int fds[2], fd, ret;
	struct data d;
	char buf[32];
	void *tret;
	int i;

	if (no_fd_pass)
		return 0;

	pthread_barrier_init(&d.barrier, NULL, 2);
	d.ring_flags = ring_flags;
	pthread_create(&d.thread, NULL, thread_fn, &d);
	pthread_barrier_wait(&d.barrier);
	memset(d.buf, 0, sizeof(d.buf));

	if (no_sparse)
		return 0;

	if (pipe(fds) < 0) {
		perror("pipe");
		return 1;
	}

	fd = fds[0];
	ret = io_uring_register_files(src, &fd, 1);
	if (ret) {
		fprintf(stderr, "register files failed: %d\n", ret);
		return 1;
	}

	for (i = 0; i < ARRAY_SIZE(buf); i++)
		buf[i] = rand();

	sqe = io_uring_get_sqe(src);
	io_uring_prep_write(sqe, fds[1], buf, sizeof(buf), 0);
	sqe->user_data = 1;

	sqe = io_uring_get_sqe(src);
	io_uring_prep_msg_ring_fd(sqe, d.ring_fd, 0, 0, 0, 0);
	sqe->user_data = 2;
	
	io_uring_submit(src);

	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(src, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe: %d\n", ret);
			return 1;
		}
		if (cqe->res < 0) {
			fprintf(stderr, "cqe res %d\n", cqe->res);
			return 1;
		}
		if (cqe->user_data == 1 && cqe->res != sizeof(buf)) {
			fprintf(stderr, "short write %d\n", cqe->res);
			return 1;
		}
		io_uring_cqe_seen(src, cqe);
	}

	pthread_join(d.thread, &tret);

	if (memcmp(buf, d.buf, sizeof(buf))) {
		fprintf(stderr, "buffers differ\n");
		return 1;
	}

	close(fds[0]);
	close(fds[1]);
	io_uring_unregister_files(src);
	return 0;
}

static int test_local(struct io_uring *src, struct io_uring *dst)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int fds[2], fd, ret;
	char buf[32], dst_buf[32];
	int i;

	if (no_fd_pass)
		return 0;

	fd = -1;
	ret = io_uring_register_files(dst, &fd, 1);
	if (ret) {
		if (ret == -EBADF || ret == -EINVAL)
			return 0;
		fprintf(stderr, "register files failed: %d\n", ret);
		return 1;
	}

	if (pipe(fds) < 0) {
		perror("pipe");
		return 1;
	}

	fd = fds[0];
	ret = io_uring_register_files(src, &fd, 1);
	if (ret) {
		fprintf(stderr, "register files failed: %d\n", ret);
		return 1;
	}

	memset(dst_buf, 0, sizeof(dst_buf));
	for (i = 0; i < ARRAY_SIZE(buf); i++)
		buf[i] = rand();

	sqe = io_uring_get_sqe(src);
	io_uring_prep_write(sqe, fds[1], buf, sizeof(buf), 0);
	sqe->user_data = 1;

	sqe = io_uring_get_sqe(src);
	io_uring_prep_msg_ring_fd(sqe, dst->ring_fd, 0, 0, 10, 0);
	sqe->user_data = 2;
	
	io_uring_submit(src);

	fd = -1;
	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(src, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe: %d\n", ret);
			return 1;
		}
		if (cqe->user_data == 2 && cqe->res == -EINVAL) {
			no_fd_pass = 1;
		} else if (cqe->res < 0) {
			fprintf(stderr, "cqe res %d\n", cqe->res);
			return 1;
		}
		if (cqe->user_data == 1 && cqe->res != sizeof(buf)) {
			fprintf(stderr, "short write %d\n", cqe->res);
			return 1;
		}
		io_uring_cqe_seen(src, cqe);
	}

	if (no_fd_pass)
		goto out;

	ret = io_uring_wait_cqe(dst, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe dst: %d\n", ret);
		return 1;
	}
	if (cqe->res < 0) {
		fprintf(stderr, "cqe error dst: %d\n", cqe->res);
		return 1;
	}

	fd = cqe->res;
	io_uring_cqe_seen(dst, cqe);
	sqe = io_uring_get_sqe(dst);
	io_uring_prep_read(sqe, fd, dst_buf, sizeof(dst_buf), 0);
	sqe->flags |= IOSQE_FIXED_FILE;
	sqe->user_data = 3;
	io_uring_submit(dst);

	ret = io_uring_wait_cqe(dst, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe dst: %d\n", ret);
		return 1;
	}
	if (cqe->res < 0) {
		fprintf(stderr, "cqe error dst: %d\n", cqe->res);
		return 1;
	}
	if (cqe->res != sizeof(dst_buf)) {
		fprintf(stderr, "short read %d\n", cqe->res);
		return 1;
	}
	if (memcmp(buf, dst_buf, sizeof(buf))) {
		fprintf(stderr, "buffers differ\n");
		return 1;
	}

out:
	close(fds[0]);
	close(fds[1]);
	io_uring_unregister_files(src);
	io_uring_unregister_files(dst);
	return 0;
}

static int test(int ring_flags)
{
	struct io_uring ring, ring2;
	int ret;

	ret = io_uring_queue_init(8, &ring, ring_flags);
	if (ret) {
		if (ret == -EINVAL)
			return 0;
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}
	ret = io_uring_queue_init(8, &ring2, ring_flags);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = test_local(&ring, &ring2);
	if (ret) {
		fprintf(stderr, "test local failed\n");
		return T_EXIT_FAIL;
	}
	if (no_msg)
		return T_EXIT_SKIP;

	ret = test_remote(&ring, ring_flags);
	if (ret) {
		fprintf(stderr, "test_remote failed\n");
		return T_EXIT_FAIL;
	}

	io_uring_queue_exit(&ring);
	io_uring_queue_exit(&ring2);
	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test(0);
	if (ret != T_EXIT_PASS) {
		fprintf(stderr, "ring flags 0 failed\n");
		return ret;
	}
	if (no_msg)
		return T_EXIT_SKIP;

	ret = test(IORING_SETUP_SINGLE_ISSUER|IORING_SETUP_DEFER_TASKRUN);
	if (ret != T_EXIT_PASS) {
		fprintf(stderr, "ring flags defer failed\n");
		return ret;
	}

	return ret;
}
