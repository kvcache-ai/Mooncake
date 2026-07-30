/* SPDX-License-Identifier: MIT */
/*
 * Description: test waiting for more events than what will be posted with
 *		a timeout with DEFER_TASKRUN. All kernels should time out,
 *		but a non-buggy kernel will end up with one CQE available
 *		for reaping. Buggy kernels will not have processed the
 *		task_work and will have 0 events.
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

struct d {
	int fd;
};

static void *thread_fn(void *data)
{
	struct d *d = data;
	int ret;

	usleep(100000);
	ret = write(d->fd, "Hello", 5);
	if (ret < 0)
		perror("write");
	return NULL;
}

static int test_poll(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct __kernel_timespec ts;
	int ret, fds[2], i;
	pthread_t thread;
	char buf[32];
	struct d d;
	void *tret;

	if (pipe(fds) < 0) {
		perror("pipe");
		return 1;
	}
	d.fd = fds[1];

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_read(sqe, fds[0], buf, sizeof(buf), 0);

	pthread_create(&thread, NULL, thread_fn, &d);

	ts.tv_sec = 1;
	ts.tv_nsec = 0;

	ret = io_uring_submit_and_wait_timeout(ring, &cqe, 2, &ts, NULL);
	if (ret != 1) {
		fprintf(stderr, "unexpected wait ret %d\n", ret);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < 2; i++) {
		ret = io_uring_peek_cqe(ring, &cqe);
		if (ret)
			break;
		io_uring_cqe_seen(ring, cqe);
	}

	if (i != 1) {
		fprintf(stderr, "Got %d request, expected 1\n", i);
		return T_EXIT_FAIL;
	}

	pthread_join(thread, &tret);
	return T_EXIT_PASS;
}

static int test_file(struct io_uring *ring, char *__fname)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct __kernel_timespec ts;
	char filename[64], *fname;
	int fd, ret, i;
	void *buf;

	if (!__fname) {
		fname = filename;
		sprintf(fname, ".defer-tw-timeout.%d", getpid());
		t_create_file(fname, 128*1024);
	} else {
		fname = __fname;
	}

	fd = open(fname, O_RDONLY | O_DIRECT);
	if (fd < 0) {
		if (errno == EINVAL || errno == EPERM || errno == EACCES) {
			if (!__fname)
				unlink(fname);
			return T_EXIT_SKIP;
		}
		perror("open");
		if (!__fname)
			unlink(fname);
		return T_EXIT_FAIL;
	}

	if (!__fname)
		unlink(fname);

	if (posix_memalign(&buf, 4096, 4096)) {
		close(fd);
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_read(sqe, fd, buf, 4096, 0);

	ts.tv_sec = 1;
	ts.tv_nsec = 0;

	ret = io_uring_submit_and_wait_timeout(ring, &cqe, 2, &ts, NULL);
	if (ret != 1) {
		fprintf(stderr, "unexpected wait ret %d\n", ret);
		close(fd);
		free(buf);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < 2; i++) {
		ret = io_uring_peek_cqe(ring, &cqe);
		if (ret)
			break;
		io_uring_cqe_seen(ring, cqe);
	}

	if (i != 1) {
		fprintf(stderr, "Got %d request, expected 1\n", i);
		close(fd);
		free(buf);
		return T_EXIT_FAIL;
	}

	close(fd);
	free(buf);
	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	char *fname = NULL;
	int ret;

	ret = io_uring_queue_init(8, &ring, IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN);
	if (ret == -EINVAL)
		return T_EXIT_SKIP;

	if (argc > 1)
		fname = argv[1];

	ret = test_file(&ring, fname);
	if (ret != T_EXIT_PASS)
		return ret;

	ret = test_poll(&ring);
	if (ret != T_EXIT_PASS)
		return ret;

	return T_EXIT_PASS;
}
