/* SPDX-License-Identifier: MIT */
/*
 * Description: Test that created thread that sets up a ring and causes io-wq
 *		worker creation doesn't need to wait for io-wq idle worker
 *		timeout to exit.
 *
 */
#include <fcntl.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "liburing.h"
#include "helpers.h"

#define BUF_LEN		20

static void *test(void *data)
{
	int fd_src = -1, fd_dst = -1, pipe_fds[2] = { -1, -1 };
	uint8_t buffer_write[BUF_LEN], buffer_read[BUF_LEN];
	char dst_fname[PATH_MAX], src_fname[PATH_MAX];
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int ret;

	ret = io_uring_queue_init(4, &ring, 0);
	if (ret < 0) {
		fprintf(stderr, "io_uring_queue_init failed: %d\n", ret);
		return NULL;
	}

	sprintf(src_fname, ".splice.%d.src", getpid());
	fd_src = open(src_fname, O_RDWR | O_CREAT | O_TRUNC, 0644);
	if (fd_src < 0) {
		perror("open source");
		goto cleanup;
	}

	sprintf(dst_fname, ".splice.%d.dst", getpid());
	fd_dst = open(dst_fname, O_RDWR | O_CREAT | O_TRUNC, 0644);
	if (fd_dst < 0) {
		perror("open destination");
		goto cleanup;
	}

	memset(buffer_write, 97, BUF_LEN);
	memset(buffer_read, 98, BUF_LEN);
	ret = pwrite(fd_src, buffer_write, BUF_LEN, 0);
	if (ret < 0) {
		perror("pwrite src");
		goto cleanup;
	}

	if (pipe(pipe_fds) < 0) {
		perror("pipe");
		goto cleanup;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_splice(sqe, fd_src, 0, pipe_fds[1], -1, 20, 0);
	sqe->user_data = 1;

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_splice(sqe, pipe_fds[0], -1, fd_dst, 10, 20, 0);
	sqe->user_data = 2;

	io_uring_submit(&ring);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		goto cleanup;
	}
	io_uring_cqe_seen(&ring, cqe);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		goto cleanup;
	}
	io_uring_cqe_seen(&ring, cqe);
cleanup:
	if (pipe_fds[0] != -1)
		close(pipe_fds[0]);
	if (pipe_fds[1] != -1)
		close(pipe_fds[1]);
	if (fd_src != -1)
		close(fd_src);
	if (fd_src != -1)
		close(fd_dst);
	io_uring_queue_exit(&ring);
	unlink(src_fname);
	unlink(dst_fname);
	return NULL;
}

static long get_time_ns(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec * 1000000000L + ts.tv_nsec;
}

int main(int argc, char *argv[])
{
	pthread_t thread;
	long start, end;

	if (argc > 1)
		return T_EXIT_SKIP;

	start = get_time_ns();
	pthread_create(&thread, NULL, test, NULL);
	pthread_join(thread, NULL);
	end = get_time_ns();
	end -= start;
	end /= 1000000;
	if (end >= 500) {
		fprintf(stderr, "Test took too long: %lu msec\n", end);
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}
