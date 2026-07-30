/* SPDX-License-Identifier: MIT */
/*
 * Description: run various truncate tests
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/ioctl.h>

#include "liburing.h"
#include "helpers.h"

#define TWO_GIG_SIZE ((loff_t)2 * 1024 * 1024 * 1024)
#define ONE_GIG_SIZE ((loff_t)1024 * 1024 * 1024)
#define HALF_GIG_SIZE ((loff_t)512 * 1024 * 1024)

static int test_truncate(struct io_uring *ring, int fd)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret = -1;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		return T_EXIT_FAIL;
	}

	memset(sqe, 0, sizeof(*sqe));

	io_uring_prep_rw(IORING_OP_FTRUNCATE, sqe, fd, "fail", 0, 4);

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
		return T_EXIT_FAIL;
	}
	ret = cqe->res;
	io_uring_cqe_seen(ring, cqe);
	if (ret == -EINVAL)
		return T_EXIT_PASS;

	fprintf(stderr, "unexpected truncate res %d\n", ret);
	return T_EXIT_FAIL;
}

static int test_ftruncate(struct io_uring *ring, int fd, loff_t len)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		goto err;
	}

	memset(sqe, 0, sizeof(*sqe));

	io_uring_prep_ftruncate(sqe, fd, len);

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
		goto err;
	}
	ret = cqe->res;
	io_uring_cqe_seen(ring, cqe);
	return ret;
err:
	return 1;
}

static int get_file_size(int fd, loff_t *size)
{
	struct stat st;

	if (fstat(fd, &st) < 0) {
		perror("fstat");
		return -1;
	}
	if (S_ISREG(st.st_mode)) {
		*size = st.st_size;
		return 0;
	} else if (S_ISBLK(st.st_mode)) {
		unsigned long long bytes;

		if (ioctl(fd, BLKGETSIZE64, &bytes) != 0) {
			perror("ioctl");
			return -1;
		}

		*size = bytes;
		return 0;
	}

	return -1;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	char path[32] = ".truncate.XXXXXX";
	int ret;
	int fd;
	int i;
	loff_t size;
	loff_t test_sizes[3];

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(1, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	fd = mkostemp(path, O_WRONLY | O_CREAT | O_TRUNC);
	if (fd < 0) {
		perror("mkostemp");
		return T_EXIT_FAIL;
	}

	test_sizes[0] = TWO_GIG_SIZE;
	test_sizes[1] = ONE_GIG_SIZE;
	test_sizes[2] = HALF_GIG_SIZE;

	for (i = 0; i < 3; i++) {
		ret = test_ftruncate(&ring, fd, test_sizes[i]);
		if (ret < 0) {
			if (ret == -EBADF || ret == -EINVAL) {
				if (i == 0) {
					fprintf(stdout, "Ftruncate not supported, skipping\n");
					ret = T_EXIT_SKIP;
					goto out;
				}
				goto err;
			}
			fprintf(stderr, "ftruncate: %s\n", strerror(-ret));
			goto err;
		} else if (ret) {
			fprintf(stderr, "unexpected cqe->res %d\n", ret);
			goto err;
		}
		if (get_file_size(fd, &size))
			goto err;
		if (size != test_sizes[i]) {
			fprintf(stderr, "fail %d size=%llu, %llu\n", i,
				(unsigned long long) size,
				(unsigned long long) test_sizes[i]);
			goto err;
		}
	}

	ret = test_truncate(&ring, fd);
	if (ret != T_EXIT_PASS)
		goto err;

out:
	unlink(path);
	close(fd);
	return T_EXIT_PASS;
err:
	unlink(path);
	close(fd);
	return T_EXIT_FAIL;
}
