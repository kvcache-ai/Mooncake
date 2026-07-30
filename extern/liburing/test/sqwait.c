/* SPDX-License-Identifier: MIT */
/*
 * Description: test that the app can always get a new sqe after having
 *		called io_uring_sqring_wait().
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "liburing.h"
#include "helpers.h"

#define NR_IOS	10000
#define INFLIGHT	256
#define FILE_SIZE	(256 * 1024 * 1024)

static int inflight;

static int reap(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	int ret;

	while (inflight >= INFLIGHT / 2) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret < 0) {
			fprintf(stderr, "wait=%d\n", ret);
			return 1;
		}
		if (cqe->res < 0) {
			printf("cqe res %d\n", cqe->res);
			return 1;
		}
		io_uring_cqe_seen(ring, cqe);
		inflight--;
	}

	return 0;
}

int main(int argc, char *argv[])
{
	struct io_uring_sqe *sqe;
	struct io_uring ring;
	int fd = -1, i, iov_off, ret, fret;
	struct iovec iovs[INFLIGHT];
	const char *fname;
	char buf[256];
	loff_t off;

	if (argc > 1) {
		fname = argv[1];
	} else {
		srand((unsigned)time(NULL));
		snprintf(buf, sizeof(buf), ".sqwait-%u-%u", (unsigned)rand(),
			 (unsigned)getpid());
		fname = buf;
		t_create_file(fname, FILE_SIZE);
	}

	fret = T_EXIT_SKIP;
	for (i = 0; i < INFLIGHT; i++) {
		if (posix_memalign(&iovs[i].iov_base, 4096, 4096))
			goto err;
		iovs[i].iov_len = 4096;
	}

	ret = io_uring_queue_init(8, &ring, IORING_SETUP_SQPOLL);
	if (ret < 0) {
		if (errno == EINVAL || errno == EPERM)
			goto err;
		fprintf(stderr, "queue init %d\n", ret);
		fret = T_EXIT_FAIL;
		goto err;
	}

	fd = open(fname, O_RDONLY | O_DIRECT);
	if (fd < 0) {
		if (errno == EACCES || errno == EPERM || errno == EINVAL)
			return T_EXIT_SKIP;
		perror("open");
		fret = T_EXIT_FAIL;
		goto err;
	}

	iov_off = off = 0;
	for (i = 0; i < NR_IOS; i++) {
		struct iovec *iov = &iovs[iov_off];

		sqe = io_uring_get_sqe(&ring);
		if (!sqe) {
			ret = io_uring_sqring_wait(&ring);
			if (ret < 0) {
				if (ret == -EINVAL)
					return T_EXIT_SKIP;
				fprintf(stderr, "sqwait=%d\n", ret);
				fret = T_EXIT_FAIL;
				goto err;
			}
			sqe = io_uring_get_sqe(&ring);
			if (!sqe) {
				fprintf(stderr, "No sqe post wait\n");
				fret = T_EXIT_FAIL;
				goto err;
			}
		}
		io_uring_prep_read(sqe, fd, iov->iov_base, iov->iov_len, 0);
		io_uring_submit(&ring);
		inflight++;

		iov_off++;
		if (iov_off == INFLIGHT)
			iov_off = 0;
		off += 8192;
		if (off > FILE_SIZE - 8192)
			off = 0;
		if (reap(&ring)) {
			fret = T_EXIT_FAIL;
			goto err;
		}
	}

	io_uring_queue_exit(&ring);
	fret = T_EXIT_PASS;
err:
	if (fd != -1)
		close(fd);
	if (fname != argv[1])
		unlink(fname);
	for (i = 0; i < INFLIGHT; i++)
		free(iovs[i].iov_base);
	return fret;
}
