/* SPDX-License-Identifier: MIT */
/*
 * Description: test that multishot read correctly keeps reading until all
 *		data has been emptied. the original implementation failed
 *		to do so, if the available buffer size was less than what
 *		was available, hence requiring multiple reads to empty the
 *		file buffer.
 */
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <sys/time.h>

#include "liburing.h"
#include "helpers.h"

#define BGID		17
#define NR_BUFS		4
#define BR_MASK		(NR_BUFS - 1)
#define BUF_SIZE	32

static int do_write(int fd, void *buf, int buf_size)
{
	int ret;

	ret = write(fd, buf, buf_size);
	if (ret < 0) {
		perror("write");
		return 0;
	} else if (ret != buf_size) {
		fprintf(stderr, "bad write size %d\n", ret);
		return 0;
	}

	return 1;
}

static void *thread_fn(void *data)
{
	char w1[BUF_SIZE], w2[BUF_SIZE];
	int *fds = data;

	memset(w1, 0x11, BUF_SIZE);
	memset(w2, 0x22, BUF_SIZE);

	if (!do_write(fds[1], w1, BUF_SIZE))
		return NULL;
	if (!do_write(fds[1], w2, BUF_SIZE))
		return NULL;

	usleep(100000);

	if (!do_write(fds[1], w1, BUF_SIZE))
		return NULL;
	if (!do_write(fds[1], w2, BUF_SIZE))
		return NULL;

	return NULL;
}

int main(int argc, char *argv[])
{
	struct io_uring_buf_ring *br;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	pthread_t thread;
	int i, ret, fds[2];
	void *buf, *tret;

	if (argc > 1)
		return T_EXIT_SKIP;

	if (pipe(fds) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	br = io_uring_setup_buf_ring(&ring, NR_BUFS, BGID, 0, &ret);
	if (!br) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "failed buffer ring %d\n", ret);
		return T_EXIT_FAIL;
	}

	buf = malloc(NR_BUFS * BUF_SIZE);
	for (i = 0; i < NR_BUFS; i++) {
		void *this_buf = buf + i * BUF_SIZE;

		io_uring_buf_ring_add(br, this_buf, BUF_SIZE, i, BR_MASK, i);
	}
	io_uring_buf_ring_advance(br, NR_BUFS);

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_read_multishot(sqe, fds[0], 0, 0, BGID);

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "bad submit %d\n", ret);
		return T_EXIT_FAIL;
	}

	/*
	 * read multishot not available would be ready as a cqe when
	 * submission returns, check and skip if not.
	 */
	ret = io_uring_peek_cqe(&ring, &cqe);
	if (!ret) {
		if (cqe->res == -EINVAL || cqe->res == -EBADF) {
			free(buf);
			return T_EXIT_SKIP;
		}
	}

	pthread_create(&thread, NULL, thread_fn, fds);

	for (i = 0; i < 4; i++) {
		int buf_index;

		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait %d\n", ret);
			break;
		}

		if (cqe->res != BUF_SIZE) {
			fprintf(stderr, "size %d\n", cqe->res);
			return T_EXIT_FAIL;
		}
		if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
			fprintf(stderr, "buffer not set\n");
			return T_EXIT_FAIL;
		}
		if (!(cqe->flags & IORING_CQE_F_MORE)) {
			fprintf(stderr, "more not set\n");
			return T_EXIT_FAIL;
		}
		buf_index = cqe->flags >> 16;
		assert(buf_index >= 0 && buf_index <= NR_BUFS);
		io_uring_cqe_seen(&ring, cqe);
	}

	pthread_join(thread, &tret);
	io_uring_free_buf_ring(&ring, br, NR_BUFS, BGID);
	io_uring_queue_exit(&ring);
	free(buf);
	return T_EXIT_PASS;
}
