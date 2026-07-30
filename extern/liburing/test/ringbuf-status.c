/* SPDX-License-Identifier: MIT */
/*
 * Description: test reading provided ring buf head
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#include "liburing.h"
#include "helpers.h"

#define BUF_SIZE	32
#define NR_BUFS		8
#define FSIZE		(BUF_SIZE * NR_BUFS)

#define BR_MASK		(NR_BUFS - 1)
#define BGID		1

static int no_buf_ring;
static int no_buf_ring_status;

static int test_max(void)
{
	struct io_uring_buf_ring *br;
	struct io_uring ring;
	int nr_bufs = 32768;
	int ret, i;
	char *buf;

	ret = io_uring_queue_init(1, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return 1;
	}

	if (posix_memalign((void **) &buf, 4096, FSIZE))
		return 1;

	br = io_uring_setup_buf_ring(&ring, nr_bufs, BGID, 0, &ret);
	if (!br) {
		fprintf(stderr, "Buffer ring register failed %d\n", ret);
		return 1;
	}

	ret = io_uring_buf_ring_available(&ring, br, BGID);
	if (ret) {
		fprintf(stderr, "Bad available count %d\n", ret);
		return 1;
	}

	for (i = 0; i < nr_bufs / 2; i++)
		io_uring_buf_ring_add(br, buf, BUF_SIZE, i + 1, nr_bufs - 1, i);
	io_uring_buf_ring_advance(br, nr_bufs / 2);

	ret = io_uring_buf_ring_available(&ring, br, BGID);
	if (ret != nr_bufs / 2) {
		fprintf(stderr, "Bad half full available count %d\n", ret);
		return 1;
	}

	for (i = 0; i < nr_bufs / 2; i++)
		io_uring_buf_ring_add(br, buf, BUF_SIZE, i + 1, nr_bufs - 1, i);
	io_uring_buf_ring_advance(br, nr_bufs / 2);

	ret = io_uring_buf_ring_available(&ring, br, BGID);
	if (ret != nr_bufs) {
		fprintf(stderr, "Bad half full available count %d\n", ret);
		return 1;
	}

	free(buf);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}

static int test(int invalid)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	struct io_uring_buf_ring *br;
	int ret, i, fds[2];
	uint16_t head;
	char *buf;
	void *ptr;
	char output[16];

	memset(output, 0x55, sizeof(output));

	ret = io_uring_queue_init(NR_BUFS, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return 1;
	}

	if (pipe(fds) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	if (posix_memalign((void **) &buf, 4096, FSIZE))
		return 1;

	br = io_uring_setup_buf_ring(&ring, NR_BUFS, BGID, 0, &ret);
	if (!br) {
		if (ret == -EINVAL) {
			no_buf_ring = 1;
			free(buf);
			return 0;
		}
		fprintf(stderr, "Buffer ring register failed %d\n", ret);
		return 1;
	}

	ptr = buf;
	for (i = 0; i < NR_BUFS; i++) {
		io_uring_buf_ring_add(br, ptr, BUF_SIZE, i + 1, BR_MASK, i);
		ptr += BUF_SIZE;
	}
	io_uring_buf_ring_advance(br, NR_BUFS);

	/* head should be zero at this point */
	head = 1;
	if (!invalid)
		ret = io_uring_buf_ring_head(&ring, BGID, &head);
	else
		ret = io_uring_buf_ring_head(&ring, BGID + 10, &head);
	if (ret) {
		if (ret == -EINVAL) {
			no_buf_ring_status = 1;
			free(buf);
			return T_EXIT_SKIP;
		}
		if (invalid && ret == -ENOENT) {
			free(buf);
			return T_EXIT_PASS;
		}
		fprintf(stderr, "buf_ring_head: %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (invalid) {
		fprintf(stderr, "lookup of bad group id succeeded\n");
		return T_EXIT_FAIL;
	}
	if (head != 0) {
		fprintf(stderr, "bad head %d\n", head);
		return T_EXIT_FAIL;
	}

	ret = io_uring_buf_ring_available(&ring, br, BGID);
	if (ret != NR_BUFS) {
		fprintf(stderr, "ring available %d\n", ret);
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_read(sqe, fds[0], NULL, BUF_SIZE, i * BUF_SIZE);
	sqe->buf_group = BGID;
	sqe->flags |= IOSQE_BUFFER_SELECT;
	sqe->user_data = 1;

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* head should still be zero at this point, no buffers consumed */
	head = 1;
	ret = io_uring_buf_ring_head(&ring, BGID, &head);
	if (head != 0) {
		fprintf(stderr, "bad head after submit %d\n", head);
		return T_EXIT_FAIL;
	}

	ret = write(fds[1], output, sizeof(output));
	if (ret != sizeof(output)) {
		fprintf(stderr, "pipe buffer write %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe failed %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res != sizeof(output)) {
		fprintf(stderr, "cqe res %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
		fprintf(stderr, "no buffer selected\n");
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(&ring, cqe);

	/* head should now be one, we consumed a buffer */
	ret = io_uring_buf_ring_head(&ring, BGID, &head);
	if (head != 1) {
		fprintf(stderr, "bad head after cqe %d\n", head);
		return T_EXIT_FAIL;
	}

	ret = io_uring_buf_ring_available(&ring, br, BGID);
	if (ret != NR_BUFS - 1) {
		fprintf(stderr, "ring available %d\n", ret);
		return T_EXIT_FAIL;
	}

	close(fds[0]);
	close(fds[1]);
	free(buf);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	int ret;

	ret = test(0);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test 0 failed\n");
		return T_EXIT_FAIL;
	}
	if (no_buf_ring || no_buf_ring_status)
		return T_EXIT_SKIP;

	ret = test(1);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_max();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_max failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}
