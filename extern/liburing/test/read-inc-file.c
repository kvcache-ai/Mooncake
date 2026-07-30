/* SPDX-License-Identifier: MIT */
/*
 * Description: test reading a normal file with incremental buffer
 *		consumption. Some kernels had a bug where the initial part
 *		of the buffer got skipped, test for that.
 *
 */
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "liburing.h"
#include "helpers.h"

#define BUF_BGID	4
#define BUF_BID		8

static void arm_read(struct io_uring *ring, int fd, int offset)
{
	struct io_uring_sqe *sqe;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_read(sqe, fd, NULL, 80, offset);
	sqe->flags = IOSQE_BUFFER_SELECT;
	sqe->buf_group = BUF_BGID;
	io_uring_submit(ring);
}

static int create_test_file(const char *fname)
{
	char buf[80], c;
	int fd, i, ret;

	fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd < 0) {
		perror("open");
		return T_EXIT_FAIL;
	}

	c = 'a';
	for (i = 0; i < 8; i++) {
		memset(buf, c, sizeof(buf));
		ret = write(fd, buf, sizeof(buf));
		if (ret < 0) {
			perror("write");
			unlink(fname);
			return T_EXIT_FAIL;
		} else if (ret != sizeof(buf)) {
			fprintf(stderr, "Short write: %d\n", ret);
			unlink(fname);
			return T_EXIT_FAIL;
		}
		c++;
	}

	close(fd);
	return 0;
}

int main(int argc, char *argv[])
{
	struct io_uring_buf_ring *br;
	struct io_uring_params p = { };
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int tret, ret, fd, i;
	char fname[64];
	char c = 'a';
	char *buf;
	void *ptr;

	if (argc > 1)
		return T_EXIT_SKIP;

	sprintf(fname, ".buf-inc-file.%d", getpid());
	if (create_test_file(fname))
		return T_EXIT_FAIL;

	fd = open(fname, O_RDONLY);
	if (fd < 0) {
		perror("open");
		goto err;
	}

	ret = io_uring_queue_init_params(64, &ring, &p);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		goto err;
	}

	if (posix_memalign((void **) &buf, 4096, 65536))
		goto err;

	tret = T_EXIT_SKIP;
	br = io_uring_setup_buf_ring(&ring, 32, BUF_BGID, IOU_PBUF_RING_INC, &ret);
	if (!br) {
		if (ret == -EINVAL)
			goto out;
		fprintf(stderr, "Buffer ring register failed %d\n", ret);
		goto err;
	}

	tret = T_EXIT_PASS;
	io_uring_buf_ring_add(br, buf, 65536, BUF_BID, 31, 0);
	io_uring_buf_ring_advance(br, 1);

	memset(buf, 0, 65536);

	ptr = buf;
	for (i = 0; i < 4; i++) {
		int bid;

		arm_read(&ring, fd, i * 80);
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait %d\n", ret);
			goto err;
		}
		if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
			fprintf(stderr, "buffer not assigned\n");
			goto err;
		}
		bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
		if (bid != BUF_BID) {
			fprintf(stderr, "got wrong buffer bid %d\n", bid);
			goto err;
		}
		if (cqe->res != 80) {
			fprintf(stderr, "bad read size %d\n", ret);
			goto err;
		}
		io_uring_cqe_seen(&ring, cqe);
		if (!memchr(ptr, c, cqe->res)) {
			fprintf(stderr, "fail buffer check loop %d\n", i);
			goto err;
		}
		c++;
		ptr += cqe->res;
	}

	io_uring_free_buf_ring(&ring, br, 32, BUF_BGID);
	io_uring_queue_exit(&ring);
out:
	free(buf);
	unlink(fname);
	return tret;
err:
	unlink(fname);
	return T_EXIT_FAIL;
}
