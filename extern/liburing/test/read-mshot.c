/* SPDX-License-Identifier: MIT */
/*
 * Description: test multishot read (IORING_OP_READ_MULTISHOT) on pipes,
 *		using ring provided buffers
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
#define BUF_SIZE_FIRST	17
#define NR_BUFS		64
#define BUF_BGID	1

#define BR_MASK		(NR_BUFS - 1)

#define NR_OVERFLOW	(NR_BUFS / 4)

static int no_buf_ring, no_read_mshot, no_buf_ring_inc;

static void arm_read(struct io_uring *ring, int fd, int use_mshot)
{
	struct io_uring_sqe *sqe;

	sqe = io_uring_get_sqe(ring);
	if (use_mshot) {
		io_uring_prep_read_multishot(sqe, fd, 0, 0, BUF_BGID);
	} else {
		io_uring_prep_read(sqe, fd, NULL, 0, 0);
		sqe->flags = IOSQE_BUFFER_SELECT;
		sqe->buf_group = BUF_BGID;
	}

	io_uring_submit(ring);
}

static int test_inc(int use_mshot, int flags)
{
	struct io_uring_buf_ring *br;
	struct io_uring_params p = { };
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int nbytes = 65536;
	int ret, fds[2], i;
	char tmp[31];
	char *buf;
	void *ptr;
	int bid = -1;
	int bid_bytes;

	if (no_buf_ring)
		return 0;

	p.flags = flags;
	ret = io_uring_queue_init_params(64, &ring, &p);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return 1;
	}

	if (pipe(fds) < 0) {
		perror("pipe");
		return 1;
	}

	if (posix_memalign((void **) &buf, 4096, 65536))
		return 1;

	br = io_uring_setup_buf_ring(&ring, 32, BUF_BGID, IOU_PBUF_RING_INC, &ret);
	if (!br) {
		if (ret == -EINVAL) {
			no_buf_ring_inc = 1;
			free(buf);
			return 0;
		}
		fprintf(stderr, "Buffer ring register failed %d\n", ret);
		return 1;
	}

	ptr = buf;
	buf = ptr + 65536 - 2048;
	for (i = 0; i < 32; i++) {
		io_uring_buf_ring_add(br, buf, 2048, i, 31, i);
		buf -= 2048;
	}
	io_uring_buf_ring_advance(br, 32);

	memset(tmp, 0x5a, sizeof(tmp));

	arm_read(&ring, fds[0], use_mshot);

	bid_bytes = 0;
	do {
		int write_size = sizeof(tmp);

		if (write_size > nbytes)
			write_size = nbytes;

		io_uring_get_events(&ring);
		ret = io_uring_peek_cqe(&ring, &cqe);
		if (!ret) {
			int this_bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
			if (bid == -1) {
				bid = this_bid;
			} else if (bid != this_bid) {
				if (bid_bytes != 2048) {
					fprintf(stderr, "unexpected bid bytes %d\n",
						bid_bytes);
					return 1;
				}
				bid = this_bid;
				bid_bytes = 0;
			}
			bid_bytes += cqe->res;
			nbytes -= cqe->res;
			if (!(cqe->flags & IORING_CQE_F_MORE))
				arm_read(&ring, fds[0], use_mshot);
			io_uring_cqe_seen(&ring, cqe);
			if (!nbytes)
				break;
		}
		usleep(1000);
		ret = write(fds[1], tmp, write_size);
		if (ret < 0) {
			perror("write");
			return 1;
		} else if (ret != write_size) {
			printf("short write %d\n", ret);
			return 1;
		}
	} while (nbytes);

	if (bid_bytes) {
		if (bid_bytes != 2048) {
			fprintf(stderr, "unexpected bid bytes %d\n", bid_bytes);
			return 1;
		}
	}

	io_uring_free_buf_ring(&ring, br, 32, BUF_BGID);
	io_uring_queue_exit(&ring);
	free(ptr);
	close(fds[0]);
	close(fds[1]);
	return 0;
}

static int test_clamp(void)
{
	struct io_uring_buf_ring *br;
	struct io_uring_params p = { };
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int ret, fds[2], i;
	char tmp[32];
	char *buf;
	void *ptr;

	ret = io_uring_queue_init_params(4, &ring, &p);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return 1;
	}

	if (pipe(fds) < 0) {
		perror("pipe");
		return 1;
	}

	if (posix_memalign((void **) &buf, 4096, NR_BUFS * BUF_SIZE))
		return 1;

	br = io_uring_setup_buf_ring(&ring, NR_BUFS, BUF_BGID, 0, &ret);
	if (!br) {
		if (ret == -EINVAL) {
			no_buf_ring = 1;
			return 0;
		}
		fprintf(stderr, "Buffer ring register failed %d\n", ret);
		return 1;
	}

	ptr = buf;
	io_uring_buf_ring_add(br, buf, 16, 1, BR_MASK, 0);
	buf += 16;
	io_uring_buf_ring_add(br, buf, 32, 2, BR_MASK, 1);
	buf += 32;
	io_uring_buf_ring_add(br, buf, 32, 3, BR_MASK, 2);
	buf += 32;
	io_uring_buf_ring_add(br, buf, 32, 4, BR_MASK, 3);
	buf += 32;
	io_uring_buf_ring_advance(br, 4);

	memset(tmp, 0xaa, sizeof(tmp));

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_read_multishot(sqe, fds[0], 0, 0, BUF_BGID);

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		return 1;
	}

	/* prevent pipe buffer merging */
	usleep(1000);
	ret = write(fds[1], tmp, 16);

	usleep(1000);
	ret = write(fds[1], tmp, sizeof(tmp));

	/* prevent pipe buffer merging */
	usleep(1000);
	ret = write(fds[1], tmp, 16);

	usleep(1000);
	ret = write(fds[1], tmp, sizeof(tmp));

	/*
	 * We should see a 16 byte completion, then a 32 byte, then a 16 byte,
	 * and finally a 32 byte again.
	 */
	for (i = 0; i < 4; i++) {
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait cqe failed %d\n", ret);
			return 1;
		}
		if (cqe->res < 0) {
			fprintf(stderr, "cqe res: %d\n", cqe->res);
			return 1;
		}
		if (!(cqe->flags & IORING_CQE_F_MORE)) {
			fprintf(stderr, "no more cqes\n");
			return 1;
		}
		if (i == 0 || i == 2) {
			if (cqe->res != 16) {
				fprintf(stderr, "%d cqe got %d\n", i, cqe->res);
				return 1;
			}
		} else if (i == 1 || i == 3) {
			if (cqe->res != 32) {
				fprintf(stderr, "%d cqe got %d\n", i, cqe->res);
				return 1;
			}
		}
		io_uring_cqe_seen(&ring, cqe);
	}

	io_uring_free_buf_ring(&ring, br, NR_BUFS, BUF_BGID);
	io_uring_queue_exit(&ring);
	free(ptr);
	return 0;
}

static int test(int first_good, int async, int overflow, int incremental)
{
	struct io_uring_buf_ring *br;
	struct io_uring_params p = { };
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int ret, fds[2], i, start_msg = 0;
	int br_flags = 0;
	char tmp[32];
	void *ptr[NR_BUFS];
	char *inc_index;

	p.flags = IORING_SETUP_CQSIZE;
	if (!overflow)
		p.cq_entries = NR_BUFS + 1;
	else
		p.cq_entries = NR_OVERFLOW;
	ret = io_uring_queue_init_params(1, &ring, &p);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return 1;
	}

	if (incremental) {
		if (no_buf_ring_inc)
			return 0;
		br_flags |= IOU_PBUF_RING_INC;
	}

	br = io_uring_setup_buf_ring(&ring, NR_BUFS, BUF_BGID, br_flags, &ret);
	if (!br) {
		if (ret == -EINVAL) {
			if (incremental) {
				no_buf_ring_inc = 1;
				return 0;
			}
			no_buf_ring = 1;
			return 0;
		}
		fprintf(stderr, "Buffer ring register failed %d\n", ret);
		return 1;
	}

	if (pipe(fds) < 0) {
		perror("pipe");
		return 1;
	}

	if (!incremental) {
		for (i = 0; i < NR_BUFS; i++) {
			unsigned size = i <= 1 ? BUF_SIZE_FIRST : BUF_SIZE;
			ptr[i] = malloc(size);
			if (!ptr[i])
				return 1;
			io_uring_buf_ring_add(br, ptr[i], size, i + 1, BR_MASK, i);
		}
		inc_index = NULL;
		io_uring_buf_ring_advance(br, NR_BUFS);
	} else {
		inc_index = ptr[0] = malloc(NR_BUFS * BUF_SIZE);
		memset(inc_index, 0, NR_BUFS * BUF_SIZE);
		io_uring_buf_ring_add(br, ptr[0], NR_BUFS * BUF_SIZE, 1, BR_MASK, 0);
		io_uring_buf_ring_advance(br, 1);
	}

	if (first_good) {
		sprintf(tmp, "this is buffer %d\n", start_msg++);
		ret = write(fds[1], tmp, strlen(tmp));
	}

	sqe = io_uring_get_sqe(&ring);
	/* len == 0 means just use the defined provided buffer length */
	io_uring_prep_read_multishot(sqe, fds[0], 0, 0, BUF_BGID);
	if (async)
		sqe->flags |= IOSQE_ASYNC;

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		return 1;
	}

	/* write NR_BUFS + 1, or if first_good is set, NR_BUFS */
	for (i = 0; i < NR_BUFS + !first_good; i++) {
		/* prevent pipe buffer merging */
		usleep(1000);
		sprintf(tmp, "this is buffer %d\n", i + start_msg);
		ret = write(fds[1], tmp, strlen(tmp));
		if (ret != strlen(tmp)) {
			fprintf(stderr, "write ret %d\n", ret);
			return 1;
		}
	}

	for (i = 0; i < NR_BUFS + 1; i++) {
		int bid;

		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait cqe failed %d\n", ret);
			return 1;
		}
		if (cqe->res < 0) {
			/* expected failure as we try to read one too many */
			if (cqe->res == -ENOBUFS && i == NR_BUFS)
				break;
			if (!i && cqe->res == -EINVAL) {
				no_read_mshot = 1;
				break;
			}
			fprintf(stderr, "%d: cqe res %d\n", i, cqe->res);
			return 1;
		} else if (i > 9 && cqe->res <= 17) {
			fprintf(stderr, "truncated message %d %d\n", i, cqe->res);
			return 1;
		}

		if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
			fprintf(stderr, "no buffer selected\n");
			return 1;
		}
		bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
		if (incremental && bid != 1) {
			fprintf(stderr, "bid %d for incremental\n", bid);
			return 1;
		}
		if (incremental && !first_good) {
			char out_buf[64];
			sprintf(out_buf, "this is buffer %d\n", i + start_msg);
			if (strncmp(inc_index, out_buf, strlen(out_buf)))
				return 1;
			inc_index += cqe->res;
		}
		if (!(cqe->flags & IORING_CQE_F_MORE)) {
			/* we expect this on overflow */
			if (overflow && i >= NR_OVERFLOW)
				break;
			fprintf(stderr, "no more cqes\n");
			return 1;
		}
		/* should've overflown! */
		if (overflow && i > NR_OVERFLOW) {
			fprintf(stderr, "Expected overflow!\n");
			return 1;
		}
		io_uring_cqe_seen(&ring, cqe);
	}


	io_uring_free_buf_ring(&ring, br, NR_BUFS, BUF_BGID);
	io_uring_queue_exit(&ring);
	if (incremental) {
		free(ptr[0]);
	} else {
		for (i = 0; i < NR_BUFS; i++)
			free(ptr[i]);
	}
	return 0;
}

static int test_invalid(int async)
{
	struct io_uring_buf_ring *br;
	struct io_uring_params p = { };
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	char fname[32] = ".mshot.%d.XXXXXX";
	int ret, fd;
	char *buf;

	p.flags = IORING_SETUP_CQSIZE;
	p.cq_entries = NR_BUFS;
	ret = io_uring_queue_init_params(1, &ring, &p);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return 1;
	}

	fd = mkstemp(fname);
	if (fd < 0) {
		perror("mkstemp");
		return 1;
	}
	unlink(fname);

	if (posix_memalign((void **) &buf, 4096, BUF_SIZE))
		return 1;

	br = io_uring_setup_buf_ring(&ring, 1, BUF_BGID, 0, &ret);
	if (!br) {
		fprintf(stderr, "Buffer ring register failed %d\n", ret);
		return 1;
	}

	io_uring_buf_ring_add(br, buf, BUF_SIZE, 1, BR_MASK, 0);
	io_uring_buf_ring_advance(br, 1);

	sqe = io_uring_get_sqe(&ring);
	/* len == 0 means just use the defined provided buffer length */
	io_uring_prep_read_multishot(sqe, fd, 0, 0, BUF_BGID);
	if (async)
		sqe->flags |= IOSQE_ASYNC;

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		return 1;
	}

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe failed %d\n", ret);
		return 1;
	}
	if (cqe->flags & IORING_CQE_F_MORE) {
		fprintf(stderr, "MORE flag set unexpected %d\n", cqe->flags);
		return 1;
	}
	if (cqe->res != -EBADFD) {
		fprintf(stderr, "Got cqe res %d, wanted -EBADFD\n", cqe->res);
		return 1;
	}

	io_uring_cqe_seen(&ring, cqe);
	io_uring_free_buf_ring(&ring, br, 1, BUF_BGID);
	io_uring_queue_exit(&ring);
	free(buf);
	return 0;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test(0, 0, 0, 0);
	if (ret) {
		fprintf(stderr, "test 0 0 0 failed\n");
		return T_EXIT_FAIL;
	}
	if (no_buf_ring || no_read_mshot) {
		printf("skip\n");
		return T_EXIT_SKIP;
	}

	ret = test(0, 1, 0, 0);
	if (ret) {
		fprintf(stderr, "test 0 1 0, failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(1, 0, 0, 0);
	if (ret) {
		fprintf(stderr, "test 1 0 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(0, 0, 1, 0);
	if (ret) {
		fprintf(stderr, "test 0 0 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(0, 1, 1, 0);
	if (ret) {
		fprintf(stderr, "test 0 1 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(1, 0, 1, 0);
	if (ret) {
		fprintf(stderr, "test 1 0 1, failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(1, 0, 1, 0);
	if (ret) {
		fprintf(stderr, "test 1 0 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(1, 1, 1, 0);
	if (ret) {
		fprintf(stderr, "test 1 1 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(0, 0, 0, 1);
	if (ret) {
		fprintf(stderr, "test 0 0 0 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(0, 0, 1, 1);
	if (ret) {
		fprintf(stderr, "test 0 0 1 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(0, 1, 0, 1);
	if (ret) {
		fprintf(stderr, "test 0 1 0 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(0, 1, 1, 1);
	if (ret) {
		fprintf(stderr, "test 0 1 1 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(1, 0, 0, 1);
	if (ret) {
		fprintf(stderr, "test 1 0 0 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(1, 0, 1, 1);
	if (ret) {
		fprintf(stderr, "test 1 0 1 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(1, 1, 0, 1);
	if (ret) {
		fprintf(stderr, "test 1 1 0 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(1, 1, 1, 1);
	if (ret) {
		fprintf(stderr, "test 1 1 1 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_invalid(0);
	if (ret) {
		fprintf(stderr, "test_invalid 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_invalid(1);
	if (ret) {
		fprintf(stderr, "test_invalid 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_clamp();
	if (ret) {
		fprintf(stderr, "test_clamp failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_inc(0, 0);
	if (ret) {
		fprintf(stderr, "test_inc 0 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_inc(0, IORING_SETUP_SQPOLL);
	if (ret) {
		fprintf(stderr, "test_inc 0 sqpoll failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_inc(0, IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN);
	if (ret) {
		fprintf(stderr, "test_inc 0 defer failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_inc(1, 0);
	if (ret) {
		fprintf(stderr, "test_inc 1 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_inc(1, IORING_SETUP_SQPOLL);
	if (ret) {
		fprintf(stderr, "test_inc 1 sqpoll failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_inc(1, IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN);
	if (ret) {
		fprintf(stderr, "test_inc 1 defer failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}
