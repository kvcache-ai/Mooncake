/* SPDX-License-Identifier: MIT */
#include <sys/mman.h>
#include <linux/mman.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <poll.h>
#include <pthread.h>
#include <errno.h>

#include "helpers.h"
#include "liburing.h"

static bool has_regvec;

struct buf_desc {
	char			*buf_wr;
	char			*buf_rd;
	size_t			size;

	struct io_uring 	ring;
	bool			ring_init;
	bool			fixed;
	int			buf_idx;
	bool			rw;
};

#define BUF_BASE_IDX	1
static int page_sz;

static void probe_support(void)
{
	struct io_uring_probe *p;
	struct io_uring ring;
	int ret = 0;

	ret = io_uring_queue_init(1, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue init failed: %d\n", ret);
		exit(ret);
	}

	p = t_calloc(1, sizeof(*p) + 256 * sizeof(struct io_uring_probe_op));
	ret = io_uring_register_probe(&ring, p, 256);

	/* if we don't have PROBE_REGISTER, we don't have OP_READ/WRITE */
	if (ret == -EINVAL)
		goto out;
	if (ret) {
		fprintf(stderr, "register_probe: %d\n", ret);
		goto out;
	}

	has_regvec = p->ops_len > IORING_OP_READV_FIXED &&
		     (p->ops[IORING_OP_READV_FIXED].flags & IO_URING_OP_SUPPORTED);
out:
	io_uring_queue_exit(&ring);
	if (p)
		free(p);
}

static void bind_ring(struct buf_desc *bd, struct io_uring *ring, unsigned buf_idx)
{
	size_t size = bd->size;
	struct iovec iov;
	int ret;

	iov.iov_len = size;
	iov.iov_base = bd->buf_wr;

	ret = io_uring_register_buffers_update_tag(ring, buf_idx, &iov, NULL, 1);
	if (ret != 1) {
		if (geteuid()) {
			fprintf(stderr, "Not root, skipping\n");
			exit(T_EXIT_SKIP);
		}
		fprintf(stderr, "buf reg failed %i\n", ret);
		exit(1);
	}
	bd->buf_idx = buf_idx;
}

static void reinit_ring(struct buf_desc *bd)
{
	struct io_uring *ring = &bd->ring;
	int ret;

	if (bd->ring_init) {
		io_uring_queue_exit(ring);
		bd->ring_init = false;
	}

	ret = io_uring_queue_init(32, ring, 0);
	if (ret) {
		fprintf(stderr, "ring init error %i\n", ret);
		exit(1);
	}

	ret = io_uring_register_buffers_sparse(ring, 128);
	if (ret) {
		fprintf(stderr, "table reg error %i\n", ret);
		exit(1);
	}

	bind_ring(bd, &bd->ring, BUF_BASE_IDX);
	bd->ring_init = true;
}

static void init_buffers(struct buf_desc *bd, size_t size)
{
	void *start;
	void *mem;

	start = mmap(NULL, size + page_sz * 2, PROT_NONE,
			MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
	if (start == MAP_FAILED) {
		fprintf(stderr, "Unable to preserve the page mixture memory.\n");
		exit(1);
	}

	mem = mmap(start + page_sz, size, PROT_READ | PROT_WRITE,
		   MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
	if (mem == MAP_FAILED) {
		fprintf(stderr, "mmap fail\n");
		exit(1);
	}

	memset(bd, 0, sizeof(*bd));
	bd->size = size;
	bd->buf_wr = mem;
	bd->buf_rd = malloc(size);
	if (!bd->buf_rd) {
		fprintf(stderr, "malloc fail\n");
		exit(1);
	}
}

static int verify_data(struct buf_desc *bd, struct iovec *wr_vecs, int nr_iovec,
			int fd)
{
	int iov_idx, ret;

	for (iov_idx = 0; iov_idx < nr_iovec; iov_idx++) {
		struct iovec *vec = &wr_vecs[iov_idx];
		size_t seg_size = vec->iov_len;
		size_t read_bytes = 0;

		while (1) {
			ret = read(fd, bd->buf_rd + read_bytes, seg_size - read_bytes);
			if (ret < 0) {
				fprintf(stderr, "read error %i", ret);
				return 1;
			}
			read_bytes += ret;
			if (read_bytes == seg_size)
				break;
			if (ret == 0) {
				fprintf(stderr, "can't read %i", ret);
				return 2;
			}
		}

		ret = memcmp(bd->buf_rd, vec->iov_base, seg_size);
		if (ret != 0) {
			fprintf(stderr, "data mismatch %i\n", ret);
			return 3;
		}
	}
	return 0;
}

struct verify_data {
	struct buf_desc *bd;
	struct iovec *vecs;
	int nr_vec;
	int fd;
};

static void *verify_thread_cb(void *data)
{
	struct verify_data *vd = data;
	int ret;

	ret = verify_data(vd->bd, vd->vecs, vd->nr_vec, vd->fd);
	return (void *)(unsigned long)ret;
}

static int test_rw(struct buf_desc *bd, struct iovec *vecs, int nr_vec, int fd_wr)
{
	unsigned buf_idx = bd->buf_idx;
	struct io_uring *ring = &bd->ring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	if (bd->fixed)
		io_uring_prep_writev_fixed(sqe, fd_wr, vecs, nr_vec, 0, 0, buf_idx);
	else
		io_uring_prep_writev(sqe, fd_wr, vecs, nr_vec, 0);

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "submit failed %i\n", ret);
		exit(1);
	}
	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe=%d\n", ret);
		exit(1);
	}

	ret = cqe->res;
	io_uring_cqe_seen(ring, cqe);
	return ret;
}

static int test_sendzc(struct buf_desc *bd, struct iovec *vecs, int nr_vec, int fd_wr)
{
	unsigned buf_idx = bd->buf_idx;
	struct io_uring *ring = &bd->ring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, cqe_ret, more;
	struct msghdr msghdr;

	memset(&msghdr, 0, sizeof(msghdr));
	msghdr.msg_iov = vecs;
	msghdr.msg_iovlen = nr_vec;

	sqe = io_uring_get_sqe(ring);
	if (bd->fixed)
		io_uring_prep_sendmsg_zc_fixed(sqe, fd_wr, &msghdr, 0, buf_idx);
	else
		io_uring_prep_sendmsg_zc(sqe, fd_wr, &msghdr, 0);

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "submit failed %i\n", ret);
		exit(1);
	}
	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe=%d\n", ret);
		exit(1);
	}

	cqe_ret = cqe->res;
	more = cqe->flags & IORING_CQE_F_MORE;
	io_uring_cqe_seen(ring, cqe);

	if (more) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe=%d\n", ret);
			exit(1);
		}
		io_uring_cqe_seen(ring, cqe);
	}
	return cqe_ret;
}

static int test_vec(struct buf_desc *bd, struct iovec *vecs, int nr_vec,
		    bool expect_fail, int *cqe_ret)
{
	struct sockaddr_storage addr;
	int sock_server, sock_client;
	struct verify_data vd;
	size_t total_len;
	int i, ret;
	void *verify_res;
	pthread_t th;

	ret = t_create_socketpair_ip(&addr, &sock_client, &sock_server,
					true, true, false, true, "::1");
	if (ret) {
		fprintf(stderr, "sock prep failed %d\n", ret);
		return 1;
	}

	for (i = 0; i < bd->size; i++)
		bd->buf_wr[i] = i;
	memset(bd->buf_rd, 0, bd->size);
	total_len = t_iovec_data_length(vecs, nr_vec);

	vd.bd = bd;
	vd.vecs = vecs;
	vd.nr_vec = nr_vec;
	vd.fd = sock_server;

	if (!expect_fail) {
		ret = pthread_create(&th, NULL, verify_thread_cb, &vd);
		if (ret) {
			fprintf(stderr, "pthread_create failed %i\n", ret);
			return ret;
		}
	}

	if (bd->rw)
		ret = test_rw(bd, vecs, nr_vec, sock_client);
	else
		ret = test_sendzc(bd, vecs, nr_vec, sock_client);

	*cqe_ret = ret;

	if (!expect_fail && ret != total_len) {
		fprintf(stderr, "invalid cqe %i, expected %lu\n",
				 ret, (unsigned long)total_len);
		return 1;
	}

	if (!expect_fail) {
		pthread_join(th, &verify_res);
		ret = (int)(unsigned long)verify_res;
		if (ret) {
			fprintf(stderr, "verify failed  %i\n", ret);
			return 1;
		}
	}
	close(sock_client);
	close(sock_server);
	return 0;
}

struct work {
	struct iovec	*vecs;
	unsigned	nr_vecs;
};

static int test_sequence(struct buf_desc *bd, unsigned nr, struct work *ws)
{
	int i, ret;
	int cqe_ret;

	reinit_ring(bd);

	for (i = 0; i < nr; i++) {
		ret = test_vec(bd, ws[i].vecs, ws[i].nr_vecs, false, &cqe_ret);
		if (ret) {
			fprintf(stderr, "sequence failed, idx %i/%i\n", i, nr);
			return ret;
		}
	}
	return 0;
}

static void test_basic(struct buf_desc *bd)
{
	void *p = bd->buf_wr;
	int ret;
	struct iovec iov_page =		{ .iov_base = p,
					  .iov_len = page_sz, };
	struct iovec iov_inner =	{ .iov_base = p + 1,
					  .iov_len = 3, };
	struct iovec iov_maxbvec =	{ .iov_base = p + page_sz - 1,
					  .iov_len = page_sz + 2, };
	struct iovec iov_big =		{ .iov_base = p,
					  .iov_len = page_sz * 12 + 33, };
	struct iovec iov_big_unalign =	{ .iov_base = p + 10,
					  .iov_len = page_sz * 7 + 41, };
	struct iovec iov_full =		{ .iov_base = p,
					  .iov_len = bd->size, };
	struct iovec iov_right1 =	{ .iov_base = p + bd->size - page_sz + 5,
					  .iov_len = page_sz - 5 };
	struct iovec iov_right2 =	{ .iov_base = p + bd->size - page_sz - 5,
					  .iov_len = page_sz + 5 };
	struct iovec iov_full_unalign = { .iov_base = p + 1,
					  .iov_len = bd->size - 1, };
	struct iovec vecs[] = {
		iov_page,
		iov_big,
		iov_inner,
		iov_big_unalign,
		iov_big_unalign,
	};
	struct iovec vecs_basic[] = { iov_page, iov_page, iov_page };
	struct iovec vecs_full[] = { iov_full, iov_full, iov_full };
	struct iovec vecs_full_unalign[] = { iov_full_unalign, iov_full_unalign,
					     iov_full_unalign };
	struct iovec vecs_maxsegs[] = { iov_maxbvec, iov_maxbvec, iov_maxbvec,
				      iov_maxbvec, iov_maxbvec, iov_maxbvec};

	ret = test_sequence(bd, 1, (struct work[]) {
			{ &iov_page, 1 },
			{ vecs, 1 }});
	if (ret) {
		fprintf(stderr, "seq failure: basic aligned, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 2, (struct work[]) {
			{ vecs, 1 },
			{ vecs, 1 }});
	if (ret) {
		fprintf(stderr, "seq failure: basic aligned, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 2, (struct work[]) {
			{ vecs + 1, 1 },
			{ vecs + 1, 1 }});
	if (ret) {
		fprintf(stderr, "seq failure: multi page buffer, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 2, (struct work[]) {
			{ vecs + 2, 1 },
			{ vecs + 2, 1 }});
	if (ret) {
		fprintf(stderr, "seq failure: misaligned buffer, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 2, (struct work[]) {
			{ vecs + 3, 1 },
			{ vecs + 3, 1 }});
	if (ret) {
		fprintf(stderr, "seq failure: misaligned multipage buffer, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 2, (struct work[]) {
			{ vecs, 1 },
			{ vecs + 3, 1 }});
	if (ret) {
		fprintf(stderr, "seq failure: realloc + increase bvec, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 2, (struct work[]) {
			{ vecs + 3, 1 },
			{ vecs + 0, 1 }});
	if (ret) {
		fprintf(stderr, "seq failure: realloc + decrease bvec, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 2, (struct work[]) {
			{ vecs, 4 },
			{ vecs, 4 }});
	if (ret) {
		fprintf(stderr, "seq failure: multisegment, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 3, (struct work[]) {
			{ vecs, 2 },
			{ vecs, 3 },
			{ vecs, 4 }});
	if (ret) {
		fprintf(stderr, "seq failure: multisegment 2, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 3, (struct work[]) {
			{ vecs_basic, 1 },
			{ vecs_basic, 2 },
			{ vecs_basic, 3 }});
	if (ret) {
		fprintf(stderr, "seq failure: increase iovec, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 3, (struct work[]) {
			{ vecs_basic, 3 },
			{ vecs_basic, 2 },
			{ vecs_basic, 1 }});
	if (ret) {
		fprintf(stderr, "seq failure: decrease iovec, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 3, (struct work[]) {
			{ &iov_right1, 1 },
			{ &iov_right2, 1 },
			{ &iov_right1, 1 }});
	if (ret) {
		fprintf(stderr, "seq failure: right aligned, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 3, (struct work[]) {
			{ vecs_full, 1 },
			{ vecs_full, 1 },
			{ vecs_full, 3 }});
	if (ret) {
		fprintf(stderr, "seq failure: full size, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 3, (struct work[]) {
			{ vecs_full_unalign, 1 },
			{ vecs_full_unalign, 1 },
			{ vecs_full_unalign, 3 }});
	if (ret) {
		fprintf(stderr, "seq failure: full size unsigned, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 3, (struct work[]) {
			{ vecs_maxsegs, 1 },
			{ vecs_maxsegs, 2 },
			{ vecs_maxsegs, 3 }});
	if (ret) {
		fprintf(stderr, "seq failure: overestimated segments, %i\n", ret);
		exit(1);
	}

	ret = test_sequence(bd, 3, (struct work[]) {
			{ vecs_maxsegs, 6 },
			{ vecs_maxsegs, 6 },
			{ vecs_maxsegs, 6 }});
	if (ret) {
		fprintf(stderr, "seq failure: overestimated segments 2, %i\n", ret);
		exit(1);
	}
}

static int test_readv_fixed(struct buf_desc *bd, struct iovec *vecs, int nr_vec)
{
	unsigned buf_idx = bd->buf_idx;
	struct io_uring *ring = &bd->ring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	size_t total_len, i, offset;
	int ret, fd;
	char *expected;
	char filename[] = ".readv_fixed.XXXXXX";

	total_len = t_iovec_data_length(vecs, nr_vec);

	expected = malloc(total_len);
	if (!expected) {
		fprintf(stderr, "malloc failed\n");
		return 1;
	}

	/* Fill expected buffer with pattern */
	for (i = 0; i < total_len; i++)
		expected[i] = i & 0xff;

	fd = mkstemp(filename);
	if (fd < 0) {
		perror("mkstemp");
		free(expected);
		return 1;
	}
	unlink(filename);

	/* Write expected data to file */
	ret = write(fd, expected, total_len);
	if (ret != total_len) {
		fprintf(stderr, "file write failed: %d\n", ret);
		close(fd);
		free(expected);
		return 1;
	}

	/* Clear registered buffer before reading into it */
	memset(bd->buf_wr, 0, bd->size);

	/* Read using READV_FIXED - vecs already point to bd->buf_wr (registered) */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_readv_fixed(sqe, fd, vecs, nr_vec, 0, 0, buf_idx);

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "submit failed %i\n", ret);
		close(fd);
		free(expected);
		return 1;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe=%d\n", ret);
		close(fd);
		free(expected);
		return 1;
	}

	ret = cqe->res;
	io_uring_cqe_seen(ring, cqe);

	close(fd);

	if (ret != total_len) {
		fprintf(stderr, "readv_fixed: got %d, expected %zu\n", ret, total_len);
		free(expected);
		return 1;
	}

	/* Verify data across all iovec segments */
	offset = 0;
	for (i = 0; i < nr_vec; i++) {
		if (memcmp(vecs[i].iov_base, expected + offset, vecs[i].iov_len) != 0) {
			fprintf(stderr, "readv_fixed: data mismatch at segment %zu\n", i);
			free(expected);
			return 1;
		}
		offset += vecs[i].iov_len;
	}

	free(expected);
	return 0;
}

static int test_readv(struct buf_desc *bd)
{
	void *p = bd->buf_wr;
	int ret;
	struct iovec iov_page = { .iov_base = p, .iov_len = page_sz };
	struct iovec iov_multi[] = {
		{ .iov_base = p, .iov_len = page_sz },
		{ .iov_base = p + page_sz, .iov_len = page_sz },
		{ .iov_base = p + page_sz * 2, .iov_len = page_sz },
	};
	struct iovec iov_unalign = { .iov_base = p + 1, .iov_len = page_sz - 1 };

	reinit_ring(bd);

	/* Single page read */
	ret = test_readv_fixed(bd, &iov_page, 1);
	if (ret) {
		fprintf(stderr, "readv_fixed: single page failed\n");
		return ret;
	}

	/* Multi-segment read */
	reinit_ring(bd);
	ret = test_readv_fixed(bd, iov_multi, 3);
	if (ret) {
		fprintf(stderr, "readv_fixed: multi-segment failed\n");
		return ret;
	}

	/* Unaligned read */
	reinit_ring(bd);
	ret = test_readv_fixed(bd, &iov_unalign, 1);
	if (ret) {
		fprintf(stderr, "readv_fixed: unaligned failed\n");
		return ret;
	}

	return 0;
}

static void test_fail(struct buf_desc *bd)
{
	int ret, cqe_ret;
	void *p = bd->buf_wr;
	struct iovec iov_0len = { .iov_base = p, .iov_len = 0 };
	struct iovec iov_0buf = { .iov_base = 0, .iov_len = 1 };
	struct iovec iov_inv = { .iov_base = (void *)-1U, .iov_len = 1 };
	struct iovec iov_under = { .iov_base = p - 1, .iov_len = 1 };
	struct iovec iov_over = { .iov_base = p + bd->size, .iov_len = 1 };
	struct iovec vecs_0[] = { iov_0len, iov_0len, iov_0len, iov_0len,
				   iov_0len, iov_0len, iov_0len, iov_0len };

	reinit_ring(bd);
	ret = test_vec(bd, vecs_0, 8, true, &cqe_ret);
	if (ret || cqe_ret > 0) {
		fprintf(stderr, "0 length test failed %i, cqe %i\n",
				ret, cqe_ret);
		exit(1);
	}

	reinit_ring(bd);
	ret = test_vec(bd, &iov_0buf, 1, true, &cqe_ret);
	if (ret || cqe_ret >= 0) {
		fprintf(stderr, "0 buf test failed %i, cqe %i\n",
				ret, cqe_ret);
		exit(1);
	}

	reinit_ring(bd);
	ret = test_vec(bd, &iov_inv, 1, true, &cqe_ret);
	if (ret || cqe_ret >= 0) {
		fprintf(stderr, "inv buf test failed %i, cqe %i\n",
				ret, cqe_ret);
		exit(1);
	}

	reinit_ring(bd);
	ret = test_vec(bd, &iov_under, 1, true, &cqe_ret);
	if (ret || cqe_ret >= 0) {
		fprintf(stderr, "inv buf underflow failed %i, cqe %i\n",
				ret, cqe_ret);
		exit(1);
	}

	reinit_ring(bd);
	ret = test_vec(bd, &iov_over, 1, true, &cqe_ret);
	if (ret || cqe_ret >= 0) {
		fprintf(stderr, "inv buf overflow failed %i, cqe %i\n",
				ret, cqe_ret);
		exit(1);
	}
}

int main(int argc, char *argv[])
{
	struct buf_desc bd = {};
	int ret, i;

	if (argc > 1)
		return T_EXIT_SKIP;

	page_sz = sysconf(_SC_PAGESIZE);

	probe_support();
	if (!has_regvec) {
		printf("doesn't support registered vector ops, skip\n");
		return T_EXIT_SKIP;
	}

	init_buffers(&bd, page_sz * 32);
	bd.fixed = true;

	for (i = 0; i < 2; i++) {
		bool rw = i & 1;

		bd.rw = rw;

		test_basic(&bd);
		test_fail(&bd);
	}

	ret = test_readv(&bd);
	if (ret) {
		fprintf(stderr, "test_readv failed\n");
		return T_EXIT_FAIL;
	}

	free(bd.buf_rd);
	io_uring_queue_exit(&bd.ring);
	return 0;
}
