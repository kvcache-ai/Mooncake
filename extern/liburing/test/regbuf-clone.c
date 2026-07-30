/* SPDX-License-Identifier: MIT */
/*
 * Description: test buffer cloning between rings
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/uio.h>
#include <string.h>
#include <limits.h>
#include <sys/mman.h>
#include <linux/mman.h>

#include "liburing.h"
#include "helpers.h"

#define NR_VECS		64
#define BUF_SIZE	8192

static int no_buf_clone;
static int no_buf_offset;

static void fdinfo_read(struct io_uring *ring)
{
	char fd_name[128];
	char *buf;
	int fd;

	buf = malloc(4096);

	sprintf(fd_name, "/proc/self/fdinfo/%d", ring->ring_fd);
	fd = open(fd_name, O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}

	do {
		int ret = read(fd, buf, 4096);

		if (ret < 0) {
			perror("fdinfo read");
			break;
		} else if (ret == 4096) {
			continue;
		}
		break;
	} while (1);

	close(fd);
	free(buf);
}

static int use_buf(struct io_uring *ring, void *addr, int index)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	char src_buf[32];
	int fds[2], ret;

	fdinfo_read(ring);

	if (pipe(fds) < 0)
		return -errno;

	memset(src_buf, 0xbb, sizeof(src_buf));

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_read_fixed(sqe, fds[0], addr, sizeof(src_buf), 0, index);
	io_uring_submit(ring);

	ret = write(fds[1], src_buf, sizeof(src_buf));
	if (ret < 0)
		return -errno;

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe: %d\n", ret);
		return ret;
	}

	ret = cqe->res;
	io_uring_cqe_seen(ring, cqe);
	if (ret < 0)
		return ret;
	close(fds[0]);
	close(fds[1]);
	return 0;
}

static int test_offsets(void)
{
	struct iovec vecs[NR_VECS];
	struct io_uring src, dst;
	unsigned int i, offset, nr;
	int ret;

	ret = io_uring_queue_init(1, &src, 0);
	if (ret) {
		fprintf(stderr, "ring_init: %d\n", ret);
		return T_EXIT_FAIL;
	}
	ret = io_uring_queue_init(1, &dst, 0);
	if (ret) {
		fprintf(stderr, "ring_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < NR_VECS; i++) {
		if (posix_memalign(&vecs[i].iov_base, 4096, BUF_SIZE))
			return T_EXIT_FAIL;
		vecs[i].iov_len = BUF_SIZE;
	}

	ret = io_uring_register_buffers(&src, vecs, NR_VECS);
	if (ret < 0) {
		if (ret == -ENOMEM)
			return T_EXIT_SKIP;
		return T_EXIT_FAIL;
	}

	/* clone half the buffers, src offset 0, but ask for too many */
	offset = NR_VECS / 2;
	nr = NR_VECS;
	ret = io_uring_clone_buffers_offset(&dst, &src, 0, offset, nr, 0);
	if (ret != -EOVERFLOW) {
		if (ret == -EINVAL) {
			no_buf_offset = 1;
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "Offset and too big total failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* ask for too many buffers */
	nr = NR_VECS + 1;
	ret = io_uring_clone_buffers_offset(&dst, &src, 0, 0, nr, 0);
	if (ret != -EINVAL) {
		fprintf(stderr, "Too many buffers total failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* clone half the buffers into start of src offset */
	nr = NR_VECS / 2;
	ret = io_uring_clone_buffers_offset(&dst, &src, 0, nr, nr, 0);
	if (ret) {
		fprintf(stderr, "Half clone with offset failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* 'nr' offset should be 0 on the src side */
	ret = use_buf(&dst, vecs[nr].iov_base, 0);
	if (ret) {
		fprintf(stderr, "1 use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&dst);
	if (ret) {
		fprintf(stderr, "Failed to unregister partial dst: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = use_buf(&dst, vecs[0].iov_base, 0);
	if (ret != -EFAULT) {
		fprintf(stderr, "2 use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	/* clone half the buffers into middle of src offset */
	nr = NR_VECS / 2;
	ret = io_uring_clone_buffers_offset(&dst, &src, nr, nr, nr, 0);
	if (ret) {
		fprintf(stderr, "Half buffers and middle offset failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = use_buf(&dst, vecs[0].iov_base, 0);
	if (ret != -EFAULT) {
		fprintf(stderr, "3 use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = use_buf(&dst, vecs[nr].iov_base, nr);
	if (ret) {
		fprintf(stderr, "4 use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&dst);
	if (ret) {
		fprintf(stderr, "Failed to unregister partial dst: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* clone buffers, but specify overflowing dst offset */
	offset = UINT_MAX - 32;
	nr = NR_VECS;
	ret = io_uring_clone_buffers_offset(&dst, &src, 0, offset, nr, 0);
	if (ret != -EOVERFLOW) {
		fprintf(stderr, "Overflow dst offset failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* clone half the buffers into middle of src offset */
	nr = NR_VECS / 2;
	ret = io_uring_clone_buffers_offset(&dst, &src, nr, nr, nr, 0);
	if (ret) {
		fprintf(stderr, "Clone half middle src offset failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = use_buf(&dst, vecs[nr].iov_base, nr);
	if (ret) {
		fprintf(stderr, "5 use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = use_buf(&dst, vecs[0].iov_base, 0);
	if (ret != -EFAULT) {
		fprintf(stderr, "5 use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	/* should get -EBUSY now, REPLACE not set */
	nr = NR_VECS / 2;
	ret = io_uring_clone_buffers_offset(&dst, &src, nr, nr, nr, 0);
	if (ret != -EBUSY) {
		fprintf(stderr, "Replace buffers failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* now replace the initial 0..n in dst (which are dummy nodes) */
	ret = io_uring_clone_buffers_offset(&dst, &src, 0, 0, nr, IORING_REGISTER_DST_REPLACE);
	if (ret) {
		fprintf(stderr, "Buffer replace failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = use_buf(&dst, vecs[0].iov_base, 0);
	if (ret) {
		fprintf(stderr, "6 use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&dst);
	if (ret) {
		fprintf(stderr, "Failed to unregister partial dst: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_buffers_sparse(&dst, NR_VECS);
	if (ret) {
		fprintf(stderr, "Register sparse buffers failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* dst has a full sparse table, replace first NR_VECS / 2 with bufs */
	nr = NR_VECS / 2;
	ret = io_uring_clone_buffers_offset(&dst, &src, 0, 0, nr, 0);
	if (ret != -EBUSY) {
		fprintf(stderr, "Buffer replace failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_clone_buffers_offset(&dst, &src, 0, 0, nr, IORING_REGISTER_DST_REPLACE);
	if (ret) {
		fprintf(stderr, "Buffer replace failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = use_buf(&dst, vecs[0].iov_base, 0);
	if (ret) {
		fprintf(stderr, "7 use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	/* now expand existing dst table, from to NR_VECS + NR_VECS / 2 */
	nr = NR_VECS;
	offset = NR_VECS / 2;
	ret = io_uring_clone_buffers_offset(&dst, &src, offset, 0, nr, IORING_REGISTER_DST_REPLACE);
	if (ret) {
		fprintf(stderr, "Buffer replace failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = use_buf(&dst, vecs[0].iov_base, 0);
	if (ret) {
		fprintf(stderr, "8 use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	offset = NR_VECS + (NR_VECS / 2) - 1;
	ret = use_buf(&dst, vecs[NR_VECS - 1].iov_base, offset);
	if (ret) {
		fprintf(stderr, "8b use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = use_buf(&dst, vecs[NR_VECS / 2].iov_base, NR_VECS);
	if (ret) {
		fprintf(stderr, "9 use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < NR_VECS; i++)
		free(vecs[i].iov_base);

	return T_EXIT_PASS;
}

static int test(int reg_src, int reg_dst)
{
	struct iovec vecs[NR_VECS];
	struct io_uring src, dst;
	int ret, i;

	ret = io_uring_queue_init(1, &src, 0);
	if (ret) {
		fprintf(stderr, "ring_init: %d\n", ret);
		return T_EXIT_FAIL;
	}
	ret = io_uring_queue_init(1, &dst, 0);
	if (ret) {
		fprintf(stderr, "ring_init: %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (reg_src) {
		ret = io_uring_register_ring_fd(&src);
		if (ret < 0) {
			if (ret == -EINVAL)
				return T_EXIT_SKIP;
			fprintf(stderr, "register ring: %d\n", ret);
			return T_EXIT_FAIL;
		}
	}
	if (reg_dst) {
		ret = io_uring_register_ring_fd(&dst);
		if (ret < 0) {
			if (ret == -EINVAL)
				return T_EXIT_SKIP;
			fprintf(stderr, "register ring: %d\n", ret);
			return T_EXIT_FAIL;
		}
	}

	/* test fail with no buffers in src */
	ret = io_uring_clone_buffers(&dst, &src);
	if (ret == -EINVAL) {
		/* no buffer copy support */
		no_buf_clone = true;
		return T_EXIT_SKIP;
	} else if (ret != -ENXIO) {
		fprintf(stderr, "empty copy: %d\n", ret);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < NR_VECS; i++) {
		if (posix_memalign(&vecs[i].iov_base, 4096, BUF_SIZE))
			return T_EXIT_FAIL;
		vecs[i].iov_len = BUF_SIZE;
	}

	ret = io_uring_register_buffers(&src, vecs, NR_VECS);
	if (ret < 0) {
		if (ret == -ENOMEM)
			return T_EXIT_SKIP;
		return T_EXIT_FAIL;
	}

	ret = use_buf(&src, vecs[0].iov_base, 0);
	if (ret) {
		fprintf(stderr, "use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = use_buf(&dst, vecs[0].iov_base, 0);
	if (ret != -EFAULT) {
		fprintf(stderr, "use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	/* copy should work now */
	ret = io_uring_clone_buffers(&dst, &src);
	if (ret) {
		fprintf(stderr, "buffer copy: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = use_buf(&dst, vecs[NR_VECS / 2].iov_base, NR_VECS / 2);
	if (ret) {
		fprintf(stderr, "use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	/* try copy again, should get -EBUSY */
	ret = io_uring_clone_buffers(&dst, &src);
	if (ret != -EBUSY) {
		fprintf(stderr, "busy copy: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&dst);
	if (ret) {
		fprintf(stderr, "dst unregister buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = use_buf(&dst, vecs[NR_VECS / 2].iov_base, NR_VECS / 2);
	if (ret != -EFAULT) {
		fprintf(stderr, "use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&dst);
	if (ret != -ENXIO) {
		fprintf(stderr, "dst unregister empty buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = use_buf(&src, vecs[NR_VECS / 2].iov_base, NR_VECS / 2);
	if (ret) {
		fprintf(stderr, "use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&src);
	if (ret) {
		fprintf(stderr, "src unregister buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = use_buf(&src, vecs[NR_VECS / 2].iov_base, NR_VECS / 2);
	if (ret != -EFAULT) {
		fprintf(stderr, "use_buf=%d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_buffers(&dst, vecs, NR_VECS);
	if (ret < 0) {
		fprintf(stderr, "register buffers dst; %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_clone_buffers(&src, &dst);
	if (ret) {
		fprintf(stderr, "buffer copy reverse: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&dst);
	if (ret) {
		fprintf(stderr, "dst unregister buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&dst);
	if (ret != -ENXIO) {
		fprintf(stderr, "dst unregister empty buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&src);
	if (ret) {
		fprintf(stderr, "src unregister buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	io_uring_queue_exit(&src);
	io_uring_queue_exit(&dst);

	for (i = 0; i < NR_VECS; i++)
		free(vecs[i].iov_base);

	return T_EXIT_PASS;
}

static int test_dummy(void)
{
	struct iovec vec = { };
	struct io_uring src, dst;
	int ret;

	ret = io_uring_queue_init(1, &src, 0);
	if (ret) {
		fprintf(stderr, "ring_init: %d\n", ret);
		return T_EXIT_FAIL;
	}
	ret = io_uring_queue_init(1, &dst, 0);
	if (ret) {
		fprintf(stderr, "ring_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_buffers(&src, &vec, 1);
	if (ret < 0) {
		fprintf(stderr, "failed to register dummy buffer: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_clone_buffers(&dst, &src);
	if (ret) {
		fprintf(stderr, "clone dummy buf: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&src);
	if (ret) {
		fprintf(stderr, "rsc unregister buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&dst);
	if (ret) {
		fprintf(stderr, "dst unregister buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	io_uring_queue_exit(&src);
	io_uring_queue_exit(&dst);

	return T_EXIT_PASS;
}

/*
 * Register sparse buffer table, then try updating that with a few huge
 * page entries.
 */
static int test_merge(void)
{
	int ret, res = T_EXIT_SKIP;
	struct iovec vecs[8];
	struct io_uring ring;
	__u64 tags[2];
	void *p1;

	p1 = mmap(NULL, 2*1024*1024, PROT_READ|PROT_WRITE,
			MAP_PRIVATE|MAP_HUGETLB | MAP_HUGE_2MB | MAP_ANONYMOUS,
			-1, 0);
	if (p1 == MAP_FAILED)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(1, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	memset(vecs, 0, sizeof(vecs));

	ret = io_uring_register_buffers(&ring, vecs, 8);
	if (ret < 0) {
		if (ret == -EINVAL)
			goto skip;
		fprintf(stderr, "failed to register initial buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	vecs[0].iov_base = p1;
	vecs[0].iov_len = 4096;
	vecs[1].iov_base = p1 + 4096;
	vecs[1].iov_len = 4096;

	tags[0] = 1;
	tags[1] = 2;
	ret = io_uring_register_buffers_update_tag(&ring, 4, vecs, tags, 2);
	if (ret < 0) {
		if (ret == -EINVAL)
			goto skip;
		fprintf(stderr, "failed to register merge buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}
	res = T_EXIT_PASS;
skip:
	munmap(p1, 2*1024*1024);
	io_uring_queue_exit(&ring);
	return res;
}

static int test_same(void)
{
	struct iovec vecs[2] = { };
	struct io_uring src;
	int ret;

	ret = io_uring_queue_init(1, &src, 0);
	if (ret) {
		fprintf(stderr, "ring_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (posix_memalign(&vecs[0].iov_base, 4096, BUF_SIZE))
		return T_EXIT_SKIP;
	vecs[0].iov_len = BUF_SIZE;

	vecs[1].iov_base = NULL;
	vecs[1].iov_len = 0;

	ret = io_uring_register_buffers(&src, vecs, 2);
	if (ret) {
		fprintf(stderr, "reg buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_clone_buffers_offset(&src, &src, 1, 0, 2, IORING_REGISTER_DST_REPLACE);
	if (ret) {
		fprintf(stderr, "clone offset: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&src);
	if (ret) {
		fprintf(stderr, "rsc unregister buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	free(vecs[0].iov_base);
	io_uring_queue_exit(&src);
	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_merge();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_merge failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(0, 0);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test 0 0 failed\n");
		return T_EXIT_FAIL;
	}
	if (no_buf_clone)
		return T_EXIT_SKIP;

	ret = test(0, 1);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test 0 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(1, 0);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test 1 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(1, 1);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test 1 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_dummy();
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test_dummy failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_offsets();
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_PASS;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test_offset failed\n");
		return T_EXIT_FAIL;
	}
	if (no_buf_offset)
		return T_EXIT_PASS;

	ret = test_same();
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_PASS;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test_same failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}
