/* SPDX-License-Identifier: MIT */
/*
 * Test fixed buffers consisting of hugepages.
 */
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <linux/mman.h>

#include "liburing.h"
#include "helpers.h"

/*
 * Before testing
 * echo (>=4) > /proc/sys/vm/nr_hugepages
 * echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
 * echo always > /sys/kernel/mm/transparent_hugepage/hugepages-16kB/enabled
 *
 * Not 100% guaranteed to get THP-backed memory, but in general it does.
 */
#define MTHP_16KB	(16UL * 1024)
#define HUGEPAGE_SIZE	(2UL * 1024 * 1024)
#define NR_BUFS		1
#define IN_FD		"/dev/urandom"
#define OUT_FD		"/dev/zero"

static int open_files(char *fname_in, int *fd_in, int *fd_out)
{
	*fd_in = open(fname_in, O_RDONLY, 0644);
	if (*fd_in < 0) {
		printf("open %s failed\n", fname_in);
		return -1;
	}

	*fd_out = open(OUT_FD, O_RDWR, 0644);
	if (*fd_out < 0) {
		printf("open %s failed\n", OUT_FD);
		return -1;
	}

	return 0;
}

static void unmap(struct iovec *iov, int nr_bufs, size_t offset)
{
	int i;

	for (i = 0; i < nr_bufs; i++) {
		if (!iov[i].iov_base)
			continue;
		munmap(iov[i].iov_base - offset, iov[i].iov_len + offset);
	}
}

static int mmap_hugebufs(struct iovec *iov, int nr_bufs, size_t buf_size, size_t offset)
{
	int i;

	for (i = 0; i < nr_bufs; i++) {
		void *base = NULL;

		base = mmap(NULL, buf_size, PROT_READ | PROT_WRITE,
				MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
		if (base == MAP_FAILED) {
			printf("Unable to map hugetlb page. Try increasing the "
				"value in /proc/sys/vm/nr_hugepages\n");
			unmap(iov, i, offset);
			return -1;
		}

		memset(base, 0, buf_size);
		iov[i].iov_base = base + offset;
		iov[i].iov_len = buf_size - offset;
	}

	return 0;
}

/* map a hugepage and smaller page to a contiguous memory */
static int mmap_mixture(struct iovec *iov, int nr_bufs, size_t buf_size, bool huge_on_left)
{
	int i;
	void *small_base = NULL, *huge_base = NULL, *start = NULL,
	     *huge_start = NULL, *small_start = NULL;
	size_t small_size = buf_size - HUGEPAGE_SIZE;
	size_t seg_size = ((buf_size / HUGEPAGE_SIZE) + 1) * HUGEPAGE_SIZE;

	start = mmap(NULL, seg_size * nr_bufs, PROT_NONE,
			MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
	if (start == MAP_FAILED) {
		printf("Unable to preserve the page mixture memory. "
			"Try increasing the RLIMIT_MEMLOCK resource limit\n");
		return -1;
	}

	for (i = 0; i < nr_bufs; i++) {
		if (huge_on_left) {
			huge_start = start;
			small_start = start + HUGEPAGE_SIZE;
		} else {
			huge_start = start + HUGEPAGE_SIZE;
			small_start = start + HUGEPAGE_SIZE - small_size;
		}

		huge_base = mmap(huge_start, HUGEPAGE_SIZE, PROT_READ | PROT_WRITE,
				MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_FIXED, -1, 0);
		if (huge_base == MAP_FAILED) {
			printf("Unable to map hugetlb page in the page mixture. "
				"Try increasing the value in /proc/sys/vm/nr_hugepages\n");
			unmap(iov, nr_bufs, 0);
			return -1;
		}

		small_base = mmap(small_start, small_size, PROT_READ | PROT_WRITE,
				MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
		if (small_base == MAP_FAILED) {
			printf("Unable to map small page in the page mixture. "
				"Try increasing the RLIMIT_MEMLOCK resource limit\n");
			unmap(iov, nr_bufs, 0);
			return -1;
		}

		if (huge_on_left) {
			iov[i].iov_base = huge_base;
			memset(huge_base, 0, buf_size);
		}
		else {
			iov[i].iov_base = small_base;
			memset(small_base, 0, buf_size);
		}
		iov[i].iov_len = buf_size;
		start += seg_size;
	}

	return 0;
}

static void free_bufs(struct iovec *iov, int nr_bufs, size_t offset)
{
	int i;

	for (i = 0; i < nr_bufs; i++)
		free(iov[i].iov_base - offset);
}

static int get_mthp_bufs(struct iovec *iov, int nr_bufs, size_t buf_size,
		size_t alignment, size_t offset)
{
	int i;

	for (i = 0; i < nr_bufs; i++) {
		void *base = NULL;

		if (posix_memalign(&base, alignment, buf_size)) {
			printf("Unable to allocate mthp pages. "
				"Try increasing the RLIMIT_MEMLOCK resource limit\n");
			free_bufs(iov, i, offset);
			return -1;
		}

		memset(base, 0, buf_size);
		iov[i].iov_base = base + offset;
		iov[i].iov_len = buf_size - offset;
	}

	return 0;
}

static int do_read(struct io_uring *ring, int fd, struct iovec *iov, int nr_bufs)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int i, ret;

	for (i = 0; i < nr_bufs; i++) {
		sqe = io_uring_get_sqe(ring);
		if (!sqe) {
			fprintf(stderr, "Could not get SQE.\n");
			return -1;
		}

		io_uring_prep_read_fixed(sqe, fd, iov[i].iov_base, iov[i].iov_len, 0, i);
		io_uring_submit(ring);

		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret < 0) {
			fprintf(stderr, "Error waiting for completion: %s\n", strerror(-ret));
			return -1;
		}

		if (cqe->res < 0) {
			fprintf(stderr, "Error in async read operation: %s\n", strerror(-cqe->res));
			return -1;
		}
		if (cqe->res != iov[i].iov_len) {
			fprintf(stderr, "cqe res: %d, expected: %lu\n", cqe->res, (unsigned long) iov[i].iov_len);
			return -1;
		}

		io_uring_cqe_seen(ring, cqe);
	}

	return 0;
}

static int do_write(struct io_uring *ring, int fd, struct iovec *iov, int nr_bufs)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int i, ret;

	for (i = 0; i < nr_bufs; i++) {
		sqe = io_uring_get_sqe(ring);
		if (!sqe) {
			fprintf(stderr, "Could not get SQE.\n");
			return -1;
		}

		io_uring_prep_write_fixed(sqe, fd, iov[i].iov_base, iov[i].iov_len, 0, i);
		io_uring_submit(ring);

		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret < 0) {
			fprintf(stderr, "Error waiting for completion: %s\n", strerror(-ret));
			return -1;
		}

		if (cqe->res < 0) {
			fprintf(stderr, "Error in async write operation: %s\n", strerror(-cqe->res));
			return -1;
		}
		if (cqe->res != iov[i].iov_len) {
			fprintf(stderr, "cqe res: %d, expected: %lu\n", cqe->res, (unsigned long) iov[i].iov_len);
			return -1;
		}

		io_uring_cqe_seen(ring, cqe);
	}

	return 0;
}

static int register_submit(struct io_uring *ring, struct iovec *iov,
						int nr_bufs, int fd_in, int fd_out)
{
	int ret;

	ret = io_uring_register_buffers(ring, iov, nr_bufs);
	if (ret) {
		if (ret != -ENOMEM && ret != -EINVAL)
			fprintf(stderr, "Error registering buffers: %s\n", strerror(-ret));
		return ret;
	}

	ret = do_read(ring, fd_in, iov, nr_bufs);
	if (ret) {
		fprintf(stderr, "Read test failed\n");
		return ret;
	}

	ret = do_write(ring, fd_out, iov, nr_bufs);
	if (ret) {
		fprintf(stderr, "Write test failed\n");
		return ret;
	}

	ret = io_uring_unregister_buffers(ring);
	if (ret) {
		fprintf(stderr, "Error unregistering buffers for one hugepage test: %s", strerror(-ret));
		return ret;
	}

	return 0;
}

static int test_one_hugepage(struct io_uring *ring, int fd_in, int fd_out)
{
	struct iovec iov[NR_BUFS];
	size_t buf_size = HUGEPAGE_SIZE;
	int ret;

	if (mmap_hugebufs(iov, NR_BUFS, buf_size, 0))
		return T_EXIT_SKIP;

	ret = register_submit(ring, iov, NR_BUFS, fd_in, fd_out);
	unmap(iov, NR_BUFS, 0);
	if (ret == -ENOMEM || ret == -EINVAL)
		return T_EXIT_SKIP;
	return ret ? T_EXIT_FAIL : T_EXIT_PASS;
}

static int test_multi_hugepages(struct io_uring *ring, int fd_in, int fd_out)
{
	struct iovec iov[NR_BUFS];
	size_t buf_size = 4 * HUGEPAGE_SIZE;
	int ret;

	if (mmap_hugebufs(iov, NR_BUFS, buf_size, 0))
		return T_EXIT_SKIP;

	ret = register_submit(ring, iov, NR_BUFS, fd_in, fd_out);
	unmap(iov, NR_BUFS, 0);
	if (ret == -ENOMEM || ret == -EINVAL)
		return T_EXIT_SKIP;
	return ret ? T_EXIT_FAIL : T_EXIT_PASS;
}

static int test_unaligned_hugepage(struct io_uring *ring, int fd_in, int fd_out)
{
	struct iovec iov[NR_BUFS];
	size_t buf_size = 3 * HUGEPAGE_SIZE;
	size_t offset = 0x1234;
	int ret;

	if (mmap_hugebufs(iov, NR_BUFS, buf_size, offset))
		return T_EXIT_SKIP;

	ret = register_submit(ring, iov, NR_BUFS, fd_in, fd_out);
	unmap(iov, NR_BUFS, offset);
	if (ret == -ENOMEM || ret == -EINVAL)
		return T_EXIT_SKIP;
	return ret ? T_EXIT_FAIL : T_EXIT_PASS;
}

static int test_multi_unaligned_mthps(struct io_uring *ring, int fd_in, int fd_out)
{
	struct iovec iov[NR_BUFS];
	int ret;
	size_t buf_size = 3 * MTHP_16KB;
	size_t offset = 0x1234;

	if (get_mthp_bufs(iov, NR_BUFS, buf_size, MTHP_16KB, offset))
		return T_EXIT_SKIP;

	ret = register_submit(ring, iov, NR_BUFS, fd_in, fd_out);
	free_bufs(iov, NR_BUFS, offset);
	if (ret == -ENOMEM || ret == -EINVAL)
		return T_EXIT_SKIP;
	return ret ? T_EXIT_FAIL : T_EXIT_PASS;
}

/* Should not coalesce */
static int test_page_mixture(struct io_uring *ring, int fd_in, int fd_out, int huge_on_left)
{
	struct iovec iov[NR_BUFS];
	size_t buf_size = HUGEPAGE_SIZE + MTHP_16KB;
	int ret;

	if (mmap_mixture(iov, NR_BUFS, buf_size, huge_on_left))
		return T_EXIT_SKIP;

	ret = register_submit(ring, iov, NR_BUFS, fd_in, fd_out);
	unmap(iov, NR_BUFS, 0);
	if (ret == -ENOMEM || ret == -EINVAL)
		return T_EXIT_SKIP;
	return ret ? T_EXIT_FAIL : T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	int ret, fd_in, fd_out;
	char *fname_in;

	if (argc > 1)
		fname_in = argv[1];
	else
		fname_in = IN_FD;

	if (open_files(fname_in, &fd_in, &fd_out))
		return T_EXIT_SKIP;

	ret = t_create_ring(8, &ring, 0);
	if (ret == T_SETUP_SKIP)
		return T_EXIT_SKIP;
	else if (ret < 0)
		return T_EXIT_FAIL;

	ret = test_one_hugepage(&ring, fd_in, fd_out);
	if (ret != T_EXIT_PASS) {
		if (ret != T_EXIT_SKIP)
			fprintf(stderr, "Test one hugepage failed.\n");
		return ret;
	}

	ret = test_multi_hugepages(&ring, fd_in, fd_out);
	if (ret != T_EXIT_PASS) {
		if (ret != T_EXIT_SKIP)
			fprintf(stderr, "Test multi hugepages failed.\n");
		return ret;
	}

	ret = test_unaligned_hugepage(&ring, fd_in, fd_out);
	if (ret != T_EXIT_PASS) {
		if (ret != T_EXIT_SKIP)
			fprintf(stderr, "Test unaligned hugepage failed.\n");
		return ret;
	}

	ret = test_multi_unaligned_mthps(&ring, fd_in, fd_out);
	if (ret != T_EXIT_PASS) {
		if (ret != T_EXIT_SKIP)
			fprintf(stderr, "Test unaligned multi-size'd THPs failed.\n");
		return ret;
	}

	ret = test_page_mixture(&ring, fd_in, fd_out, true);
	if (ret != T_EXIT_PASS) {
		if (ret != T_EXIT_SKIP)
			fprintf(stderr, "Test huge small page mixture (start with huge) failed.\n");
		return ret;
	}

	ret = test_page_mixture(&ring, fd_in, fd_out, false);
	if (ret != T_EXIT_PASS) {
		if (ret != T_EXIT_SKIP)
			fprintf(stderr, "Test huge small page mixture (start with small) failed.\n");
		return ret;
	}

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}
