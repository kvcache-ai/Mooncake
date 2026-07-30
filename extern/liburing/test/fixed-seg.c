/* SPDX-License-Identifier: MIT */
/*
 * Description: test various offset of fixed buffer read
 *
 */
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include "liburing.h"
#include "helpers.h"

static struct iovec rvec, wvec;

static int read_it(struct io_uring *ring, int fd, int len, int off)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_read_fixed(sqe, fd, rvec.iov_base + off, len, 0, 0);
	sqe->user_data = 1;

	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait %d\n", ret);
		return 1;
	}
	if (cqe->res < 0) {
		fprintf(stderr, "cqe res %s\n", strerror(-cqe->res));
		return 1;
	}
	if (cqe->res != len) {
		fprintf(stderr, "Bad read amount: %d\n", cqe->res);
		return 1;
	}
	io_uring_cqe_seen(ring, cqe);

	if (memcmp(wvec.iov_base, rvec.iov_base + off, len)) {
		fprintf(stderr, "%d %d verify failed\n", len, off);
		return 1;
	}

	return 0;
}

static int test(struct io_uring *ring, int fd, int vec_off)
{
	struct iovec v = rvec;
	int ret;

	v.iov_base += vec_off;
	v.iov_len -= vec_off;
	ret = io_uring_register_buffers(ring, &v, 1);
	if (ret) {
		fprintf(stderr, "Vec register: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = read_it(ring, fd, 4096, vec_off);
	if (ret) {
		fprintf(stderr, "4096 0 failed\n");
		return T_EXIT_FAIL;
	}
	ret = read_it(ring, fd, 8192, 4096);
	if (ret) {
		fprintf(stderr, "8192 4096 failed\n");
		return T_EXIT_FAIL;
	}
	ret = read_it(ring, fd, 4096, 4096);
	if (ret) {
		fprintf(stderr, "4096 4096 failed\n");
		return T_EXIT_FAIL;
	}
	
	io_uring_unregister_buffers(ring);
	return T_EXIT_PASS;
}

static int is_bdev(int fd)
{
	struct stat sb;

	if (fstat(fd, &sb) < 0) {
		perror("fstat");
		return -1;
	}

	return S_ISBLK(sb.st_mode);
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	const char *fname;
	char buf[256];
	int fd, rnd_fd, ret, bs = 512;

	if (argc > 1) {
		fname = argv[1];
	} else {
		srand((unsigned)time(NULL));
		snprintf(buf, sizeof(buf), ".fixed-seg-%u-%u", (unsigned) rand(),
				(unsigned)getpid());
		fname = buf;
		t_create_file(fname, 128*1024);
	}

	fd = open(fname, O_RDWR | O_DIRECT | O_EXCL);
	if (fd < 0) {
		perror("open");
		return 1;
	}
	ret = is_bdev(fd);
	if (fd < 0)
		return 1;
	if (ret && ioctl(fd, BLKSSZGET, &bs) < 0) {
		perror("ioctl BLKSSZGET,");
		return 1;
	}

	rnd_fd = open("/dev/urandom", O_RDONLY);
	if (fd < 0) {
		perror("urandom: open");
		goto err;
	}

	if (posix_memalign(&wvec.iov_base, 4096, 512*1024))
		goto err;
	wvec.iov_len = 512*1024;

	ret = read(rnd_fd, wvec.iov_base, wvec.iov_len);
	if (ret != wvec.iov_len) {
		fprintf(stderr, "Precondition, urandom read failed, ret: %d\n", ret);
		goto err;
	}

	ret = write(fd, wvec.iov_base, wvec.iov_len);
	if (ret != wvec.iov_len) {
		fprintf(stderr, "Precondition, write failed, ret: %d\n", ret);
		goto err;
	}

	if (posix_memalign(&rvec.iov_base, 4096, 512*1024))
		goto err;
	rvec.iov_len = 512*1024;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue_init: %d\n", ret);
		goto err;
	}

	ret = test(&ring, fd, 0);
	if (ret) {
		fprintf(stderr, "test 0 failed\n");
		goto err;
	}

	ret = test(&ring, fd, bs);
	if (ret) {
		fprintf(stderr, "test %d failed\n", bs);
		goto err;
	}

	if ((bs == 512) && test(&ring, fd, 3584)) {
		fprintf(stderr, "test 3584 failed\n");
		goto err;
	}

	close(fd);
	io_uring_queue_exit(&ring);
	if (fname != argv[1])
		unlink(fname);
	return T_EXIT_PASS;
err:
	if (fname != argv[1])
		unlink(fname);
	return T_EXIT_FAIL;
}
