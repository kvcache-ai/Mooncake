/* SPDX-License-Identifier: MIT */
/*
 * Description: test installing a direct descriptor into the regular
 *		file table
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

static int no_fd_install;

/* test that O_CLOEXEC is accepted, and others are not */
static int test_flags(struct io_uring *ring, int async)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, fds[2], fd;

	if (pipe(fds) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_files(ring, &fds[0], 1);
	if (ret) {
		fprintf(stderr, "failed register files %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* check that setting an invalid flag fails */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_fixed_fd_install(sqe, 0, 1U << 17);
	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res != -EINVAL) {
		fprintf(stderr, "unexpected cqe res %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(ring, cqe);

	/* check that IORING_FIXED_FD_NO_CLOEXEC is accepted */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_fixed_fd_install(sqe, 0, IORING_FIXED_FD_NO_CLOEXEC);
	if (async)
		sqe->flags |= IOSQE_ASYNC;
	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res < 0) {
		fprintf(stderr, "unexpected cqe res %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	fd = cqe->res;
	io_uring_cqe_seen(ring, cqe);

	close(fds[0]);
	close(fds[1]);
	close(fd);
	io_uring_unregister_files(ring);
	
	return T_EXIT_PASS;
}

static int test_linked(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, fds[2], fd, i;

	if (pipe(fds) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_files(ring, &fds[0], 1);
	if (ret) {
		fprintf(stderr, "failed register files %d\n", ret);
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	sqe->flags |= IOSQE_IO_LINK;
	sqe->user_data = 1;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_fixed_fd_install(sqe, 0, 0);
	sqe->user_data = 2;

	ret = io_uring_submit(ring);
	if (ret != 2) {
		fprintf(stderr, "submit: %d\n", ret);
		return T_EXIT_FAIL;
	}

	fd = -1;
	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait cqe %d\n", ret);
			return T_EXIT_FAIL;
		}
		if (cqe->res < 0) {
			fprintf(stderr, "unexpected cqe res %d\n", cqe->res);
			return T_EXIT_FAIL;
		}
		if (cqe->user_data == 2)
			fd = cqe->res;
		io_uring_cqe_seen(ring, cqe);
	}

	close(fds[0]);
	close(fds[1]);
	if (fd != -1)
		close(fd);
	io_uring_unregister_files(ring);
	return T_EXIT_PASS;
}

/* test not setting IOSQE_FIXED_FILE */
static int test_not_fixed(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, fds[2];

	if (pipe(fds) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_files(ring, &fds[0], 1);
	if (ret) {
		fprintf(stderr, "failed register files %d\n", ret);
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_fixed_fd_install(sqe, 0, 0);
	sqe->flags &= ~IOSQE_FIXED_FILE;
	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res != -EBADF) {
		fprintf(stderr, "unexpected cqe res %d\n", cqe->res);
		return T_EXIT_FAIL;
	}

	io_uring_cqe_seen(ring, cqe);

	close(fds[0]);
	close(fds[1]);
	io_uring_unregister_files(ring);
	
	return T_EXIT_PASS;
}

/* test invalid direct descriptor indexes */
static int test_bad_fd(struct io_uring *ring, int some_fd)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_fixed_fd_install(sqe, some_fd, 0);
	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res != -EBADF) {
		fprintf(stderr, "unexpected cqe res %d\n", cqe->res);
		return T_EXIT_FAIL;
	}

	io_uring_cqe_seen(ring, cqe);
	return T_EXIT_PASS;
}

/* test basic functionality of shifting a direct descriptor to a normal file */
static int test_working(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, fds[2];
	char buf[32];

	if (pipe(fds) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	/* register read side */
	ret = io_uring_register_files(ring, &fds[0], 1);
	if (ret) {
		fprintf(stderr, "failed register files %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* close normal descriptor */
	close(fds[0]);

	/* normal read should fail */
	ret = read(fds[0], buf, 1);
	if (ret != -1) {
		fprintf(stderr, "unexpected read ret %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (errno != EBADF) {
		fprintf(stderr, "unexpected read failure %d\n", errno);
		return T_EXIT_FAIL;
	}

	/* verify we can read the data */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_read(sqe, 0, buf, sizeof(buf), 0);
	sqe->flags |= IOSQE_FIXED_FILE;
	io_uring_submit(ring);

	/* put some data in the pipe */
	ret = write(fds[1], "Hello", 5);
	if (ret < 0) {
		perror("write");
		return T_EXIT_FAIL;
	} else if (ret != 5) {
		fprintf(stderr, "short write %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res != 5) {
		fprintf(stderr, "weird pipe read ret %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(ring, cqe);

	/* fixed pipe read worked, now re-install as a regular fd */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_fixed_fd_install(sqe, 0, 0);
	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res == -EINVAL) {
		no_fd_install = 1;
		return T_EXIT_SKIP;
	}
	if (cqe->res < 0) {
		fprintf(stderr, "failed install fd: %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	/* stash new pipe read side fd in old spot */
	fds[0] = cqe->res;
	io_uring_cqe_seen(ring, cqe);

	ret = write(fds[1], "Hello", 5);
	if (ret < 0) {
		perror("write");
		return T_EXIT_FAIL;
	} else if (ret != 5) {
		fprintf(stderr, "short write %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* normal pipe read should now work with new fd */
	ret = read(fds[0], buf, sizeof(buf));
	if (ret != 5) {
		fprintf(stderr, "unexpected read ret %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* close fixed file */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_close_direct(sqe, 0);
	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res) {
		fprintf(stderr, "close fixed fd %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(ring, cqe);

	ret = write(fds[1], "Hello", 5);
	if (ret < 0) {
		perror("write");
		return T_EXIT_FAIL;
	} else if (ret != 5) {
		fprintf(stderr, "short write %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* normal pipe read should still work with new fd */
	ret = read(fds[0], buf, sizeof(buf));
	if (ret != 5) {
		fprintf(stderr, "unexpected read ret %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* fixed fd pipe read should now fail */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_read(sqe, 0, buf, sizeof(buf), 0);
	sqe->flags = IOSQE_FIXED_FILE;
	io_uring_submit(ring);

	/* put some data in the pipe */
	ret = write(fds[1], "Hello", 5);
	if (ret < 0) {
		perror("write");
		return T_EXIT_FAIL;
	} else if (ret != 5) {
		fprintf(stderr, "short write %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res != -EBADF) {
		fprintf(stderr, "weird pipe read ret %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(ring, cqe);

	close(fds[0]);
	close(fds[1]);
	io_uring_unregister_files(ring);
	return T_EXIT_PASS;
}

static int test_creds(struct io_uring *ring, int async)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int cred_id, ret, fds[2];

	if (pipe(fds) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_files(ring, &fds[0], 1);
	if (ret) {
		fprintf(stderr, "failed register files %d\n", ret);
		return T_EXIT_FAIL;
	}

	cred_id = io_uring_register_personality(ring);
	if (cred_id < 0) {
		fprintf(stderr, "Failed registering creds: %d\n", cred_id);
		return T_EXIT_FAIL;
	}

	/* check that asking for creds fails */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_fixed_fd_install(sqe, 0, 0);
	if (async)
		sqe->flags |= IOSQE_ASYNC;
	sqe->personality = cred_id;
	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res > 0) {
		fprintf(stderr, "install succeeded with creds\n");
		return T_EXIT_FAIL;
	}
	if (cqe->res != -EPERM) {
		fprintf(stderr, "unexpected cqe res %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(ring, cqe);

	close(fds[0]);
	close(fds[1]);
	io_uring_unregister_files(ring);
	io_uring_unregister_personality(ring, cred_id);
	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(4, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = test_working(&ring);
	if (ret != T_EXIT_PASS) {
		if (ret == T_EXIT_FAIL)
			fprintf(stderr, "test_working failed\n");
		return ret;
	}
	if (no_fd_install)
		return T_EXIT_SKIP;

	ret = test_bad_fd(&ring, 0);
	if (ret != T_EXIT_PASS) {
		if (ret == T_EXIT_FAIL)
			fprintf(stderr, "test_bad_fd 0 failed\n");
		return ret;
	}

	ret = test_bad_fd(&ring, 500);
	if (ret != T_EXIT_PASS) {
		if (ret == T_EXIT_FAIL)
			fprintf(stderr, "test_bad_fd 500 failed\n");
		return ret;
	}
	
	ret = test_not_fixed(&ring);
	if (ret != T_EXIT_PASS) {
		if (ret == T_EXIT_FAIL)
			fprintf(stderr, "test_not_fixed failed\n");
		return ret;
	}

	ret = test_flags(&ring, 0);
	if (ret != T_EXIT_PASS) {
		if (ret == T_EXIT_FAIL)
			fprintf(stderr, "test_flags 0 failed\n");
		return ret;
	}

	ret = test_flags(&ring, 1);
	if (ret != T_EXIT_PASS) {
		if (ret == T_EXIT_FAIL)
			fprintf(stderr, "test_flags 1 failed\n");
		return ret;
	}

	ret = test_creds(&ring, 0);
	if (ret != T_EXIT_PASS) {
		if (ret == T_EXIT_FAIL)
			fprintf(stderr, "test_creds 0 failed\n");
		return ret;
	}

	ret = test_creds(&ring, 1);
	if (ret != T_EXIT_PASS) {
		if (ret == T_EXIT_FAIL)
			fprintf(stderr, "test_creds 1 failed\n");
		return ret;
	}

	ret = test_linked(&ring);
	if (ret != T_EXIT_PASS) {
		if (ret == T_EXIT_FAIL)
			fprintf(stderr, "test_linked failed\n");
		return ret;
	}
	
	return T_EXIT_PASS;
}
