/* SPDX-License-Identifier: MIT */
/*
 * Description: Check that closing a file with SQPOLL has it immediately closed
 *		upon receiving the CQE for the close. The 6.9 kernel had a bug
 *		where SQPOLL would not run kernel wide task_work when running the
 *		private task_work, which would defer the close if this was the
 *		final close of the file.
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "helpers.h"
#include "liburing.h"

static int fill_exec_target(char *dst, char *path)
{
	struct stat sb;

	/*
	 * Should either be ./exec-target.t or test/exec-target.t
	 */
	sprintf(dst, "%s", path);
	return stat(dst, &sb);
}

static int test_exec(struct io_uring *ring, char * const argv[])
{
	char prog_path[PATH_MAX];
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, wstatus, fd;
	pid_t p;

	if (fill_exec_target(prog_path, "./exec-target.t") &&
	    fill_exec_target(prog_path, "test/exec-target.t")) {
		fprintf(stdout, "Can't find exec-target, skipping\n");
		return 0;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_openat(sqe, AT_FDCWD, prog_path, O_WRONLY, 0);
	sqe->user_data = 0;

	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		return 1;
	}
	if (cqe->res < 0) {
		fprintf(stderr, "open: %d\n", cqe->res);
		return 1;
	}
	fd = cqe->res;
	io_uring_cqe_seen(ring, cqe);

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_close(sqe, fd);
	sqe->user_data = 1;

	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		return 1;
	}
	if (cqe->res < 0) {
		fprintf(stderr, "close: %d\n", cqe->res);
		return 1;
	}
	io_uring_cqe_seen(ring, cqe);

	p = fork();
	if (p == -1) {
		fprintf(stderr, "fork() failed\n");
		return 1;
	}

	if (p == 0) {
		/* file should be closed, try exec'ing it */
		ret = execve(prog_path, argv, NULL);
		if (ret) {
			fprintf(stderr, "exec failed: %s\n", strerror(errno));
			exit(1);
		}
	}

	if (waitpid(p, &wstatus, 0) == (pid_t)-1) {
		perror("waitpid()");
		return 1;
	}
	if (!WIFEXITED(wstatus) || WEXITSTATUS(wstatus))
		return 1;

	return 0;
}

int main(int argc, char * const argv[])
{
	struct io_uring_params p = { .flags = IORING_SETUP_SQPOLL, };
	struct io_uring ring;
	int ret, i;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = t_create_ring_params(8, &ring, &p);
	if (ret == T_SETUP_SKIP)
		return T_EXIT_SKIP;
	else if (ret != T_SETUP_OK)
		return T_EXIT_FAIL;

	for (i = 0; i < 20; i++) {
		ret = test_exec(&ring, argv);
		if (ret) {
			fprintf(stderr, "test_exec failed\n");
			return ret;
		}
	}

	return T_EXIT_PASS;
}
