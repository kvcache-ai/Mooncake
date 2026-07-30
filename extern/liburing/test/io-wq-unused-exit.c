/* SPDX-License-Identifier: MIT */
/*
 * Description: Ensure that an io-wq worker created by io_uring doesn't linger
 *		in the task thread group after the last ring is closed.
 *
 * This matters for checkpoint/restore of long-running processes which quiesce
 * and close their io_uring instance(s) before dumping process state.
 */
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "liburing.h"
#include "helpers.h"

#define IOWQ_COMM_PREFIX	"iou-wrk-"

static int count_iowq_workers(void)
{
	DIR *dir;
	struct dirent *de;
	int nr = 0;

	dir = opendir("/proc/self/task");
	if (!dir)
		return -errno;

	while ((de = readdir(dir)) != NULL) {
		char path[PATH_MAX];
		char comm[64];
		int fd, ret;

		if (de->d_name[0] == '.')
			continue;

		snprintf(path, sizeof(path), "/proc/self/task/%s/comm", de->d_name);
		fd = open(path, O_RDONLY);
		if (fd < 0)
			continue;

		ret = read(fd, comm, sizeof(comm) - 1);
		close(fd);
		if (ret <= 0)
			continue;

		comm[ret] = '\0';
		if (!strncmp(comm, IOWQ_COMM_PREFIX, strlen(IOWQ_COMM_PREFIX)))
			nr++;
	}

	closedir(dir);
	return nr;
}

static int wait_for_iowq_workers(bool want_any, unsigned long timeout_ms)
{
	struct timeval start;
	int nr = 0;

	gettimeofday(&start, NULL);
	do {
		nr = count_iowq_workers();
		if (nr < 0) {
			fprintf(stderr, "count_iowq_workers: %s\n", strerror(-nr));
			return T_EXIT_SKIP;
		}

		if (want_any) {
			if (nr > 0)
				return T_EXIT_PASS;
		} else {
			if (!nr)
				return T_EXIT_PASS;
		}

		usleep(10 * 1000);
	} while (mtime_since_now(&start) < timeout_ms);

	fprintf(stderr, "timeout waiting for iou-wrk threads (want_any=%d, nr=%d)\n",
		want_any, nr);
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	bool ring_inited = false;
	bool ring_exited = false;
	char src_fname[PATH_MAX];
	char dst_fname[PATH_MAX];
	char buf[32];
	int pipe_fds[2] = { -1, -1 };
	int fd_src = -1, fd_dst = -1;
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	if (t_create_ring(4, &ring, 0) != T_SETUP_OK)
		return T_EXIT_SKIP;
	ring_inited = true;

	snprintf(src_fname, sizeof(src_fname), ".iowq_unused_exit.%d.src", getpid());
	fd_src = open(src_fname, O_RDWR | O_CREAT | O_TRUNC, 0644);
	if (fd_src < 0) {
		perror("open source");
		ret = T_EXIT_SKIP;
		goto out;
	}

	memset(buf, 0xab, sizeof(buf));
	if (write(fd_src, buf, sizeof(buf)) != sizeof(buf)) {
		perror("write source");
		ret = T_EXIT_FAIL;
		goto out;
	}

	snprintf(dst_fname, sizeof(dst_fname), ".iowq_unused_exit.%d.dst", getpid());
	fd_dst = open(dst_fname, O_RDWR | O_CREAT | O_TRUNC, 0644);
	if (fd_dst < 0) {
		perror("open destination");
		ret = T_EXIT_SKIP;
		goto out;
	}

	if (pipe(pipe_fds) < 0) {
		perror("pipe");
		ret = T_EXIT_FAIL;
		goto out;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_splice(sqe, fd_src, 0, pipe_fds[1], -1, sizeof(buf), 0);
	sqe->user_data = 1;

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_splice(sqe, pipe_fds[0], -1, fd_dst, 0, sizeof(buf), 0);
	sqe->user_data = 2;

	ret = io_uring_submit(&ring);
	if (ret < 0) {
		fprintf(stderr, "io_uring_submit: %d\n", ret);
		ret = T_EXIT_FAIL;
		goto out;
	}
	if (ret != 2) {
		fprintf(stderr, "io_uring_submit: submitted %d (expected 2)\n", ret);
		ret = T_EXIT_FAIL;
		goto out;
	}

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe: %d\n", ret);
		ret = T_EXIT_FAIL;
		goto out;
	}
	if (cqe->res != sizeof(buf)) {
		fprintf(stderr, "splice cqe res %d (expected %zu)\n", cqe->res,
			sizeof(buf));
		ret = T_EXIT_FAIL;
		goto out;
	}
	io_uring_cqe_seen(&ring, cqe);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe: %d\n", ret);
		ret = T_EXIT_FAIL;
		goto out;
	}
	if (cqe->res != sizeof(buf)) {
		fprintf(stderr, "splice cqe res %d (expected %zu)\n", cqe->res,
			sizeof(buf));
		ret = T_EXIT_FAIL;
		goto out;
	}
	io_uring_cqe_seen(&ring, cqe);

	/*
	 * Ensure we actually created io-wq worker(s) before checking they exit
	 * after closing the last ring.
	 */
	ret = wait_for_iowq_workers(true, 1000);
	if (ret != T_EXIT_PASS) {
		if (ret == T_EXIT_FAIL)
			ret = T_EXIT_SKIP;
		goto out;
	}

	io_uring_queue_exit(&ring);
	ring_exited = true;

	/* Now the worker(s) should exit quickly. */
	ret = wait_for_iowq_workers(false, 2000);

out:
	if (ring_inited && !ring_exited)
		io_uring_queue_exit(&ring);
	if (pipe_fds[0] != -1)
		close(pipe_fds[0]);
	if (pipe_fds[1] != -1)
		close(pipe_fds[1]);
	if (fd_src != -1)
		close(fd_src);
	if (fd_dst != -1)
		close(fd_dst);
	if (fd_src != -1)
		unlink(src_fname);
	if (fd_dst != -1)
		unlink(dst_fname);
	return ret;
}
