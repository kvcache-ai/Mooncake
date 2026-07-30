/* SPDX-License-Identifier: MIT */
/*
 * Description: test toggling of iowait on kernels that support it
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "liburing.h"
#include "helpers.h"

static int get_iowait(int cpu)
{
	char cpu_buf[32], this_cpu[32];
	int user, nice, system, idle, iowait, ret;
	FILE *file;

	file = fopen("/proc/stat", "r");
	if (!file) {
		perror("fopen stat");
		return 0;
	}

	sprintf(this_cpu, "cpu%d", cpu);
	ret = 0;
	do {
		int r;
		r = fscanf(file, "%s %d %d %d %d %d", cpu_buf, &user, &nice,
				&system, &idle, &iowait);
		if (r != 6)
			continue;
		if (strncmp(cpu_buf, this_cpu, strlen(this_cpu)))
			continue;
		ret = iowait;
		break;
	} while (1);

	fclose(file);
	return ret;
}

static int test(struct io_uring *ring, int with_iowait, int cpu)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int iowait_pre, iowait_post;
	struct __kernel_timespec ts;
	int ret, fds[2], diff;
	char buf[32];

	if (!(ring->features & IORING_FEAT_NO_IOWAIT))
		return T_EXIT_SKIP;

	if (pipe(fds) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	ret = io_uring_set_iowait(ring, with_iowait);
	if (ret) {
		fprintf(stderr, "set_iowait=%d\n", ret);
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_read(sqe, fds[0], buf, sizeof(buf), 0);
	io_uring_submit(ring);

	ts.tv_sec = 1;
	ts.tv_nsec = 0;
	iowait_pre = get_iowait(cpu);
	ret = io_uring_wait_cqe_timeout(ring, &cqe, &ts);
	if (ret != -ETIME) {
		fprintf(stderr, "Unexpected wait ret: %d\n", ret);
		return T_EXIT_FAIL;
	}
	iowait_post = get_iowait(cpu);
	diff = iowait_post - iowait_pre;

	close(fds[0]);
	close(fds[1]);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "Unexpected wait ret: %d\n", ret);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(ring, cqe);

	if (with_iowait) {
		if (diff < 50) {
			fprintf(stderr, "iowait diff too small: %d\n", diff);
			return T_EXIT_FAIL;
		}
	} else {
		if (diff > 10) {
			fprintf(stderr, "iowait diff too large: %d\n", diff);
			return T_EXIT_FAIL;
		}
	}
	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	cpu_set_t cpuset;
	int ret;

	CPU_ZERO(&cpuset);
	CPU_SET(0, &cpuset);
	if (sched_setaffinity(0, sizeof(cpuset), &cpuset))
		return T_EXIT_SKIP;

	ret = t_create_ring(64, &ring, 0);
	if (ret == T_SETUP_SKIP)
		return T_EXIT_SKIP;
	else if (ret != T_SETUP_OK)
		return T_EXIT_FAIL;

	ret = test(&ring, 0, 0);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(&ring, 1, 0);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test 1 failed\n");
		return T_EXIT_FAIL;
	}

	io_uring_queue_exit(&ring);
	return 0;
}
