/* SPDX-License-Identifier: MIT */
/*
 * Description: mixed sqes utilizing basic nop and io_uring passthrough commands
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>

#include "helpers.h"
#include "liburing.h"
#include "nvme.h"

#define len 0x1000
static unsigned char buf[len];
static int seq;

static int test_single_nop(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		return T_EXIT_FAIL;
	}

	io_uring_prep_nop(sqe);
	sqe->user_data = ++seq;

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
	} else if (cqe->user_data != seq) {
		fprintf(stderr, "Unexpected user_data: %ld\n", (long) cqe->user_data);
	} else {
		io_uring_cqe_seen(ring, cqe);
		return T_EXIT_PASS;
	}
	return T_EXIT_FAIL;
}

static int test_single_nvme_read(struct io_uring *ring, int fd)
{
	struct nvme_uring_cmd *cmd;
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret;

	sqe = io_uring_get_sqe128(ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		return T_EXIT_FAIL;
	}

	io_uring_prep_uring_cmd128(sqe, NVME_URING_CMD_IO, fd);
	sqe->user_data = ++seq;

	cmd = (struct nvme_uring_cmd *)sqe->cmd;
	memset(cmd, 0, sizeof(struct nvme_uring_cmd));
	cmd->opcode = nvme_cmd_read;
	cmd->cdw12 = (len >> lba_shift) - 1;
	cmd->addr = (__u64)(uintptr_t)buf;
	cmd->data_len = len;
	cmd->nsid = nsid;

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
	} else if (cqe->res != 0) {
		fprintf(stderr, "cqe res %d, wanted 0\n", cqe->res);
	} else if (cqe->user_data != seq) {
		fprintf(stderr, "Unexpected user_data: %ld\n", (long) cqe->user_data);
	} else {
		io_uring_cqe_seen(ring, cqe);
		return T_EXIT_PASS;
	}
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	struct stat sb;
	int fd, ret, i;

	if (argc < 2)
		return T_EXIT_SKIP;

	ret = nvme_get_info(argv[1]);
	if (ret)
		return T_EXIT_SKIP;

	fd = open(argv[1], O_RDONLY);
	if (fd < 0) {
		if (errno == EACCES || errno == EPERM)
			return T_EXIT_SKIP;
		perror("file open");
		return T_EXIT_FAIL;
	}

	if (fstat(fd, &sb) < 0) {
		perror("fstat");
		ret = T_EXIT_FAIL;
		goto close;
	}
	if (!S_ISCHR(sb.st_mode)) {
		ret = T_EXIT_SKIP;
		goto close;
	}

	ret = io_uring_queue_init(8, &ring,
		IORING_SETUP_CQE_MIXED | IORING_SETUP_SQE_MIXED);
	if (ret) {
		if (ret == -EINVAL) {
			ret = T_EXIT_SKIP;
		} else {
			fprintf(stderr, "ring setup failed: %d\n", ret);
			ret = T_EXIT_FAIL;
		}
		goto close;
	}

	for (i = 0; i < 32; i++) {
		if (i & 1)
			ret = test_single_nvme_read(&ring, fd);
		else
			ret = test_single_nop(&ring);

		if (ret)
			break;
	}

	io_uring_queue_exit(&ring);
close:
	close(fd);
	return ret;
}
