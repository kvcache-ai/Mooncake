/* SPDX-License-Identifier: MIT */

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#include "liburing.h"
#include "helpers.h"

#define MAX_TEST_LBAS		1024

static const char *filename;
struct opcode {
	int op;
	bool test;
	bool not_supported;
};

#define TEST_BLOCK_URING_CMD_MAX		3

static struct opcode opcodes[TEST_BLOCK_URING_CMD_MAX] = {
	{ .op = BLOCK_URING_CMD_DISCARD, .test = true, },
	{ .test = false, },
	{ .test = false, },
};

static int lba_size;
static uint64_t bdev_size;
static uint64_t bdev_size_lbas;
static char *buffer;

static void prep_blk_cmd(struct io_uring_sqe *sqe, int fd,
			 uint64_t from, uint64_t len,
			 int cmd_op)
{
	assert(cmd_op == BLOCK_URING_CMD_DISCARD);

	io_uring_prep_cmd_discard(sqe, fd, from, len);
}

static int queue_cmd_range(struct io_uring *ring, int bdev_fd,
			   uint64_t from, uint64_t len, int cmd_op)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int err;

	sqe = io_uring_get_sqe(ring);
	assert(sqe != NULL);
	prep_blk_cmd(sqe, bdev_fd, from, len, cmd_op);

	err = io_uring_submit_and_wait(ring, 1);
	if (err != 1) {
		fprintf(stderr, "io_uring_submit_and_wait failed %d\n", err);
		exit(1);
	}

	err = io_uring_wait_cqe(ring, &cqe);
	if (err) {
		fprintf(stderr, "io_uring_wait_cqe failed %d (op %i)\n",
				err, cmd_op);
		exit(1);
	}

	err = cqe->res;
	io_uring_cqe_seen(ring, cqe);
	return err;
}

static int queue_cmd_lba(struct io_uring *ring, int bdev_fd,
			 uint64_t from, uint64_t nr_lba, int cmd_op)
{
	return queue_cmd_range(ring, bdev_fd, from * lba_size,
				nr_lba * lba_size, cmd_op);
}

static int queue_discard_lba(struct io_uring *ring, int bdev_fd,
			     uint64_t from, uint64_t nr_lba)
{
	return queue_cmd_lba(ring, bdev_fd, from, nr_lba,
				BLOCK_URING_CMD_DISCARD);
}

static int test_parallel(struct io_uring *ring, int fd, int cmd_op)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int inflight = 0;
	int max_inflight = 16;
	int left = 1000;
	int ret;

	while (left || inflight) {
		int queued = 0;
		unsigned head, nr_cqes = 0;
		int lba_len = 8;

		while (inflight < max_inflight && left) {
			int off = rand() % (MAX_TEST_LBAS - lba_len);
			sqe = io_uring_get_sqe(ring);
			assert(sqe != NULL);

			prep_blk_cmd(sqe, fd, off * lba_size,
				     lba_len * lba_size, cmd_op);
			if (rand() & 1)
				sqe->flags |= IOSQE_ASYNC;

			queued++;
			left--;
			inflight++;
		}
		if (queued) {
			ret = io_uring_submit(ring);
			if (ret != queued) {
				fprintf(stderr, "io_uring_submit failed %d\n", ret);
				return T_EXIT_FAIL;
			}
		}

		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "io_uring_wait_cqe failed %d\n", ret);
			exit(1);
		}

		io_uring_for_each_cqe(ring, head, cqe) {
			nr_cqes++;
			inflight--;
			if (cqe->res != 0) {
				fprintf(stderr, "cmd %i failed %i\n", cmd_op,
						cqe->res);
				return T_EXIT_FAIL;
			}
		}
		io_uring_cq_advance(ring, nr_cqes);
	}

	return 0;
}

static int cmd_issue_verify(struct io_uring *ring, int fd, int lba, int len,
			    int cmd_op)
{
	int verify = (cmd_op != BLOCK_URING_CMD_DISCARD);
	int ret, i;
	ssize_t res;

	if (verify) {
		for (i = 0; i < len; i++) {
			size_t off = (i + lba) * lba_size;

			res = pwrite(fd, buffer, lba_size, off);
			if (res == -1) {
				fprintf(stderr, "pwrite failed\n");
				return T_EXIT_FAIL;
			}
		}
	}

	ret = queue_cmd_lba(ring, fd, lba, len, cmd_op);
	if (ret) {
		if (ret == -EINVAL || ret == -EOPNOTSUPP)
			return T_EXIT_SKIP;

		fprintf(stderr, "cmd_issue_verify %i fail lba %i len %i  ret %i\n",
				cmd_op, lba, len, ret);
		return T_EXIT_FAIL;
	}

	if (verify) {
		for (i = 0; i < len; i++) {
			size_t off = (i + lba) * lba_size;

			res = pread(fd, buffer, lba_size, off);
			if (res == -1) {
				fprintf(stderr, "pread failed\n");
				return T_EXIT_FAIL;
			}
			if (!memchr(buffer, 0, lba_size)) {
				fprintf(stderr, "mem cmp failed, lba %i\n", lba + i);
				return T_EXIT_FAIL;
			}
		}
	}
	return 0;
}

static int basic_cmd_test(struct io_uring *ring, int op)
{
	int cmd_op = opcodes[op].op;
	int ret, fd;

	if (!opcodes[op].test)
		return T_EXIT_SKIP;

	fd = open(filename, O_DIRECT | O_RDWR | O_EXCL);
	if (fd < 0) {
		if (errno == EINVAL || errno == EBUSY)
			return T_EXIT_SKIP;
		fprintf(stderr, "open failed %i\n", errno);
		return T_EXIT_FAIL;
	}

	ret = cmd_issue_verify(ring, fd, 0, 1, cmd_op);
	if (ret == T_EXIT_SKIP) {
		printf("cmd %i not supported, skip\n", cmd_op);
		opcodes[op].not_supported = 1;
		close(fd);
		return T_EXIT_SKIP;
	} else if (ret) {
		fprintf(stderr, "cmd %i fail 0 1\n", cmd_op);
		return T_EXIT_FAIL;
	}

	ret = cmd_issue_verify(ring, fd, 7, 15, cmd_op);
	if (ret) {
		fprintf(stderr, "cmd %i fail 7 15 %i\n", cmd_op, ret);
		return T_EXIT_FAIL;
	}

	ret = cmd_issue_verify(ring, fd, 1, MAX_TEST_LBAS - 1, cmd_op);
	if (ret) {
		fprintf(stderr, "large cmd %i failed %i\n", cmd_op, ret);
		return T_EXIT_FAIL;
	}

	ret = test_parallel(ring, fd, cmd_op);
	if (ret) {
		fprintf(stderr, "test_parallel() %i failed %i\n", cmd_op, ret);
		return T_EXIT_FAIL;
	}

	close(fd);
	return 0;
}

static int test_fail_edge_cases(struct io_uring *ring, int op)
{
	int cmd_op = opcodes[op].op;
	int ret, fd;

	if (!opcodes[op].test)
		return T_EXIT_SKIP;

	fd = open(filename, O_DIRECT | O_RDWR | O_EXCL);
	if (fd < 0) {
		if (errno == EINVAL || errno == EBUSY)
			return T_EXIT_SKIP;
		fprintf(stderr, "open failed %i\n", errno);
		return T_EXIT_FAIL;
	}

	ret = queue_cmd_lba(ring, fd, bdev_size_lbas, 1, cmd_op);
	if (ret >= 0) {
		fprintf(stderr, "cmd %i beyond capacity %i\n",
				cmd_op, ret);
		return 1;
	}

	ret = queue_cmd_lba(ring, fd, bdev_size_lbas - 1, 2, cmd_op);
	if (ret >= 0) {
		fprintf(stderr, "cmd %i beyond capacity with overlap %i\n",
				cmd_op, ret);
		return 1;
	}

	ret = queue_cmd_range(ring, fd, (uint64_t)-lba_size, lba_size + 2,
			      cmd_op);
	if (ret >= 0) {
		fprintf(stderr, "cmd %i range overflow %i\n",
				cmd_op, ret);
		return 1;
	}

	ret = queue_cmd_range(ring, fd, lba_size / 2, lba_size, cmd_op);
	if (ret >= 0) {
		fprintf(stderr, "cmd %i unaligned offset %i\n",
				cmd_op, ret);
		return 1;
	}

	ret = queue_cmd_range(ring, fd, 0, lba_size / 2, cmd_op);
	if (ret >= 0) {
		fprintf(stderr, "cmd %i unaligned size %i\n",
				cmd_op, ret);
		return 1;
	}

	close(fd);
	return 0;
}

static int test_rdonly(struct io_uring *ring, int op)
{
	int ret, fd;
	int ro;

	if (!opcodes[op].test)
		return T_EXIT_SKIP;

	fd = open(filename, O_DIRECT | O_RDONLY | O_EXCL);
	if (fd < 0) {
		if (errno == EINVAL || errno == EBUSY)
			return T_EXIT_SKIP;
		fprintf(stderr, "open failed %i\n", errno);
		return T_EXIT_FAIL;
	}

	ret = queue_discard_lba(ring, fd, 0, 1);
	if (ret >= 0) {
		fprintf(stderr, "discarded with O_RDONLY %i\n", ret);
		return 1;
	}
	close(fd);

	fd = open(filename, O_DIRECT | O_RDWR | O_EXCL);
	if (fd < 0) {
		if (errno == EINVAL || errno == EBUSY)
			return T_EXIT_SKIP;
		fprintf(stderr, "open failed %i\n", errno);
		return T_EXIT_FAIL;
	}

	ro = 1;
	ret = ioctl(fd, BLKROSET, &ro);
	if (ret) {
		fprintf(stderr, "BLKROSET 1 failed %i\n", errno);
		return T_EXIT_FAIL;
	}

	ret = queue_discard_lba(ring, fd, 0, 1);
	if (ret >= 0) {
		fprintf(stderr, "discarded with O_RDONLY %i\n", ret);
		return 1;
	}

	ro = 0;
	ret = ioctl(fd, BLKROSET, &ro);
	if (ret) {
		fprintf(stderr, "BLKROSET 0 failed %i\n", errno);
		return T_EXIT_FAIL;
	}
	close(fd);
	return 0;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	int fd, ret, i, fret;
	int cmd_op;

	if (argc != 2)
		return T_EXIT_SKIP;
	filename = argv[1];

	fd = open(filename, O_DIRECT | O_RDONLY | O_EXCL);
	if (fd < 0) {
		if (errno == EINVAL || errno == EBUSY)
			return T_EXIT_SKIP;
		fprintf(stderr, "open failed %i\n", errno);
		return T_EXIT_FAIL;
	}

	ret = ioctl(fd, BLKGETSIZE64, &bdev_size);
	if (ret < 0) {
		fprintf(stderr, "BLKGETSIZE64 failed %i\n", errno);
		return T_EXIT_FAIL;
	}
	ret = ioctl(fd, BLKSSZGET, &lba_size);
	if (ret < 0) {
		fprintf(stderr, "BLKSSZGET failed %i\n", errno);
		return T_EXIT_FAIL;
	}
	assert(bdev_size % lba_size == 0);
	bdev_size_lbas = bdev_size / lba_size;
	close(fd);

	buffer = t_aligned_alloc(lba_size, lba_size);
	if (!buffer) {
		fprintf(stderr, "t_aligned_alloc failed\n");
		return T_EXIT_FAIL;
	}
	for (i = 0; i < lba_size; i++)
		buffer[i] = i ^ 0xA7;

	if (bdev_size_lbas < MAX_TEST_LBAS) {
		fprintf(stderr, "the device is too small, skip\n");
		return T_EXIT_SKIP;
	}

	ret = io_uring_queue_init(16, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue init failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	fret = T_EXIT_SKIP;
	for (cmd_op = 0; cmd_op < TEST_BLOCK_URING_CMD_MAX; cmd_op++) {
		if (!opcodes[cmd_op].test)
			continue;
		ret = basic_cmd_test(&ring, cmd_op);
		if (ret == T_EXIT_FAIL) {
			fprintf(stderr, "basic_cmd_test() failed, cmd %i\n",
					cmd_op);
			return T_EXIT_FAIL;
		}

		ret = test_rdonly(&ring, cmd_op);
		if (ret == T_EXIT_FAIL) {
			fprintf(stderr, "test_rdonly() failed, cmd %i\n",
					cmd_op);
			return T_EXIT_FAIL;
		}

		ret = test_fail_edge_cases(&ring, cmd_op);
		if (ret == T_EXIT_FAIL) {
			fprintf(stderr, "test_fail_edge_cases() failed, cmd %i\n",
					cmd_op);
			return T_EXIT_FAIL;
		}
		if (ret == T_EXIT_SKIP)
			fret = T_EXIT_SKIP;
		else
			fret = T_EXIT_PASS;
	}

	io_uring_queue_exit(&ring);
	free(buffer);
	return fret;
}
