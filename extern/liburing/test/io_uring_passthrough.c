/* SPDX-License-Identifier: MIT */
/*
 * Description: basic read/write tests for io_uring passthrough commands
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include "helpers.h"
#include "liburing.h"
#include "../src/syscall.h"
#include "nvme.h"

#define min(a, b)	((a) < (b) ? (a) : (b))

#define FILE_SIZE	(256 * 1024)
#define BS		8192
#define BUFFERS		(FILE_SIZE / BS)

static void *meta_mem;
static struct iovec *vecs, *backing_vec;
static int no_pt;
static bool vec_fixed_supported = true;

/*
 * Each offset in the file has the ((test_case / 2) * FILE_SIZE)
 * + (offset / sizeof(int)) stored for every
 * sizeof(int) address.
 */
static int verify_buf(int tc, void *buf, off_t off)
{
	int i, u_in_buf = BS / sizeof(unsigned int);
	unsigned int *ptr;

	off /= sizeof(unsigned int);
	off += (tc / 2) * FILE_SIZE;
	ptr = buf;
	for (i = 0; i < u_in_buf; i++) {
		if (off != *ptr) {
			fprintf(stderr, "Found %u, wanted %llu\n", *ptr,
					(unsigned long long) off);
			return 1;
		}
		ptr++;
		off++;
	}

	return 0;
}

static int fill_pattern(int tc)
{
	unsigned int val, *ptr;
	int i, j;
	int u_in_buf = BS / sizeof(val);

	val = (tc / 2) * FILE_SIZE;
	for (i = 0; i < BUFFERS; i++) {
		ptr = vecs[i].iov_base;
		for (j = 0; j < u_in_buf; j++) {
			*ptr = val;
			val++;
			ptr++;
		}
	}

	return 0;
}

static int __test_io(const char *file, struct io_uring *ring, int tc, int read,
		     int sqthread, int fixed, int nonvec, int async, int linked)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct nvme_uring_cmd *cmd;
	int open_flags;
	int do_fixed;
	int i, ret, fd = -1, use_fd = -1, submit_count = 0;
	off_t offset;
	__u64 slba;
	__u32 nlb;

	if (read)
		open_flags = O_RDONLY;
	else
		open_flags = O_WRONLY;

	if (fixed) {
		ret = t_register_buffers(ring, backing_vec, 1);
		if (ret == T_SETUP_SKIP)
			return 0;
		if (ret != T_SETUP_OK) {
			fprintf(stderr, "buffer reg failed: %d\n", ret);
			goto err;
		}
	}

	fd = open(file, open_flags);
	if (fd < 0) {
		if (errno == EACCES || errno == EPERM)
			return T_EXIT_SKIP;
		perror("file open");
		goto err;
	}

	if (sqthread) {
		ret = io_uring_register_files(ring, &fd, 1);
		if (ret) {
			fprintf(stderr, "file reg failed: %d\n", ret);
			goto err;
		}
	}

	if (!read)
		fill_pattern(tc);

	offset = 0;
	for (i = 0; i < BUFFERS; i++) {
		unsigned int iovcnt = 1;
		size_t total_len;

		if (linked) {
			sqe = io_uring_get_sqe(ring);
			io_uring_prep_nop(sqe);
			if (async)
				sqe->flags |= IOSQE_ASYNC;
			sqe->flags |= IOSQE_IO_LINK;
			sqe->user_data = 0x1000;
			submit_count++;
		}

		sqe = io_uring_get_sqe(ring);
		if (!sqe) {
			fprintf(stderr, "sqe get failed\n");
			goto err;
		}
		use_fd = fd;
		do_fixed = fixed;

		if (sqthread)
			use_fd = 0;
		if (nonvec)
			io_uring_prep_uring_cmd(sqe, NVME_URING_CMD_IO, use_fd);
		else
			io_uring_prep_uring_cmd(sqe, NVME_URING_CMD_IO_VEC, use_fd);
		if (fixed && (i & 1))
			do_fixed = 0;
		if (do_fixed)
			sqe->buf_index = 0;
		if (async)
			sqe->flags |= IOSQE_ASYNC;
		if (do_fixed)
			sqe->uring_cmd_flags |= IORING_URING_CMD_FIXED;
		sqe->user_data = ((uint64_t)offset << 32) | i;
		if (sqthread)
			sqe->flags |= IOSQE_FIXED_FILE;

		cmd = (struct nvme_uring_cmd *)sqe->cmd;
		memset(cmd, 0, sizeof(struct nvme_uring_cmd));

		cmd->opcode = read ? nvme_cmd_read : nvme_cmd_write;

		if (!nonvec) {
			iovcnt = (submit_count % 3 == 0) ? 1 : ((submit_count % 3 == 1) ? 3 : 9);
			iovcnt = min(iovcnt, BUFFERS - i);
		}
		total_len = BS * iovcnt;

		slba = offset >> lba_shift;
		nlb = (total_len >> lba_shift) - 1;

		/* cdw10 and cdw11 represent starting lba */
		cmd->cdw10 = slba & 0xffffffff;
		cmd->cdw11 = slba >> 32;
		/* cdw12 represent number of lba's for read/write */
		cmd->cdw12 = nlb;
		if (nonvec) {
			cmd->addr = (__u64)(uintptr_t)vecs[i].iov_base;
			cmd->data_len = vecs[i].iov_len;
		} else {
			cmd->addr = (__u64)(uintptr_t)&vecs[i];
			cmd->data_len = iovcnt;
		}

		if (meta_size) {
			cmd->metadata = (__u64)(uintptr_t)(meta_mem +
						meta_size * i * (nlb + 1));
			cmd->metadata_len = meta_size * (nlb + 1);
		}
		cmd->nsid = nsid;

		offset += total_len;
		if (!nonvec)
			i += iovcnt - 1;
		submit_count++;
	}

	ret = io_uring_submit(ring);
	if (ret != submit_count) {
		fprintf(stderr, "submit got %d, wanted %d\n", ret, BUFFERS);
		goto err;
	}

	for (i = 0; i < submit_count; i++) {
		int is_link;

		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe=%d\n", ret);
			goto err;
		}
		if (cqe->res != 0) {
			if (cqe->res == -EINVAL && fixed && !nonvec) {
				vec_fixed_supported = false;
				goto cleanup_and_skip;
			}
			if (!no_pt) {
				no_pt = 1;
				goto skip;
			}
			fprintf(stderr, "cqe res %d, wanted 0\n", cqe->res);
			goto err;
		}
		is_link = cqe->user_data == 0x1000;
		io_uring_cqe_seen(ring, cqe);
		if (is_link)
			continue;
		if (read) {
			int index = cqe->user_data & 0xffffffff;
			void *buf = vecs[index].iov_base;
			off_t voff = cqe->user_data >> 32;

			if (verify_buf(tc, buf, voff))
				goto err;
		}
	}

cleanup_and_skip:
	if (fixed) {
		ret = io_uring_unregister_buffers(ring);
		if (ret) {
			fprintf(stderr, "buffer unreg failed: %d\n", ret);
			goto err;
		}
	}
	if (sqthread) {
		ret = io_uring_unregister_files(ring);
		if (ret) {
			fprintf(stderr, "file unreg failed: %d\n", ret);
			goto err;
		}
	}

skip:
	close(fd);
	return 0;
err:
	if (fd != -1)
		close(fd);
	return 1;
}

static int test_io(const char *file, int tc, int read, int sqthread,
		   int fixed, int nonvec, int hybrid, int async, int linked)
{
	struct io_uring ring;
	int ret, ring_flags = 0;

	ring_flags |= IORING_SETUP_SQE128;
	ring_flags |= IORING_SETUP_CQE32;

	if (sqthread)
		ring_flags |= IORING_SETUP_SQPOLL;

	if (hybrid)
		ring_flags |= IORING_SETUP_IOPOLL | IORING_SETUP_HYBRID_IOPOLL;

	if (fixed && (!vec_fixed_supported && !nonvec))
		return 0;

	ret = t_create_ring(128, &ring, ring_flags);
	if (ret == T_SETUP_SKIP)
		return 0;
	if (ret != T_SETUP_OK) {
		if (ret == -EINVAL) {
			no_pt = 1;
			return T_SETUP_SKIP;
		}
		fprintf(stderr, "ring create failed: %d\n", ret);
		return 1;
	}

	ret = __test_io(file, &ring, tc, read, sqthread, fixed, nonvec, async, linked);
	io_uring_queue_exit(&ring);

	return ret;
}

/*
 * Send a passthrough command that nvme will fail during submission.
 * This comes handy for testing error handling.
 */
static int test_invalid_passthru_submit(const char *file)
{
	struct io_uring ring;
	int fd, ret, ring_flags, open_flags;
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct nvme_uring_cmd *cmd;

	ring_flags = IORING_SETUP_CQE32 | IORING_SETUP_SQE128;

	ret = t_create_ring(1, &ring, ring_flags);
	if (ret != T_SETUP_OK) {
		fprintf(stderr, "ring create failed: %d\n", ret);
		return 1;
	}

	open_flags = O_RDONLY;
	fd = open(file, open_flags);
	if (fd < 0) {
		perror("file open");
		goto err;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_uring_cmd(sqe, NVME_URING_CMD_IO, fd);
	sqe->user_data = 1;
	cmd = (struct nvme_uring_cmd *)sqe->cmd;
	memset(cmd, 0, sizeof(struct nvme_uring_cmd));
	cmd->opcode = nvme_cmd_read;
	cmd->addr = (__u64)(uintptr_t)&vecs[0].iov_base;
	cmd->data_len = vecs[0].iov_len;
	/* populate wrong nsid to force failure */
	cmd->nsid = nsid + 1;

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submit got %d, wanted %d\n", ret, 1);
		goto err;
	}
	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe=%d\n", ret);
		goto err;
	}
	if (cqe->res == 0) {
		fprintf(stderr, "cqe res %d, wanted failure\n", cqe->res);
		goto err;
	}
	io_uring_cqe_seen(&ring, cqe);
	close(fd);
	io_uring_queue_exit(&ring);
	return 0;
err:
	if (fd != -1)
		close(fd);
	io_uring_queue_exit(&ring);
	return 1;
}

/*
 * if we are polling io_uring_submit needs to always enter the
 * kernel to fetch events
 */
static int test_io_uring_submit_enters(const char *file)
{
	struct io_uring ring;
	int fd, i, ret, ring_flags, open_flags;
	unsigned head;
	struct io_uring_cqe *cqe;
	struct nvme_uring_cmd *cmd;
	struct io_uring_sqe *sqe;

	ring_flags = IORING_SETUP_IOPOLL;
	ring_flags |= IORING_SETUP_SQE128;
	ring_flags |= IORING_SETUP_CQE32;

	ret = io_uring_queue_init(64, &ring, ring_flags);
	if (ret) {
		fprintf(stderr, "ring create failed: %d\n", ret);
		return 1;
	}

	open_flags = O_WRONLY;
	fd = open(file, open_flags);
	if (fd < 0) {
		perror("file open");
		goto err;
	}

	for (i = 0; i < BUFFERS; i++) {
		off_t offset = BS * (rand() % BUFFERS);
		__u64 slba;
		__u32 nlb;

		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_uring_cmd(sqe, NVME_URING_CMD_IO, fd);
		sqe->user_data = i;
		cmd = (struct nvme_uring_cmd *)sqe->cmd;
		memset(cmd, 0, sizeof(struct nvme_uring_cmd));

		slba = offset >> lba_shift;
		nlb = (BS >> lba_shift) - 1;

		cmd->opcode = nvme_cmd_read;
		cmd->cdw10 = slba & 0xffffffff;
		cmd->cdw11 = slba >> 32;
		cmd->cdw12 = nlb;
		cmd->addr = (__u64)(uintptr_t)&vecs[i];
		cmd->data_len = 1;
		cmd->nsid = nsid;

		if (meta_size) {
			cmd->metadata = (__u64)(uintptr_t)(meta_mem +
						meta_size * i * (nlb + 1));
			cmd->metadata_len = meta_size * (nlb + 1);
		}
	}

	/* submit manually to avoid adding IORING_ENTER_GETEVENTS */
	ret = __sys_io_uring_enter(ring.ring_fd, __io_uring_flush_sq(&ring), 0,
						0, NULL);
	if (ret < 0)
		goto err;

	for (i = 0; i < 500; i++) {
		ret = io_uring_submit(&ring);
		if (ret != 0) {
			fprintf(stderr, "still had %d sqes to submit\n", ret);
			goto err;
		}

		io_uring_for_each_cqe(&ring, head, cqe) {
			if (cqe->res == -EOPNOTSUPP)
				fprintf(stdout, "Device doesn't support polled IO\n");
			goto ok;
		}
		usleep(10000);
	}
err:
	ret = 1;
	if (fd != -1)
		close(fd);

ok:
	io_uring_queue_exit(&ring);
	return ret;
}

int main(int argc, char *argv[])
{
	int i, ret;
	char *fname;

	if (argc < 2)
		return T_EXIT_SKIP;

	fname = argv[1];
	ret = nvme_get_info(fname);

	if (ret)
		return T_EXIT_SKIP;

	vecs = t_malloc(BUFFERS * sizeof(struct iovec));
	backing_vec = t_create_buffers(1, BUFFERS * BS);
	/* Slice single large backing_vec into multiple smaller vecs */
	for (int i = 0; i < BUFFERS; i++) {
		vecs[i].iov_base = backing_vec[0].iov_base + i * BS;
		vecs[i].iov_len = BS;
	}
	if (meta_size)
		t_posix_memalign(&meta_mem, 0x1000,
				 meta_size * BUFFERS * (BS >> lba_shift));

	for (i = 0; i < 64; i++) {
		int read = (i & 1) != 0;
		int sqthread = (i & 2) != 0;
		int fixed = (i & 4) != 0;
		int nonvec = (i & 8) != 0;
		int hybrid = (i & 16) != 0;
		int async = (i & 32) != 0;
		int linked = (i & 64) != 0;

		ret = test_io(fname, i, read, sqthread, fixed, nonvec, hybrid, async, linked);
		if (no_pt)
			break;
		if (ret) {
			fprintf(stderr, "test_io failed %d/%d/%d/%d/%d\n",
				read, sqthread, fixed, nonvec, hybrid);
			goto err;
		}
	}

	if (no_pt)
		return T_EXIT_SKIP;

	ret = test_io_uring_submit_enters(fname);
	if (ret) {
		fprintf(stderr, "test_io_uring_submit_enters failed\n");
		goto err;
	}

	ret = test_invalid_passthru_submit(fname);
	if (ret) {
		fprintf(stderr, "test_invalid_passthru_submit failed\n");
		goto err;
	}

	return T_EXIT_PASS;
err:
	return T_EXIT_FAIL;
}
