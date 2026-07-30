/* SPDX-License-Identifier: MIT */
/*
 * Description: test IOU_PBUF_RING_MMAP with a ring setup with a ring
 *		setup without mmap'ing sq/cq arrays
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>

#include "liburing.h"
#include "helpers.h"

static int bgid = 5;
static int bid = 89;

int main(int argc, char *argv[])
{
	struct io_uring_buf_ring *br;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	size_t ring_size;
	int ret, ring_mask, fds[2];
	struct io_uring_buf_reg reg = {
		.ring_entries = 1,
		.bgid = bgid,
		.flags = IOU_PBUF_RING_MMAP,
	};
	struct io_uring_params p = { };
	void *ring_mem;
	char buf[32];
	off_t off;

	if (argc > 1)
		return T_EXIT_SKIP;

	if (posix_memalign(&ring_mem, 16384, 16384))
		return T_EXIT_FAIL;

	memset(ring_mem, 0, 16384);

	p.flags = IORING_SETUP_NO_MMAP;
	ret = io_uring_queue_init_mem(1, &ring, &p, ring_mem, 16384);
	if (ret < 0) {
		if (ret == -EINVAL || ret == -ENOMEM) {
			free(ring_mem);
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "queue init failed %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (pipe(fds) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	ring_size = sizeof(struct io_uring_buf);
	ring_mask = io_uring_buf_ring_mask(1);

	ret = io_uring_register_buf_ring(&ring, &reg, 0);
	if (ret) {
		if (ret == -EINVAL) {
			free(ring_mem);
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "reg buf ring: %d\n", ret);
		return T_EXIT_FAIL;
	}

	off = IORING_OFF_PBUF_RING |
			(unsigned long long) bgid << IORING_OFF_PBUF_SHIFT;
	br = mmap(NULL, ring_size, PROT_READ | PROT_WRITE,
				MAP_SHARED | MAP_POPULATE, ring.ring_fd, off);
	if (br == MAP_FAILED) {
		if (errno == ENOMEM) {
			free(ring_mem);
			return T_EXIT_SKIP;
		}
		perror("mmap");
		return T_EXIT_FAIL;
	}

	io_uring_buf_ring_add(br, buf, sizeof(buf), bid, ring_mask, 0);
	io_uring_buf_ring_advance(br, 1);

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_read(sqe, fds[0], NULL, 0, 0);
	sqe->flags |= IOSQE_BUFFER_SELECT;
	sqe->buf_group = bgid;

	io_uring_submit(&ring);

	ret = write(fds[1], "Hello", 5);
	if (ret < 0) {
		perror("write");
		return T_EXIT_FAIL;
	} else if (ret != 5) {
		fprintf(stderr, "short write %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res < 0) {
		fprintf(stderr, "cqe res %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
		fprintf(stderr, "buffer not selected in cqe\n");
		return T_EXIT_FAIL;
	}
	if ((cqe->flags >> IORING_CQE_BUFFER_SHIFT) != bid) {
		fprintf(stderr, "wrong buffer id returned\n");
		return T_EXIT_FAIL;
	}

	io_uring_cqe_seen(&ring, cqe);

	io_uring_queue_exit(&ring);
	free(ring_mem);
	return T_EXIT_PASS;
}
