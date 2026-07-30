/* SPDX-License-Identifier: MIT */
/*
 * Description: test persistence of mmap'ed provided ring buffers. Use a range
 *		of buffer group IDs that puts us into both the lower end array
 *		and higher end xarry.
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

#define BGID_START	60
#define BGID_NR		10
#define ENTRIES		512

int main(int argc, char *argv[])
{
	struct io_uring_buf_ring *br[BGID_NR];
	struct io_uring ring;
	size_t ring_size;
	int ret, i, j;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(1, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue init failed %d\n", ret);
		return T_EXIT_FAIL;
	}

	ring_size = ENTRIES * sizeof(struct io_uring_buf);

	for (i = 0; i < BGID_NR; i++) {
		int bgid = BGID_START + i;
		struct io_uring_buf_reg reg = {
			.ring_entries = ENTRIES,
			.bgid = bgid,
			.flags = IOU_PBUF_RING_MMAP,
		};
		off_t off;

		ret = io_uring_register_buf_ring(&ring, &reg, 0);
		if (ret) {
			if (ret == -EINVAL)
				return T_EXIT_SKIP;
			fprintf(stderr, "reg buf ring: %d\n", ret);
			return T_EXIT_FAIL;
		}

		off = IORING_OFF_PBUF_RING |
			(unsigned long long) bgid << IORING_OFF_PBUF_SHIFT;
		br[i] = mmap(NULL, ring_size, PROT_READ | PROT_WRITE,
				MAP_SHARED | MAP_POPULATE, ring.ring_fd, off);
		if (br[i] == MAP_FAILED) {
			perror("mmap");
			return T_EXIT_FAIL;
		}
	}

	for (i = 0; i < BGID_NR; i++) {
		ret = io_uring_unregister_buf_ring(&ring, BGID_START + i);
		if (ret) {
			fprintf(stderr, "reg buf ring: %d\n", ret);
			return T_EXIT_FAIL;
		}
	}

	for (j = 0; j < 1000; j++) {
		for (i = 0; i < BGID_NR; i++)
			memset(br[i], 0x5a, ring_size);
		usleep(1000);
	}

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}
