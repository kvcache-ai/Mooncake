/* SPDX-License-Identifier: MIT */
/*
 * 6.10-rc merge window had a bug where the rewritten mmap support caused
 * rings allocated with > 1 page, but asking for smaller mappings, would
 * cause -EFAULT to be returned rather than a successful map. This hit
 * applications either using an ancient liburing with IORING_FEAT_SINGLE_MMAP
 * support, or application just ignoring that feature flag and still doing
 * 3 mmap operations to map the ring.
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "../src/syscall.h"
#include "liburing.h"
#include "helpers.h"

#define ENTRIES	128

int main(int argc, char *argv[])
{
	struct io_uring_params p = { };
	void *ptr;
	int fd;

	if (argc > 1)
		return T_EXIT_SKIP;

	fd = __sys_io_uring_setup(ENTRIES, &p);
	if (fd < 0)
		return T_EXIT_SKIP;

	if (!(p.features & IORING_FEAT_SINGLE_MMAP)) {
		close(fd);
		return T_EXIT_SKIP;
	}

	ptr = __sys_mmap(0, ENTRIES * sizeof(unsigned), PROT_READ | PROT_WRITE,
				MAP_SHARED | MAP_POPULATE, fd,
				IORING_OFF_SQ_RING);
	if (!IS_ERR(ptr)) {
		close(fd);
		return T_EXIT_PASS;
	}

	fprintf(stderr, "ring sqe array mmap: %d\n", PTR_ERR(ptr));
	return T_EXIT_FAIL;
}
