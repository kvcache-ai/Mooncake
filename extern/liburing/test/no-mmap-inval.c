/* SPDX-License-Identifier: MIT */
/*
 * Description: test that using SETUP_NO_MMAP with an invalid SQ ring
 *		address fails.
 *
 */
#include <stdlib.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>

#include "liburing.h"
#include "helpers.h"

int main(int argc, char *argv[])
{
	struct io_uring_params p = {
		.sq_entries	= 2,
		.cq_entries	= 4,
		.flags		= IORING_SETUP_NO_MMAP,
	};
	struct io_uring ring;
	void *addr;
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	t_posix_memalign(&addr, sysconf(_SC_PAGESIZE), 8192);
	p.cq_off.user_addr = (unsigned long long) (uintptr_t) addr;

	ret = io_uring_queue_init_params(2, &ring, &p);
	if (ret == -EINVAL || ret == -ENOENT) {
		/*  kernel doesn't support SETUP_NO_MMAP */
		free(addr);
		return T_EXIT_SKIP;
	} else if (ret && (ret != -EFAULT && ret != -ENOMEM)) {
		fprintf(stderr, "Got %d, wanted -EFAULT\n", ret);
		return T_EXIT_FAIL;
	}

	free(addr);
	return T_EXIT_PASS;
}
