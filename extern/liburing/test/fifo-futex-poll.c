/* SPDX-License-Identifier: MIT */
/*
 * Description: Run a failing futex wait command followed up a POLL_ADD
 *		on a fifo.
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <linux/futex.h>
#include <linux/poll.h>
#include <unistd.h>

#include "liburing.h"
#include "helpers.h"

#ifndef FUTEX2_SIZE_U32
#define FUTEX2_SIZE_U32		0x02
#endif

int main(int argc, char *argv[])
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int ret, pipe;

	if (argc > 1)
		return T_EXIT_SKIP;

	/*
	 * Setup a FIFO for polling. FIFOs are interesting because they end
	 * up using two wait queue entries for poll, hitting double poll
	 * in io_uring.
	 */
	ret = mkfifo("fifo", 0644);
	if (ret < 0) {
		perror("mkfifo");
		return 1;
	}
	pipe = open("fifo", O_RDWR);
	if (pipe < 0) {
		perror("open fifo");
		ret = T_EXIT_FAIL;
		goto err;
	}

	ret = io_uring_queue_init(64, &ring, 0);
	if (ret) {
		ret = T_EXIT_FAIL;
		goto err;
	}

	/*
	 * Submit invalid futex wait, will error inline.
	 */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_futex_wait(sqe, NULL, 0, FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32, 0);
	sqe->user_data = 1;
	io_uring_submit(&ring);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait %d \n", ret);
		ret = T_EXIT_FAIL;
		goto err_ring;
	}
	if (cqe->res != -EFAULT) {
		if (cqe->res == -EINVAL) {
			ret = T_EXIT_SKIP;
			goto err_ring;
		}
		fprintf(stderr, "Unexpected futex return: %d\n", cqe->res);
		ret = T_EXIT_FAIL;
		goto err_ring;
	}
	io_uring_cqe_seen(&ring, cqe);

	ret = io_uring_register_files(&ring, &pipe, 1);
	if (ret < 0) {
		fprintf(stderr, "io_uring register files failed \n");
		ret = T_EXIT_FAIL;
		goto err_ring;
	}

	/*
	 * Submit poll request for POLLIN on the fifo
	 */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_poll_add(sqe, 0, POLLIN);
	sqe->flags |= IOSQE_FIXED_FILE;
	sqe->user_data = 2;
	io_uring_submit(&ring);

	/*
	 * Trigger the poll request
	 */
	ret = write(pipe, "FIFO test\n", 10);
	if (ret < 0)
		perror("fifo write");

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait %d \n", ret);
		ret = T_EXIT_FAIL;
	}
	io_uring_cqe_seen(&ring, cqe);
err_ring:
	io_uring_queue_exit(&ring);
err:
	if (pipe != -1)
		close(pipe);
	unlink("fifo");
	return ret;
}
