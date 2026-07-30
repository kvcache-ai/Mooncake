/* SPDX-License-Identifier: MIT */
/*
 * Description: test that having a command overwrite the provided ring
 *		buffer itself doesn't impact committing of the received
 *		length at completion time. Based on a test case posted
 *		here:
 *
 *		https://lore.kernel.org/io-uring/tencent_2DEFFED135071FB225305A65D1688B303A09@qq.com/
 *
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>

#include "liburing.h"
#include "helpers.h"

#define BGID		0
#define RENTRIES	2
#define RMASK		(RENTRIES - 1)

int main(int argc, char *argv[])
{
	struct io_uring_buf_ring *br;
	struct io_uring_sqe *sqe;
	struct io_uring ring;
	void *send_buf;
	int ret, fds[2];

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(1, &ring, IORING_SETUP_NO_SQARRAY);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring setup: %d\n", ret);
		return T_EXIT_FAIL;
	}

	br = io_uring_setup_buf_ring(&ring, RENTRIES, BGID, IOU_PBUF_RING_INC, &ret);
	if (!br) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "buf ring setup: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/*
	 * Use the buffer ring itself as the provided buffer address. Once the
	 * recv completes, it will have zero filled the buffer addr/len.
	 */
	io_uring_buf_ring_add(br, br, 4096, 0, RMASK, 0);
	io_uring_buf_ring_advance(br, 1);

	if (socketpair(AF_UNIX, SOCK_DGRAM, 0, fds) < 0) {
		perror("socketpair");
		return T_EXIT_FAIL;
	}

	/*
	 * Send zeroes, overwriting the buffer ring
	 */
	send_buf = calloc(1, 32);
	ret = send(fds[0], send_buf, 32, MSG_DONTWAIT);
	if (ret < 0) {
		perror("send");
		return T_EXIT_FAIL;
	}

	/*
	 * Do recv, picking the first buffer. When buffer is picked,
	 * it's still fully valid. By the time it needs to get committed,
	 * it will have invalid addr/len fields (all zeroes from the recv)
	 */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_recv(sqe, fds[1], NULL, 0, 0);
	sqe->flags |= IOSQE_BUFFER_SELECT;
	sqe->buf_index = BGID;
	io_uring_submit_and_wait(&ring, 1);

	io_uring_free_buf_ring(&ring, br, RENTRIES, BGID);
	io_uring_queue_exit(&ring);
	free(send_buf);
	return T_EXIT_PASS;
}
