/* SPDX-License-Identifier: MIT */
#ifndef LIBURING_SETUP_H
#define LIBURING_SETUP_H

int __io_uring_queue_init_params(unsigned entries, struct io_uring *ring,
				 struct io_uring_params *p, void *buf,
				 size_t buf_size);
void io_uring_unmap_rings(struct io_uring_sq *sq, struct io_uring_cq *cq);
int io_uring_mmap(int fd, struct io_uring_params *p, struct io_uring_sq *sq,
		  struct io_uring_cq *cq);
void io_uring_setup_ring_pointers(struct io_uring_params *p,
				  struct io_uring_sq *sq,
				  struct io_uring_cq *cq);

#endif
