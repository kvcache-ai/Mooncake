/* SPDX-License-Identifier: MIT */
#ifndef LIBURING_PROXY_H
#define LIBURING_PROXY_H

#include <sys/time.h>

/*
 * Generic opcode agnostic encoding to sqe/cqe->user_data
 */
struct userdata {
	union {
		struct {
			uint16_t op_tid; /* 4 bits op, 12 bits tid */
			uint16_t bid;
			uint16_t fd;
		};
		uint64_t val;
	};
};

#define OP_SHIFT	(12)
#define TID_MASK	((1U << 12) - 1)

/*
 * Packs the information that we will need at completion time into the
 * sqe->user_data field, which is passed back in the completion in
 * cqe->user_data. Some apps would need more space than this, and in fact
 * I'd love to pack the requested IO size in here, and it's not uncommon to
 * see apps use this field as just a cookie to either index a data structure
 * at completion time, or even just put the pointer to the associated
 * structure into this field.
 */
static inline void __encode_userdata(struct io_uring_sqe *sqe, int tid, int op,
				     int bid, int fd)
{
	struct userdata ud = {
		.op_tid = (op << OP_SHIFT) | tid,
		.bid = bid,
		.fd = fd
	};

	io_uring_sqe_set_data64(sqe, ud.val);
}

static inline uint64_t __raw_encode(int tid, int op, int bid, int fd)
{
	struct userdata ud = {
		.op_tid = (op << OP_SHIFT) | tid,
		.bid = bid,
		.fd = fd
	};

	return ud.val;
}

static inline int cqe_to_op(struct io_uring_cqe *cqe)
{
	struct userdata ud = { .val = cqe->user_data };

	return ud.op_tid >> OP_SHIFT;
}

static inline int cqe_to_bid(struct io_uring_cqe *cqe)
{
	struct userdata ud = { .val = cqe->user_data };

	return ud.bid;
}

static inline int cqe_to_fd(struct io_uring_cqe *cqe)
{
	struct userdata ud = { .val = cqe->user_data };

	return ud.fd;
}

static unsigned long long mtime_since(const struct timeval *s,
				      const struct timeval *e)
{
	long long sec, usec;

	sec = e->tv_sec - s->tv_sec;
	usec = (e->tv_usec - s->tv_usec);
	if (sec > 0 && usec < 0) {
		sec--;
		usec += 1000000;
	}

	sec *= 1000;
	usec /= 1000;
	return sec + usec;
}

static unsigned long long mtime_since_now(struct timeval *tv)
{
	struct timeval end;

	gettimeofday(&end, NULL);
	return mtime_since(tv, &end);
}

#endif
