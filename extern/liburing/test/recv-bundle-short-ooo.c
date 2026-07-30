/* SPDX-License-Identifier: MIT */
/*
 * Description: Run recv multishot with bundle support, and verify that
 *		data is always received in the correct order. A kernel
 *		commit sometimes broke this:
 *
 *		7c71a0af81ba ("io_uring/net: improve recv bundles")
 *
 *		Test case heavily based on the excellent reproducer posted
 *		by royonia in this bug report:
 *
 *		https://github.com/axboe/liburing/issues/1409
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "liburing.h"
#include "helpers.h"

static int no_buf_ring, no_recv_mshot;

/* Configuration constants */
#define ONE_MB      (1024 * 1024)	/* Size of test data (1MB) */
#define BUFFER_SIZE 1024		/* Size of each buffer in bytes */

#define BUFFER_COUNT 4		/* Number of buffers in the ring */
#define QUEUE_DEPTH 16		/* io_uring queue depth */

#define min(a, b) (((a) < (b)) ? (a) : (b))

/* Global state tracking */
static size_t data_received = 0;	/* Tracks total bytes received */

/**
 * Buffer data structure
 * Contains information about a buffer from the ring
 */
struct buf_data {
	void *addr;		/* Buffer memory address */
	uint16_t bid;		/* Buffer ID within the ring */
	uint32_t len;		/* Length of valid data in the buffer */
};

/**
 * Buffer ring data structure
 * Holds information needed to manage a buffer ring
 */
struct buf_ring_data {
	struct io_uring_buf_ring *buf_ring;	/* The io_uring buffer ring */
	void *buffer_memory;	/* Memory for all buffers */
	uint16_t ring_entries;	/* Number of entries in the ring */
	uint32_t buf_size;	/* Size of each buffer */
};

/**
 * Sets up and initializes the buffer ring for io_uring
 *
 * This function allocates memory for the buffer ring and all individual buffers,
 * initializes the buffer ring, registers it with io_uring, and adds all buffers
 * to the ring.
 *
 * @param ring      Pointer to the io_uring instance
 * @param entries   Number of buffer entries to create
 * @param buf_size  Size of each buffer in bytes
 * @param bgid      Buffer group ID to use
 *
 */
static int setup_buf_ring(struct buf_ring_data *data, struct io_uring *ring,
			  uint16_t entries, uint32_t buf_size, int bgid)
{
	data->ring_entries = entries;
	data->buf_size = buf_size;

	/* Allocate page-aligned memory for all buffers */
	size_t total_size = entries * buf_size;
	int page_size = sysconf(_SC_PAGESIZE);
	size_t aligned_size = (total_size + page_size - 1) & ~(page_size - 1);

	void *buffer_memory = mmap(NULL, aligned_size, PROT_READ | PROT_WRITE,
		 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	assert(buffer_memory != MAP_FAILED);

	/* Verify buffer memory is page-aligned as guaranteed by mmap */
	data->buffer_memory = buffer_memory;

	/* Allocate and setup buffer ring with page alignment */
	void *mapped;
	struct io_uring_buf_ring *buf_ring;
	int ring_size = entries * sizeof(struct io_uring_buf);

	/* Round up ring size to page boundary */
	ring_size = (ring_size + page_size - 1) & ~(page_size - 1);

	mapped = mmap(NULL, ring_size, PROT_READ | PROT_WRITE,
		      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	assert(mapped != MAP_FAILED);

	buf_ring = (struct io_uring_buf_ring *)mapped;

	/* Initialize the buffer ring structure */
	io_uring_buf_ring_init(buf_ring);
	data->buf_ring = buf_ring;

	/* Prepare registration parameters */
	struct io_uring_buf_reg reg = {
		.ring_addr = (unsigned long)buf_ring,
		.ring_entries = entries,
		.bgid = 0
	};

	/* Register the buffer ring with io_uring */
	int ret = io_uring_register_buf_ring(ring, &reg, 0);

	if (ret) {
		if (ret == -EINVAL) {
			no_buf_ring = 1;
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "Buffer ring setup: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* Add all individual buffers to the ring */
	for (int i = 0; i < entries; i++) {
		void *buf_addr = buffer_memory + i * buf_size;

		io_uring_buf_ring_add(buf_ring, buf_addr, buf_size, i,
				      io_uring_buf_ring_mask(entries), i);
	}

	/* Make all buffers available by advancing the tail pointer */
	io_uring_buf_ring_advance(buf_ring, entries);
	return T_EXIT_PASS;
}

static void dump_buf(const char *msg, uint8_t *buf, int len)
{
	int i;

	fprintf(stderr, "Buffer %s\n", msg);
	for (i = 0; i < len; i++) {
		fprintf(stderr, "%3x ", buf[i]);
		if (i && !(i & 15))
			fprintf(stderr, "\n");
	}
	fprintf(stderr, "\n");
}

/**
 * Verifies that received buffer data matches expected data
 *
 * This function compares each byte of received data against the expected data
 * and asserts if any mismatch is found. It also prints the comparison for debugging.
 *
 * @param buf                 Pointer to buffer containing received data
 * @param expected_data_start Pointer to start of expected data for comparison
 */
static int verify_received_buffer(struct buf_data *buf, uint8_t *expected_data_start)
{
	uint8_t *data = buf->addr;

	for (uint32_t i = 0; i < buf->len; i++) {
		if (data[i] != expected_data_start[i]) {
			fprintf(stderr, "Recv data ordering mismatch, offset %d\n", i);
			dump_buf("expected", expected_data_start, buf->len);
			dump_buf("received", data, buf->len);
			return 1;
		}
	}

	return 0;
}

/**
 * Processes a completed io_uring receive operation
 *
 * This function handles the completion queue entry by:
 * 1. Extracting the buffer ID and received data from the CQE
 * 2. Updating the global data received counter
 * 3. Verifying the data matches the expected pattern
 * 4. Recycling the buffer back to the buffer ring
 *
 * @param cqe             Completion queue entry to process
 * @param br_data         Buffer ring data structure
 * @param current_expect  Pointer to current position in expected data (updated by this function)
 */
static int process_completion(struct io_uring_cqe *cqe, struct buf_ring_data *br_data,
			      uint8_t **current_expect)
{
	usleep(1);

	if (cqe->res <= 0) {
		/* Handle error or EOF condition */
		if (cqe->res == 0) {
			fprintf(stderr, "EOF reached\n");
		} else if (cqe->res == -EINVAL) {
			/* no recv mshot support */
			no_recv_mshot = 1;
		} else if (cqe->res != -ENOBUFS) {
			fprintf(stderr, "CQE res %d\n", cqe->res);
			return 1;
		}
		return 0;
	}

	/* Extract buffer ID and data length from completion */
	uint16_t bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
	uint32_t total_len = cqe->res;

	uint32_t nr_packet = 0;
	while (total_len) {
		uint32_t this_len = min(BUFFER_SIZE, total_len);
		/* should never get a len large then bundled buffer size */
		assert(this_len <= BUFFER_SIZE);

		void *buffer_addr = (uint8_t*)br_data->buffer_memory + (bid * BUFFER_SIZE);
		/* Prepare buffer data structure for verification */
		struct buf_data buf = {
			.addr = buffer_addr,
			.bid = bid,
			.len = this_len
		};

		/* Update global counter of total bytes received */
		data_received += this_len;

		/* Verify received data against expected pattern */
		if (verify_received_buffer(&buf, *current_expect))
			return T_EXIT_FAIL;
		*current_expect += this_len;	/* Move expected pointer forward */

		/* Rearm the buffer */
		io_uring_buf_ring_add(br_data->buf_ring, buffer_addr,
				      BUFFER_SIZE, bid,
				      io_uring_buf_ring_mask(br_data->ring_entries),
				      nr_packet);
		nr_packet++;

		/* Calculate next buffer id */
		bid = (bid + 1) & (BUFFER_COUNT - 1);
		total_len -= this_len;
	}
	if (nr_packet)
		io_uring_buf_ring_advance(br_data->buf_ring, nr_packet);
	return 0;
}

/**
 * Writes all the data to the specified file descriptor
 *
 * This function ensures that all data is written, handling partial writes
 * by making repeated calls to write() until all bytes are sent.
 *
 * @param fd File descriptor to write to
 * @param data Pointer to the data buffer to write
 * @param size Number of bytes to write
 */
static void write_all(int fd, const void *data, size_t size)
{
	const uint8_t *buf = data;
	size_t bytes_sent = 0;

	/* Continue until all data is sent */
	while (bytes_sent < size) {
		ssize_t sent;

		sent = write(fd, buf + bytes_sent, size - bytes_sent);
		assert(sent > 0);	/* Ensure write succeeded */
		bytes_sent += sent;
	}
}

/**
 * Main test function for io_uring bundle receive mechanism
 *
 * This function demonstrates the complete flow of using io_uring's buffer ring
 * and multishot receive with IORING_RECVSEND_BUNDLE flag. It performs these steps:
 * 1. Set up an io_uring instance and buffer ring
 * 2. Create a TCP socket pair for testing
 * 3. Send 1MB of pattern data over the socket
 * 4. Receive and verify the data using io_uring operations
 *
 */
static int test_recv_multi_large_packet_isolate_ring(int queue_flags)
{
	/* Initialize io_uring with parameters */
	struct io_uring ring;
	struct io_uring_params params = { .flags = queue_flags, };
	int ret, eret;

	ret = t_create_ring_params(QUEUE_DEPTH, &ring, &params);
	if (ret == T_SETUP_SKIP)
		return T_EXIT_SKIP;
	else if (ret != T_SETUP_OK)
		return T_EXIT_FAIL;

	/* Set up the buffer ring for receiving data */
	struct buf_ring_data br_data;
	ret = setup_buf_ring(&br_data, &ring, BUFFER_COUNT, BUFFER_SIZE, 0);
	if (ret == T_EXIT_SKIP)
		return T_EXIT_SKIP;
	else if (ret != T_EXIT_PASS)
		return T_EXIT_FAIL;

	/* Create socket pair for local communication testing */
	int socket_fds[2];
	ret = t_create_socket_pair(socket_fds, true);
	assert(ret == 0);

	int receiver_fd = socket_fds[0];
	int sender_fd = socket_fds[1];

	/* Allocate and initialize test data with pattern */
	uint8_t *test_data = malloc(ONE_MB);
	for (int i = 0; i < ONE_MB; i++)
		test_data[i] = i % 256;	/* Create repeating pattern */

	/* Send test data through the socket */
	write_all(sender_fd, test_data, ONE_MB);

	/* Close sender side to signal EOF to receiver */
	close(sender_fd);

	/* Initialize pointer to track our position in expected data */
	uint8_t *current_expect = test_data;

	/* Submit initial multishot receive operations with buffer selection */
	for (int i = 0; i < BUFFER_COUNT; i++) {
		struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);

		io_uring_prep_recv_multishot(sqe, receiver_fd, NULL, 0, 0);
		sqe->flags |= IOSQE_BUFFER_SELECT;
		sqe->ioprio |= IORING_RECVSEND_BUNDLE;
		sqe->buf_group = 0;
	}
	io_uring_submit(&ring);

	/* Process completions from io_uring */
	struct io_uring_cqe *cqe;
	int poll_count = 0;

	/* Loop until we've received all data or exceed maximum iterations */
	while (data_received < ONE_MB && poll_count < 5000) {
		/* Wait for a completion event */
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe=%d\n", ret);
			eret = T_EXIT_FAIL;
			goto exit;
		}
		/* Process this completion */
		if (process_completion(cqe, &br_data, &current_expect)) {
			eret = T_EXIT_FAIL;
			goto exit;
		}
		if (no_recv_mshot) {
			eret = T_EXIT_SKIP;
			goto exit;
		}

		/* Check for EOF (no more data and no more expected) */
		if (!(cqe->flags & IORING_CQE_F_MORE) && !(cqe->res)) {
			io_uring_cq_advance(&ring, 1);
			break;	/* Exit loop on EOF */
		}

		/* Respawn recv request if needed (when this one is done but no EOF) */
		if (!(cqe->flags & IORING_CQE_F_MORE) && cqe->res) {
			/* Get a submission queue entry */
			struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);

			/* Set up another multishot receive with same parameters */
			io_uring_prep_recv_multishot(sqe, receiver_fd, NULL, 0, 0);
			sqe->flags |= IOSQE_BUFFER_SELECT;
			sqe->ioprio |= IORING_RECVSEND_BUNDLE;
			sqe->buf_group = 0;

			/* Submit the new request */
			io_uring_submit(&ring);
		}

		/* Mark completion as processed */
		io_uring_cq_advance(&ring, 1);
		poll_count++;
	}

	/* Verify we received all expected data */
	if (data_received != ONE_MB) {
		fprintf(stderr, "Received %u, wanted %u\n", (int) data_received, ONE_MB);
		return T_EXIT_FAIL;
	}

	eret = T_EXIT_PASS;
exit:
	/* Clean up all allocated resources */
	close(receiver_fd);	/* Close socket */
	io_uring_queue_exit(&ring);	/* Clean up io_uring */

	/* Free memory resources */
	munmap(br_data.buffer_memory, BUFFER_COUNT * BUFFER_SIZE);
	munmap(br_data.buf_ring, BUFFER_COUNT * sizeof(struct io_uring_buf));
	free(test_data);

	return eret;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_recv_multi_large_packet_isolate_ring(0);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test 0 failed\n");
		return ret;
	}
	if (no_buf_ring || no_recv_mshot)
		return T_EXIT_SKIP;

	ret = test_recv_multi_large_packet_isolate_ring(IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test defer failed\n");
		return ret;
	}

	ret = test_recv_multi_large_packet_isolate_ring(IORING_SETUP_SQPOLL);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test sqpoll failed\n");
		return ret;
	}

	ret = test_recv_multi_large_packet_isolate_ring(IORING_SETUP_COOP_TASKRUN);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test coop failed\n");
		return ret;
	}

	return T_EXIT_PASS;
}
