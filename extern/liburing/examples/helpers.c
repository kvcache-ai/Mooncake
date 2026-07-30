/* SPDX-License-Identifier: MIT */
#include <fcntl.h>
#include <stdint.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdarg.h>

#include "helpers.h"

#ifndef CONFIG_HAVE_MEMFD_CREATE
#include <sys/syscall.h>
int memfd_create(const char *name, unsigned int flags)
{
	return (int)syscall(SYS_memfd_create, name, flags);
}
#endif

int setup_listening_socket(int port, int ipv6)
{
	struct sockaddr_in srv_addr = { };
	struct sockaddr_in6 srv_addr6 = { };
	int fd, enable, ret, domain;

	if (ipv6)
		domain = AF_INET6;
	else
		domain = AF_INET;

	fd = socket(domain, SOCK_STREAM, 0);
	if (fd == -1) {
		perror("socket()");
		return -1;
	}

	enable = 1;
	ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));
	if (ret < 0) {
		perror("setsockopt(SO_REUSEADDR)");
		return -1;
	}

	if (ipv6) {
		srv_addr6.sin6_family = AF_INET6;
		srv_addr6.sin6_port = htons(port);
		srv_addr6.sin6_addr = in6addr_any;
		ret = bind(fd, (const struct sockaddr *)&srv_addr6, sizeof(srv_addr6));
	} else {
		srv_addr.sin_family = AF_INET;
		srv_addr.sin_port = htons(port);
		srv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
		ret = bind(fd, (const struct sockaddr *)&srv_addr, sizeof(srv_addr));
	}

	if (ret < 0) {
		perror("bind()");
		return -1;
	}

	if (listen(fd, 1024) < 0) {
		perror("listen()");
		return -1;
	}

	return fd;
}

void *t_aligned_alloc(size_t alignment, size_t size)
{
	void *ret;

	if (posix_memalign(&ret, alignment, size))
		return NULL;

	return ret;
}

void t_error(int status, int errnum, const char *format, ...)
{
	va_list args;
	va_start(args, format);

	vfprintf(stderr, format, args);
	if (errnum)
		fprintf(stderr, ": %s", strerror(errnum));

	fprintf(stderr, "\n");
	va_end(args);
	exit(status);
}
