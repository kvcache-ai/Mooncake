#ifndef _MXA_OS_H_
#define _MXA_OS_H_

#include <errno.h>

#include <unistd.h>
#include <pthread.h>

#include "compiler.h"

MXA_BEGIN_C_DECLS

static inline int close_ret(int fd) {
    int err = errno;
    int ret = close(fd);
    errno = err;
    return ret;
}

static inline ssize_t read_all(int fd, void *buf, size_t len) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = read(fd, buf + total, len - total);
        if (n == -1) {
            return -1;
        }
        if (n == 0) {
            break;
        }
        total += n;
    }
    return total;
}

static inline ssize_t write_all(int fd, const void *buf, size_t len) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = write(fd, buf + total, len - total);
        if (n == -1) {
            return -1;
        }
        total += n;
    }
    return total;
}

static inline void mutex_init(pthread_mutex_t *mutex) {
    mxa_assert(pthread_mutex_init(mutex, NULL) == 0);
}

static inline void mutex_lock(pthread_mutex_t *mutex) {
    mxa_assert(pthread_mutex_lock(mutex) == 0);
}

static inline void mutex_unlock(pthread_mutex_t *mutex) {
    mxa_assert(pthread_mutex_unlock(mutex) == 0);
}

static inline void mutex_destroy(pthread_mutex_t *mutex) {
    mxa_assert(pthread_mutex_destroy(mutex) == 0);
}

MXA_END_C_DECLS

#endif
