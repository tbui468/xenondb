#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>

#define xnresult_t __attribute__((warn_unused_result)) bool

/*
bool xn_malloc(size_t size, void **ptr) {
    *ptr = malloc(size);
    return *ptr != NULL;
}

bool xn_aligned_malloc(size_t size, void **ptr) {
    struct stat fstat;
    xn_ensure(stat("/", &fstat) == 0);
    size_t block_size = fstat.st_blksize;
    xn_ensure(size % block_size == 0);

    return posix_memalign(ptr, block_size, size) == 0;
}*/

//#define xn_malloc(s, p) ({ *p = malloc(s); _all_ptrs_[_all_ptr_count_++] = *p; *p != NULL; })
//#define xn_malloc(s, p) ({ *p = malloc(s); *p != NULL; })

//#define xn_aligned_malloc(s, p)

#define xnmm_init() int _all_ptr_count_ = 0; \
                     int _defer_ptr_count_ = 0; \
                     void *_all_ptrs_[16]; \
                     void *_defer_ptrs_[16];

#define xnmm_defer(p) _defer_ptrs_[_defer_ptr_count_] = p; \
                      _defer_ptr_count_++;

#define xnmm_cleanup() for (int _i_ = _defer_ptr_count_ - 1; _i_ >= 0; _i_--) { \
                        free(_defer_ptrs_[_i_]); \
                    }

#define xnmm_err_cleanup() for (int _i_ = _all_ptr_count_ - 1; _i_ >= 0; _i_--) { \
                        free(_all_ptrs_[_i_]); \
                    }


extern char _xnerr_buf[1024];

//#define xn_ok() ({ for (int _i_ = _all_ptr_count_ - 1; _i_ >= 0; _i_--) free(_all_ptrs_[_i_]); true; })
#define xn_ok() true

/*
#define xn_failed(msg) ({ strcat(_xnerr_buf, "'"); \
    strcat(_xnerr_buf, __func__); \
    strcat(_xnerr_buf, "' in "); \
    strcat(_xnerr_buf, __FILE__); \
    strcat(_xnerr_buf, " [line "); \
    char buf[10]; \
    sprintf(buf, "%d", __LINE__); \
    strcat(_xnerr_buf, buf); \
    strcat(_xnerr_buf, "] failed: "); \
    strcat(_xnerr_buf, msg); \
    false; })*/

#define xnerr_tostring() _xnerr_buf

#define xn_ensure(b) if (!(b)) { printf("%s\n", __func__); return false; }

bool xn_malloc(size_t size, void **ptr);
bool xn_aligned_malloc(size_t size, void **ptr);


xnresult_t xn_realpath(const char *path, char *out);
xnresult_t xn_stat(const char *path, struct stat *s);
xnresult_t xn_open(const char *path, int flags, mode_t mode, int *out_fd);
xnresult_t xn_mutex_lock(pthread_mutex_t *lock);
xnresult_t xn_mutex_unlock(pthread_mutex_t *lock);
xnresult_t xn_cond_signal(pthread_cond_t *cv);
xnresult_t xn_cond_wait(pthread_cond_t *cv, pthread_mutex_t *lock);
xnresult_t xn_mutex_init(pthread_mutex_t *lock);
xnresult_t xn_mutex_destroy(pthread_mutex_t *lock);
