#pragma once

#define _GNU_SOURCE

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
//#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#define xnresult_t __attribute__((warn_unused_result)) bool

/*
void xnmm_init();
void xnmm_defer(void *ptr);

void xnmm_cleanup_all();
void xnmm_cleanup_deferred();

bool xn_malloc(size_t size, void **ptr);
bool xn_aligned_malloc(size_t size, void **ptr);*/

#define xnmm_init() int _all_ptr_count_ = 0; \
    int _defer_ptr_count_ = 0; \
    void *_all_ptrs_[16]; \
    void *_defer_ptrs_[16] \

#define xnmm_defer(p) _defer_ptrs_[_defer_ptr_count_++] = p

#define xnmm_cleanup_all() for (int i = _all_ptr_count_ - 1; i >= 0; i--) \
        free(_all_ptrs_[i])

#define xnmm_cleanup_deferred() for (int i = _defer_ptr_count_ - 1; i >= 0; i--) \
        free(_defer_ptrs_[i]); 


#define xn_malloc(size, ptr) ({*ptr = malloc(size); \
    if (*ptr != NULL) _all_ptrs_[_all_ptr_count_++] = *ptr; \
    *ptr != NULL; })

#define xn_aligned_malloc(size, ptr) ({ if (posix_memalign(ptr, 4096, size) == 0) \
        if (*ptr != NULL) \
            _all_ptrs_[_all_ptr_count_++] = *ptr; \
        *ptr != NULL; })

#define xn_ok() ({ xnmm_cleanup_deferred(); true; })

#define xn_ensure(b) if (!(b)) { \
                         fprintf(stderr, "%s\n", __func__); \
                         xnmm_cleanup_all(); \
                         return false; \
                     }

xnresult_t xn_realpath(const char *path, char *out);
xnresult_t xn_stat(const char *path, struct stat *s);
xnresult_t xn_open(const char *path, int flags, mode_t mode, int *out_fd);
xnresult_t xn_mutex_lock(pthread_mutex_t *lock);
xnresult_t xn_mutex_unlock(pthread_mutex_t *lock);
xnresult_t xn_cond_signal(pthread_cond_t *cv);
xnresult_t xn_cond_wait(pthread_cond_t *cv, pthread_mutex_t *lock);
xnresult_t xn_mutex_init(pthread_mutex_t *lock);
xnresult_t xn_mutex_destroy(pthread_mutex_t *lock);
xnresult_t xn_atomic_increment(int *i, pthread_mutex_t *lock);
xnresult_t xn_atomic_decrement_and_signal(int *i, pthread_mutex_t *lock, pthread_cond_t *cv);
xnresult_t xn_atomic_decrement(int *i, pthread_mutex_t *lock);
xnresult_t xn_wait_until_zero(int *count, pthread_mutex_t *lock, pthread_cond_t *cv);

uint32_t xn_hash(const uint8_t *buf, int length);
