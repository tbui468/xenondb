#pragma once

#include <pthread.h>
#include <stdio.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <sys/types.h>

#define xnresult_t __attribute__((warn_unused_result)) bool

void xnmm_init();
void xnmm_defer(void *ptr);

void xnmm_cleanup_all();
void xnmm_cleanup_deferred();

#define xn_ok() ({ xnmm_cleanup_deferred(); true; })

#define xn_ensure(b) if (!(b)) { \
                         printf("%s\n", __func__); \
                         xnmm_cleanup_all(); \
                         return false; \
                     }

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
xnresult_t xn_atomic_increment(int *i, pthread_mutex_t *lock);
xnresult_t xn_atomic_decrement_and_signal(int *i, pthread_mutex_t *lock, pthread_cond_t *cv);
xnresult_t xn_atomic_decrement(int *i, pthread_mutex_t *lock);
