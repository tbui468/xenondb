#define _GNU_SOURCE
#include "util.h"

#include <stdlib.h>
#include <fcntl.h>

bool xn_free(void **ptr) {
    free(*ptr);
    return true;
}

bool xn_malloc(size_t size, void **ptr) {
    *ptr = malloc(size);
    return *ptr != NULL;
}

bool xn_aligned_malloc(size_t size, void **ptr) {
    struct stat fstat;
    if (stat("/", &fstat) != 0)
        return false;

    size_t block_size = fstat.st_blksize;
    if (size % block_size != 0)
        return false;

    return posix_memalign(ptr, block_size, size) == 0;
}

xnresult_t xn_realpath(const char *path, char *out) {
    xnmm_init();
    xn_ensure(realpath(path, out) != NULL);
    return xn_ok();
}

xnresult_t xn_stat(const char *path, struct stat *s) {
    xnmm_init();
    xn_ensure(stat(path, s) == 0);
    return xn_ok();
}

xnresult_t xn_open(const char *path, int flags, mode_t mode, int *out_fd) {
    xnmm_init();
    xn_ensure((*out_fd = open(path, flags, mode)) != -1);
    return xn_ok();
}

xnresult_t xn_mutex_lock(pthread_mutex_t *lock) {
    xnmm_init();
    xn_ensure(pthread_mutex_lock(lock) == 0);
    return xn_ok();
}

xnresult_t xn_mutex_unlock(pthread_mutex_t *lock) {
    xnmm_init();
    xn_ensure(pthread_mutex_unlock(lock) == 0);
    return xn_ok();
}

xnresult_t xn_cond_signal(pthread_cond_t *cv) {
    xnmm_init();
    xn_ensure(pthread_cond_signal(cv) == 0);
    return xn_ok();
}

xnresult_t xn_cond_wait(pthread_cond_t *cv, pthread_mutex_t *lock) {
    xnmm_init();
    xn_ensure(pthread_cond_wait(cv, lock) == 0);
    return xn_ok();
}

xnresult_t xn_mutex_init(pthread_mutex_t *lock) {
    xnmm_init();
    xn_ensure(pthread_mutex_init(lock, NULL) == 0);
    return xn_ok();
}

xnresult_t xn_mutex_destroy(pthread_mutex_t *lock) {
    xnmm_init();
    xn_ensure(pthread_mutex_destroy(lock) == 0);
    return xn_ok();
}

xnresult_t xn_atomic_increment(int *i, pthread_mutex_t *lock) {
    xnmm_init();
    xn_ensure(xn_mutex_lock(lock));
    (*i)++;
    xn_ensure(xn_mutex_unlock(lock));
    return xn_ok();
}

xnresult_t xn_atomic_decrement_and_signal(int *i, pthread_mutex_t *lock, pthread_cond_t *cv) {
    xnmm_init();
    xn_ensure(xn_mutex_lock(lock));
    (*i)--;
    if (*i == 0)
        xn_ensure(xn_cond_signal(cv));
    xn_ensure(xn_mutex_unlock(lock));
    return xn_ok();
}

xnresult_t xn_atomic_decrement(int *i, pthread_mutex_t *lock) {
    xnmm_init();
    xn_ensure(xn_mutex_lock(lock));
    (*i)--;
    xn_ensure(xn_mutex_unlock(lock));
    return xn_ok();
}

xnresult_t xn_wait_until_zero(int *count, pthread_mutex_t *lock, pthread_cond_t *cv) {
    xnmm_init();
    xn_ensure(xn_mutex_lock(lock));
    while (*count > 0) {
        xn_ensure(xn_cond_wait(cv, lock));
    }
    xn_ensure(xn_mutex_unlock(lock));

    return xn_ok();
}

//hash function from 'Crafting Interpreters'
uint32_t xn_hash(const uint8_t *buf, int length) {
    uint32_t hash = 2166136261u;
    for (int i = 0; i < length; i++) {
        hash ^= buf[i];
        hash *= 16777619;
    }
    return hash;
}

