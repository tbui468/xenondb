#ifndef XN_UTIL
#define XN_UTIL
#define _GNU_SOURCE

#include <sys/types.h>
#include <stdbool.h>
#include <stddef.h>
#include <pthread.h>

void *xn_malloc(size_t size);
void xn_write(int fd, const void* buf, size_t count);
int xn_open(const char* path, int flags, mode_t mode);
off_t xn_seek(int fd, off_t offset, int whence);
void xn_read(int fd, void *buf, size_t count);

struct xnilist {
    int count;
    int capacity;
    int* ints;
};

void xnilist_init(struct xnilist **il);
void xnilist_append(struct xnilist *il, int n); 
bool xnilist_contains(struct xnilist *il, int n);
void *xn_aligned_alloc(size_t size);

char *xn_strcpy(char *dst, const char *src);
char *xn_strcat(char* dst, const char *src);

void xn_rwlock_init(pthread_rwlock_t *lock);
void xn_rwlock_destroy(pthread_rwlock_t *lock);
void xn_rwlock_unlock(pthread_rwlock_t *lock);
void xn_rwlock_slock(pthread_rwlock_t *lock);
void xn_rwlock_xlock(pthread_rwlock_t *lock);

#endif
