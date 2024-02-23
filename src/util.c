#include "util.h"

#include <sys/stat.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>

void *xn_malloc(size_t size) {
    void *p;
    if (!(p = malloc(size))) {
        fprintf(stderr, "xenondb: malloc failed\n");
        exit(1);
    }
    return p;
}


void *xn_realloc(void* ptr, size_t size) {
    void *p;
    if (!(p = realloc(ptr, size))) {
        fprintf(stderr, "xenondb: malloc failed\n");
        exit(1);
    }
    return p;
}

void xn_free(void *ptr) {
    free(ptr);
}

void xn_write(int fd, const void* buf, size_t count) {
    int total = 0;
    while (total < count) {
        int result = write(fd, buf + total, count - total);
        if (result < 0) {
            if (errno != EINTR) {
                fprintf(stderr, "xenondb: write failed\n");
                char *s = strerror(errno);
                fprintf(stderr, "%s\n", s);
                exit(1);
            }
        } 

        total += result;
    }
}

int xn_open(const char* path, int flags, mode_t mode) {
    int fd;
    if ((fd = open(path, flags, mode)) == -1) {
        fprintf(stderr, "xenondb: open failed: %s\n", strerror(errno));
        exit(1);
    }
    return fd;
}

off_t xn_seek(int fd, off_t offset, int whence) {
    int result;
    if ((result = lseek(fd, offset, whence)) == -1) {
        fprintf(stderr, "xenondb: lseek failed.");
        exit(1);
    }

    return result;
}

void xn_read(int fd, void *buf, size_t count) {
    int result;
    int total = 0;
    while (total < count) {
        int result = read(fd, buf + total, count - total);
        if (result < 0) {
            fprintf(stderr, "xenondb: write failed\n");
            exit(1);
        } 

        total += result;
    }
}


void xnilist_init(struct xnilist **il) {
    *il = xn_malloc(sizeof(struct xnilist));
    (*il)->ints = NULL;
    (*il)->count = 0;
    (*il)->capacity = 0;
}

void xnilist_append(struct xnilist *il, int n) {
    if (il->count == il->capacity) {
        il->capacity = il->capacity == 0 ? 8 : il->capacity * 2;
        il->ints = xn_realloc(il->ints, il->capacity * sizeof(int));
    }

    il->ints[il->count] = n;
    il->count++;
}

bool xnilist_contains(struct xnilist *il, int n) {
    for (int i = 0; i < il->count; i++) {
        if (*(il->ints + i) == n)
            return true;
    }
    return false;
}

void *xn_aligned_alloc(size_t size) {
    struct stat fstat;
    stat("/", &fstat);
    size_t block_size = fstat.st_blksize;
    
    void *ptr;
    if (!(ptr = aligned_alloc(block_size, size))) {
        fprintf(stderr, "xenondb: posix_memalign failed\n");
        exit(1);
    }
    return ptr;
}

char *xn_strcpy(char *dst, const char *src) {
    strcpy(dst, src);
}

char *xn_strcat(char* dst, const char *src) {
    strcat(dst, src);
}

void xn_rwlock_init(pthread_rwlock_t *lock) {
    if (pthread_rwlock_init(lock, NULL)) {
        fprintf(stderr, "xenondb: pthread_rwlock_init failed\n");
        exit(1);
    }
}

void xn_rwlock_destroy(pthread_rwlock_t *lock) {
    if (pthread_rwlock_destroy(lock)) {
        fprintf(stderr, "xenondb: pthread_rwlock_destroy failed\n");
        exit(1);
    }
}
void xn_rwlock_unlock(pthread_rwlock_t *lock) {
    if(pthread_rwlock_unlock(lock)) {
        fprintf(stderr, "xenondb: pthread_rwlock_unlock failed\n");
        exit(1);
    }
}

void xn_rwlock_slock(pthread_rwlock_t *lock) {
    if (pthread_rwlock_rdlock(lock)) {
        fprintf(stderr, "xenondb: pthread_rwlock_rdlock failed\n");
        exit(1);
    }
}
void xn_rwlock_xlock(pthread_rwlock_t *lock) {
    if (pthread_rwlock_wrlock(lock)) {
        fprintf(stderr, "xenondb: pthread_rwlock_wrlock failed\n");
        exit(1);
    }
}
