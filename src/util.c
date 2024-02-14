#include "util.h"

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
        fprintf(stderr, "xenondb: open failed.");
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

