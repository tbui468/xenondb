#define _GNU_SOURCE
#include "common.h"
#include <string.h>
#include <sys/stat.h>
#include <stdlib.h>

char _xnerr_buf[1024];

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
}
