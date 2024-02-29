#pragma once
#include "common.h"

#include <limits.h>
#include <sys/mman.h>

struct xnfile {
    int fd;
    char *path;
    size_t size;
};

__attribute__((warn_unused_result)) bool xnfile_create(const char *name, bool direct, struct xnfile **handle);
__attribute__((warn_unused_result)) bool xnfile_close(struct xnfile *handle);
__attribute__((warn_unused_result)) bool xnfile_set_size(struct xnfile *handle, size_t size);
__attribute__((warn_unused_result)) bool xnfile_sync(struct xnfile *handle);
__attribute__((warn_unused_result)) bool xnfile_write(struct xnfile *handle, const char *buf, off_t off, size_t size);
__attribute__((warn_unused_result)) bool xnfile_read(struct xnfile *handle, char *buf, off_t off, size_t size);
__attribute__((warn_unused_result)) bool xnfile_mmap(void *addr, size_t len, int prot, int flags, int fd, off_t offset, void **out_ptr);
__attribute__((warn_unused_result)) bool xnfile_munmap(void *addr, size_t len);
