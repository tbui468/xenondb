#pragma once
#include "util.h"

struct xnfile {
    int fd;
    char *path;
    size_t size;
    size_t block_size;
};

xnresult_t xnfile_create(const char *name, bool create, bool direct, struct xnfile **handle);
xnresult_t xnfile_close(struct xnfile *handle);
xnresult_t xnfile_set_size(struct xnfile *handle, size_t size);
xnresult_t xnfile_sync(struct xnfile *handle);
xnresult_t xnfile_write(struct xnfile *handle, const char *buf, off_t off, size_t size);
xnresult_t xnfile_read(struct xnfile *handle, char *buf, off_t off, size_t size);
xnresult_t xnfile_mmap(struct xnfile *handle, off_t offset, size_t len, void **out_ptr);
xnresult_t xnfile_munmap(void *addr, size_t len);
