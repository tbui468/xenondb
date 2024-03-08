#pragma once
#include "util.h"

struct xnfile {
    int fd;
    char *path;
    size_t size;
};

xnresult_t xnfile_create(const char *name, bool direct, struct xnfile **handle);
xnresult_t xnfile_close(struct xnfile *handle);
xnresult_t xnfile_set_size(struct xnfile *handle, size_t size);
xnresult_t xnfile_sync(struct xnfile *handle);
xnresult_t xnfile_write(struct xnfile *handle, const char *buf, off_t off, size_t size);
xnresult_t xnfile_read(struct xnfile *handle, char *buf, off_t off, size_t size);
//TODO these two functions can just be integrated into page.h and page.c
xnresult_t xnfile_mmap(void *addr, size_t len, int fd, off_t offset, void **out_ptr);
xnresult_t xnfile_munmap(void *addr, size_t len);
