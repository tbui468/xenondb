#define _GNU_SOURCE

#include "file.h"

#include <assert.h>
#include <libgen.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>

static xnresult_t xnfile_sync_parent(const char* child_path) {
    xnmm_init();
    char buf[PATH_MAX];
    strcpy(buf, child_path);
    char *parent_dir = dirname(buf);
    int parent_fd;
    xn_ensure((parent_fd = open(parent_dir, O_RDONLY)) != -1);
    xn_ensure(fsync(parent_fd) == 0);
    xn_ensure(close(parent_fd) == 0);
    return xn_ok();
}

xnresult_t xnfile_create(const char *relpath, bool direct, struct xnfile **out_handle) {
    xnmm_init();
    struct xnfile *handle;
    xn_ensure(xn_malloc(sizeof(struct xnfile), (void**)&handle));

    //TODO if an error occurs after this line, need to free handle - how can we do this in a readable way?

    char buf[PATH_MAX];
    xn_ensure(realpath(relpath, buf) != NULL);
    xn_ensure(xn_malloc(strlen(buf) + 1, (void**)&handle->path));
    strcpy(handle->path, buf);

    int flags = O_CREAT | O_RDWR;
    if (direct) {
        flags |= O_DIRECT;
        flags |= O_DSYNC;
    }

    xn_ensure((handle->fd = open(handle->path, flags, S_IRUSR | S_IWUSR)) != -1);

    struct stat s;
    xn_ensure(stat(handle->path, &s) == 0);
    handle->size = s.st_size;

    //need to sync parent directory to ensure new file remains on disk in case of failure
    if (handle->size == 0) {
        xn_ensure(xnfile_sync_parent(buf));
    }

    *out_handle = handle;
    return xn_ok();
}

xnresult_t xnfile_close(struct xnfile *handle) {
    xnmm_init();
    xn_ensure(close(handle->fd) == 0);
    free(handle);
    return xn_ok();
}

xnresult_t xnfile_set_size(struct xnfile *handle, size_t size) {
    xnmm_init();
    xn_ensure(ftruncate(handle->fd, size) == 0);
    xn_ensure(xnfile_sync_parent(handle->path));
    handle->size = size;
    return xn_ok();
}

xnresult_t xnfile_sync(struct xnfile *handle) {
    xnmm_init();
    xn_ensure(fdatasync(handle->fd) == 0);
    return xn_ok();
}

xnresult_t xnfile_write(struct xnfile *handle, const char *buf, off_t off, size_t size) {
    xnmm_init();
    assert(off + size <= handle->size);
    xn_ensure(lseek(handle->fd, off, SEEK_SET) != -1);

    size_t written = 0;

    while (written < size) {
        ssize_t res = write(handle->fd, buf + written, size - written);

        if (res == -1) {
            xn_ensure(errno == EINTR);
            continue;
        }

        written += res;
    }

    return xn_ok();
}

xnresult_t xnfile_read(struct xnfile *handle, char *buf, off_t off, size_t size) {
    xnmm_init();
    assert(off + size <= handle->size);
    xn_ensure(lseek(handle->fd, off, SEEK_SET) != -1);

    size_t red = 0;

    while (red < size) {
        ssize_t res = read(handle->fd, buf + red, size - red);

        if (res == -1) {
            xn_ensure(errno == EINTR);
            continue;
        }

        red += res;
    }

    return xn_ok();
}

xnresult_t xnfile_mmap(void *addr, size_t len, int prot, int flags, int fd, off_t offset, void **out_ptr) {
    xnmm_init();
    void *ptr;
    xn_ensure((ptr = mmap(addr, len, prot, flags, fd, offset)) != MAP_FAILED);
    *out_ptr = ptr;
    return xn_ok();
}

xnresult_t xnfile_munmap(void *addr, size_t len) {
    xnmm_init();
    xn_ensure((munmap(addr, len)) == 0);
    return xn_ok();
}
