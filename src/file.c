#define _GNU_SOURCE

#include "file.h"
#include "page.h"

#include <libgen.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/mman.h>
#include <math.h>


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

xnresult_t xnfile_create(struct xnfile **out_handle, const char *relpath, bool create, bool direct) {
    xnmm_init();
    struct xnfile *handle;
    xn_ensure(xn_malloc((void**)&handle, sizeof(struct xnfile)));

    int flags = O_RDWR;
    if (create) {
        flags |= O_CREAT;
    }
    if (direct) {
        flags |= O_DIRECT;
        flags |= O_DSYNC;
    }

    //TODO if an error occurs after this line, need to free handle - how can we do this in a readable way?
    xn_ensure(xn_open(relpath, flags, S_IRUSR | S_IWUSR, &handle->fd));

    char buf[PATH_MAX];
    xn_ensure(xn_realpath(relpath, buf));
    xn_ensure(xn_malloc((void**)&handle->path, strlen(buf) + 1));
    strcpy(handle->path, buf);

    struct stat s;
    xn_ensure(xn_stat(handle->path, &s));
    handle->size = s.st_size;
    handle->block_size = s.st_blksize;

    //need to sync parent directory to ensure new file remains on disk in case of failure
    if (handle->size == 0) {
        xn_ensure(xnfile_sync_parent(buf));
    }

    *out_handle = handle;
    return xn_ok();
}

xnresult_t xnfile_close(void **handle) {
    xnmm_init();
    xn_ensure(close(((struct xnfile*)(*handle))->fd) == 0);
    free(*handle);
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
    xn_ensure(off + size <= handle->size);
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
    xn_ensure(off + size <= handle->size);
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

xnresult_t xnfile_mmap(struct xnfile *handle, off_t offset, size_t len, void **out_ptr) {
    xnmm_init();
    xn_ensure(offset % handle->block_size == 0);
    xn_ensure(offset + len <= handle->size);
    void *ptr;
    xn_ensure((ptr = mmap(NULL, len, MAP_SHARED, PROT_READ, handle->fd, offset)) != MAP_FAILED);
    *out_ptr = ptr;
    return xn_ok();
}

xnresult_t xnfile_munmap(void *addr, size_t len) {
    xnmm_init();
    xn_ensure((munmap(addr, len)) == 0);
    return xn_ok();
}

xnresult_t xnfile_grow(struct xnfile *handle) {
    xnmm_init();
    size_t new_size = ceil((handle->size * 1.2f) / XNPG_SZ) * XNPG_SZ;
    xn_ensure(xnfile_set_size(handle, new_size));
    xn_ensure(xnfile_sync(handle));
    return xn_ok();
}
