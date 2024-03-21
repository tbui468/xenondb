#define _GNU_SOURCE

#include "file.h"
#include "page.h"
#include "tx.h"

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
    xnmm_alloc(xn_free, xn_malloc, (void**)&handle, sizeof(struct xnfile));

    int flags = O_RDWR;
    if (create) {
        flags |= O_CREAT;
    }
    if (direct) {
        flags |= O_DIRECT;
        flags |= O_DSYNC;
    }

    //closing file immediately in case of failures in following function calls.
    //reopening fd at the end of this function.  Not the best way to deal with
    //closing a fd on failure, but it works for now
    xn_ensure(xn_open(relpath, flags, S_IRUSR | S_IWUSR, &handle->fd));
    xn_ensure(close(handle->fd) == 0);

    char buf[PATH_MAX];
    xn_ensure(xn_realpath(relpath, buf));
    xnmm_alloc(xn_free, xn_malloc, (void**)&handle->path, strlen(buf) + 1);
    strcpy(handle->path, buf);

    struct stat s;
    xn_ensure(xn_stat(handle->path, &s));
    handle->size = s.st_size;
    handle->block_size = s.st_blksize;

    //need to sync parent directory to ensure new file remains on disk in case of failure
    if (handle->size == 0) {
        xn_ensure(xnfile_sync_parent(buf));
    }

    xn_ensure(xn_open(relpath, flags, S_IRUSR | S_IWUSR, &handle->fd));

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

//grow file size by 20%, rounded up to the nearest page
xnresult_t xnfile_grow(struct xnfile *handle) {
    xnmm_init();
    size_t new_size = ceil((handle->size * 1.2f) / XNPG_SZ) * XNPG_SZ;
    xn_ensure(xnfile_set_size(handle, new_size));
    xn_ensure(xnfile_sync(handle));
    return xn_ok();
}

xnresult_t xnfile_init(struct xnfile *file, struct xntx *tx) {
    xnmm_init();

    //metadata page
    struct xnpg meta_page = { .file_handle = file, .idx = 0 };

    //initialize metadata page
    {
        //zero out page
        xnmm_scoped_alloc(scoped_ptr, xn_free, xn_malloc, &scoped_ptr, XNPG_SZ);
        uint8_t *buf = (uint8_t*)scoped_ptr;
        memset(buf, 0, XNPG_SZ);
        xn_ensure(xnpg_write(&meta_page, tx, buf, 0, XNPG_SZ, true));

        //set bit for metadata page to 'used'
        uint8_t page0_used = 1;
        xn_ensure(xnpg_write(&meta_page, tx, &page0_used, 0, sizeof(uint8_t), true));
    }

    return xn_ok();
}

static xnresult_t xnfile_find_free_page(struct xnfile *file, struct xntx *tx, struct xnpg *new_page) {
    xnmm_init();

    int byte_count = file->size / XNPG_SZ / 8;
    int i, j;
    struct xnpg meta_page = { .file_handle = file, .idx = 0 };
    for (i = 0; i < byte_count; i++) {
        uint8_t byte;
        xn_ensure(xnpg_read(&meta_page, tx, &byte, i * sizeof(uint8_t), sizeof(uint8_t)));
        for (j = 0; j < 8; j++) {
            uint8_t mask = 1 << j;
            uint8_t bit = (mask & byte) >> j;

            if (bit == 0)
                goto found_free_bit;
        }
    }

    //if no free page found, grow file and set i = byte_count to allocate fresh page 
    xn_ensure(xnfile_grow(file));
    i = byte_count;
    j = 0;

found_free_bit:
    new_page->file_handle = file;
    new_page->idx = i * 8 + j;
    return xn_ok();
}

static int xnpgr_bitmap_byte_offset(uint64_t page_idx) {
    return page_idx / 8;
}

xnresult_t xnfile_free_page(struct xnfile *file, struct xntx *tx, struct xnpg *page) {
    xnmm_init();

    //set bit to 'free' in metadata page
    struct xnpg meta_page = { .file_handle = file, .idx = 0 };
    uint8_t byte;
    xn_ensure(xnpg_read(&meta_page, tx, &byte, xnpgr_bitmap_byte_offset(page->idx), sizeof(uint8_t)));

    //ensure that page is actually allocated
    xn_ensure((byte & (1 << (page->idx % 8))) != 0);

    byte &= ~(1 << (page->idx % 8));
    xn_ensure(xnpg_write(&meta_page, tx, &byte, xnpgr_bitmap_byte_offset(page->idx), sizeof(uint8_t), true));
    return xn_ok();
}

xnresult_t xnfile_allocate_page(struct xnfile *file, struct xntx *tx, struct xnpg *page) {
    xnmm_init();

    xn_ensure(xnfile_find_free_page(file, tx, page));

    //zero out new page data
    xnmm_scoped_alloc(scoped_ptr, xn_free, xn_malloc, &scoped_ptr, XNPG_SZ);
    uint8_t *buf = (uint8_t*)scoped_ptr;

    memset(buf, 0, XNPG_SZ);
    xn_ensure(xnpg_write(page, tx, buf, 0, XNPG_SZ, true));

    //set bit to 'used' in metadata page
    struct xnpg meta_page = { .file_handle = file, .idx = 0 };
    uint8_t byte;
    xn_ensure(xnpg_read(&meta_page, tx, &byte, xnpgr_bitmap_byte_offset(page->idx), sizeof(uint8_t)));
    byte |= 1 << (page->idx % 8);
    xn_ensure(xnpg_write(&meta_page, tx, &byte, xnpgr_bitmap_byte_offset(page->idx), sizeof(uint8_t), true));

    return xn_ok();
}
