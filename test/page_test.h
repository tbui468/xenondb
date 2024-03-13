#pragma once

#include "page.h"
#include "test.h"
#include <stdlib.h>

void page_copy() {
    struct xnfile *handle;
    assert(xnfile_create("dummy", true, false, &handle));
    assert(xnfile_set_size(handle, XNPG_SZ));
    assert(handle->size == XNPG_SZ);

    const char *msg = "hello";
    assert(xnfile_write(handle, msg, 0, strlen(msg) + 1));
    uint8_t *buf = malloc(XNPG_SZ);

    //basic copy
    {
        struct xnpg page = { .file_handle = handle, .idx = 0 };
        assert(xnpg_copy(&page, buf));
        assert(strcmp(buf, msg) == 0);
    }

    //copy past end of file
    {
        struct xnpg page = { .file_handle = handle, .idx = 1 };
        assert(xnpg_copy(&page, buf) == false);
    }
        

    free(buf);
    assert(xnfile_close(handle));
}

void page_flush() {
    struct xnfile *handle;
    assert(xnfile_create("dummy", true, false, &handle));
    assert(xnfile_set_size(handle, XNPG_SZ));
    assert(handle->size == XNPG_SZ);

    const char *msg = "hello";
    assert(xnfile_write(handle, msg, 0, strlen(msg) + 1));
    uint8_t *buf = malloc(XNPG_SZ);

    struct xnpg page = { .file_handle = handle, .idx = 0 };
    assert(xnpg_copy(&page, buf));
    assert(strcmp(buf, msg) == 0);

    const char *other_msg = "world";
    strcpy(buf, other_msg);

    //writing to buffer doesn't modify page yet
    char sbuf[8];
    assert(xnfile_read(handle, sbuf, 0, strlen(msg) + 1));
    assert(strcmp(sbuf, msg) == 0);

    //flushing page modifies data on disk
    assert(xnpg_flush(&page, buf));
    assert(xnfile_read(handle, sbuf, 0, strlen(other_msg) + 1));
    assert(strcmp(sbuf, other_msg) == 0);

    //attempt flush on invalid page past end of file
    struct xnpg other_page = { .file_handle = handle, .idx = 1 };
    assert(xnpg_flush(&other_page, buf) == false);

    free(buf);
    assert(xnfile_close(handle));
}

void page_mmap() {
    struct xnfile *handle;
    assert(xnfile_create("dummy", true, false, &handle));
    assert(xnfile_set_size(handle, XNPG_SZ));
    assert(handle->size == XNPG_SZ);

    const char *msg = "hello";
    assert(xnfile_write(handle, msg, 0, strlen(msg) + 1));

    //basic mmap 
    {
        struct xnpg page = { .file_handle = handle, .idx = 0 };
        uint8_t *ptr;
        assert(xnpg_mmap(&page, &ptr));
        assert(strcmp(ptr, msg) == 0);
        assert(xnpg_munmap(ptr));
    }

    //attempt to mmap past end of file
    {
        struct xnpg page = { .file_handle = handle, .idx = 1 };
        uint8_t *ptr;
        assert(xnpg_mmap(&page, &ptr) == false);
    }
}

void page_tests() {
    append_test(page_copy);
    append_test(page_flush);
    append_test(page_mmap);
}
