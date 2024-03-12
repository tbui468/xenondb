#pragma once

#include "table.h"
#include "test.h"

void table_create_free() {
    //free emptytable with regular buffers
    {
        struct xntbl *tbl;
        assert(xntbl_create(&tbl));
        assert(xntbl_free(tbl, false));
    }

    //free empty table with mmapped pointers
    {
        struct xntbl *tbl;
        assert(xntbl_create(&tbl));
        assert(xntbl_free(tbl, true));
    }
}

void table_insert_buffer() {
    struct xntbl *tbl;
    assert(xntbl_create(&tbl));
    struct xnfile *handle;
    assert(xnfile_create("dummy", true, false, &handle));

    uint8_t *buf;
    assert(xn_malloc(XNPG_SZ, (void**)&buf));
    struct xnpg page = { .file_handle = handle, .idx = 0 };

    assert(xntbl_insert(tbl, &page, buf));
    assert(xntbl_free(tbl, false));

    assert(xnfile_close(handle));
}

void table_find_buffer() {
    struct xntbl *tbl;
    assert(xntbl_create(&tbl));
    struct xnfile *handle;
    assert(xnfile_create("dummy", true, false, &handle));

    //not found
    {
        struct xnpg page = { .file_handle = handle, .idx = 0 };
        assert(xntbl_find(tbl, &page) == NULL);
    }

    //insert, find and check data
    {
        uint8_t *buf;
        assert(xn_malloc(XNPG_SZ, (void**)&buf));
        const char *msg = "hello";
        strcpy(buf, msg);
        struct xnpg page = { .file_handle = handle, .idx = 0 };
        assert(xntbl_insert(tbl, &page, buf));
        uint8_t *value;
        assert((value = xntbl_find(tbl, &page)));
        assert(strcmp(value, msg) == 0);
    }

    assert(xntbl_free(tbl, false));

    assert(xnfile_close(handle));
}

void table_insert_mmap() {
    struct xntbl *tbl;
    assert(xntbl_create(&tbl));
    struct xnfile *handle;
    assert(xnfile_create("dummy", true, false, &handle));
    assert(xnfile_set_size(handle, XNPG_SZ));
    assert(handle->size == XNPG_SZ);

    //write data to test reading
    const char *msg = "hello";
    assert(xnfile_write(handle, msg, 0, strlen(msg) + 1));

    //map page
    struct xnpg page = { .file_handle = handle, .idx = 0 };
    uint8_t *ptr;
    assert(xnpg_mmap(&page, &ptr));

    //insert mapped page
    assert(xntbl_insert(tbl, &page, ptr));

    assert(xntbl_free(tbl, true));
    assert(xnfile_close(handle));
}

void table_find_mmap() {
    struct xntbl *tbl;
    assert(xntbl_create(&tbl));
    struct xnfile *handle;
    assert(xnfile_create("dummy", true, false, &handle));
    assert(xnfile_set_size(handle, XNPG_SZ));
    assert(handle->size == XNPG_SZ);

    //not found
    {
        struct xnpg page = { .file_handle = handle, .idx = 0 };
        assert(xntbl_find(tbl, &page) == NULL);
    }

    //write data to test reading
    const char *msg = "hello";
    assert(xnfile_write(handle, msg, 0, strlen(msg) + 1));

    //map page
    struct xnpg page = { .file_handle = handle, .idx = 0 };
    uint8_t *ptr;
    assert(xnpg_mmap(&page, &ptr));

    //insert, find and check data
    {
        struct xnpg page = { .file_handle = handle, .idx = 0 };
        assert(xntbl_insert(tbl, &page, ptr));
        uint8_t *value;
        assert((value = xntbl_find(tbl, &page)));
        assert(strcmp(value, msg) == 0);
    }

    assert(xntbl_free(tbl, true));
    assert(xnfile_close(handle));
}

void table_tests() {
    append_test(table_create_free);
    append_test(table_insert_buffer);
    append_test(table_find_buffer);
    append_test(table_insert_mmap);
    append_test(table_find_mmap);
}
