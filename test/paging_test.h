#pragma once

#include "page.h"
#include "test.h"

void paging_allocate_page() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    struct xntx *tx;
    assert(xntx_create(&tx, db, XNTXMODE_WR));
    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
    struct xnpg page;
    for (int i = 0; i < 2; i++) {
        assert(xnpgr_allocate_page(&meta_page, tx, &page));
    }
    assert(xntx_commit(tx));
    assert(xndb_free(db));

    //read file directly and assert that 3 pages allocated (metadata + 2 other pages)
    struct xnfile *handle;
    assert(xnfile_create(&handle, "dummy", false, false));
    uint8_t *buf = malloc(XNPG_SZ);
    assert(xnfile_read(handle, buf, 0, XNPG_SZ));
    assert(*buf == 7);
    assert(xnfile_close((void**)&handle));
    free(buf);
}

void paging_free_page() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    struct xnpg meta_page = {.file_handle = db->file_handle, .idx = 0 };
    struct xnpg page;
    //allocate 2 pages
    {
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_WR));
        for (int i = 0; i < 2; i++) {
            assert(xnpgr_allocate_page(&meta_page, tx, &page));
        }
        assert(xntx_commit(tx));
    }
    //free one page
    {
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_WR));
        assert(xnpgr_free_page(&meta_page, tx, page));
        assert(xntx_commit(tx));
    }
    assert(xndb_free(db));

    //read file directly and assert that 2 pages allocated (metadata + 1 other page)
    struct xnfile *handle;
    assert(xnfile_create(&handle, "dummy", false, false));
    uint8_t *buf = malloc(XNPG_SZ);
    assert(xnfile_read(handle, buf, 0, XNPG_SZ));
    assert(*buf == 3);
    assert(xnfile_close((void**)&handle));
    free(buf);
}

void paging_insufficient_file_size() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));

    struct xntx *tx;
    assert(xntx_create(&tx, db, XNTXMODE_WR));

    struct xnpg meta_page = {.file_handle = db->file_handle, .idx = 0 };
    struct xnpg page;
    size_t old_file_size = db->file_handle->size;
    for (int i = 0; i < old_file_size / XNPG_SZ; i++) {
        assert(xnpgr_allocate_page(&meta_page, tx, &page));
    }

    //file size should have grown by a percentage if page allocations surpass current size
    assert(db->file_handle->size > old_file_size);

    assert(xntx_commit(tx));
    assert(xndb_free(db));
}

void paging_free_invalid_page() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    struct xnpg meta_page = { .file_handle = db->file_handle, .idx = 0 };
    struct xnpg page = { .file_handle = db->file_handle, .idx = 1 };
    //attempt to free non-allocated page
    {
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_WR));
        assert(xnpgr_free_page(&meta_page, tx, page) == false);
        assert(xntx_rollback((void**)&tx));
    }
    assert(xndb_free(db));
}

void paging_tests() {
    append_test(paging_allocate_page);
    append_test(paging_free_page);
    append_test(paging_insufficient_file_size);
    append_test(paging_free_invalid_page);
}
