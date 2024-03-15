#pragma once

#include "db.h"
#include "test.h"

void db_create_free() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    assert(xndb_free(db));
}

void db_recover() {
    //write data to database (allocating 3 pages)
    {
        struct xndb *db;
        assert(xndb_create("dummy", true, &db));
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_WR));
        struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
        struct xnpg page;
        for (int i = 0; i < 3; i++) {
            assert(xnpgr_allocate_page(&meta_page, tx, &page));
        }
        assert(xntx_commit(tx));
        assert(xndb_free(db));
    }

    uint8_t *data = malloc(XNPG_SZ);
    {
        //copy meta page into buffer to compare after recovery
        struct xnfile *handle;
        assert(xnfile_create(&handle, "dummy", false, false));
        assert(xnfile_read(handle, data, 0, XNPG_SZ));

        //scramble data in metadata page
        uint8_t *junk = malloc(XNPG_SZ);
        memset(junk, 'a', XNPG_SZ);
        assert(xnfile_write(handle, junk, 0, XNPG_SZ));
        assert(xnfile_close((void**)&handle));
        free(junk);
    }

    //open database again and let recovery run from the log
    {
        struct xndb *db;
        assert(xndb_create("dummy", false, &db));

        struct xnpg page = { .file_handle = db->file_handle, .idx = 0 };
        uint8_t *ptr;
        assert(xnpg_mmap(&page, &ptr));
        assert(memcmp(ptr, data, XNPG_SZ) == 0);
        assert(xnpg_munmap(ptr));
        assert(xndb_free(db));
    }

    free(data);
}

void db_tests() {
    append_test(db_create_free);
    append_test(db_recover);
}
