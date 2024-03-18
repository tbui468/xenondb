#pragma once

#include "container.h"
#include "test.h"

void ctnitr_create_free() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    struct xntx *tx;
    assert(xntx_create(&tx, db, XNTXMODE_WR));
    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
    struct xnpg page;
    assert(xnpgr_allocate_page(&meta_page, tx, &page));

    struct xnctn *ctn;
    assert(xnctn_create(&ctn, tx, page)); 

    assert(xnctn_init(ctn));

    struct xnctnitr *itr;
    assert(xnctnitr_create(&itr, ctn));
    assert(xnctnitr_free((void**)&itr));

    assert(xntx_commit(tx));
    assert(xnctn_free((void**)&ctn));
    assert(xndb_free(db));
}

void ctnitr_basic_iterate() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    struct xntx *tx;
    assert(xntx_create(&tx, db, XNTXMODE_WR));
    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
    struct xnpg page;
    assert(xnpgr_allocate_page(&meta_page, tx, &page));

    struct xnctn *ctn;
    assert(xnctn_create(&ctn, tx, page)); 

    assert(xnctn_init(ctn));


    struct xnitemid id1;
    struct xnitemid id2;
    struct xnitemid id3;
    char in_buf[10];
    {
        memset(in_buf, 'x', 10);
        assert(xnctn_insert(ctn, in_buf, 10, &id1));
    }
    {
        memset(in_buf, 'y', 10);
        assert(xnctn_insert(ctn, in_buf, 10, &id2));
    }
    {
        memset(in_buf, 'z', 10);
        assert(xnctn_insert(ctn, in_buf, 10, &id3));
    }

    struct xnctnitr *itr;
    assert(xnctnitr_create(&itr, ctn));
    bool valid;
    int count = 0;
    while (true) {
        assert(xnctnitr_next(itr, &valid));
        if (!valid)
            break;
        count++;
        struct xnitemid id;
        assert(xnctnitr_itemid(itr, &id));
        size_t size;
        assert(xnctn_get_size(ctn, id1, &size));
        assert(size == 10);
        char *out_buf = malloc(size);
        assert(xnctn_get(ctn, id1, out_buf, size));
        free(out_buf);
    }

    assert(count == 3);

    assert(xnctnitr_free((void**)&itr));

    assert(xntx_commit(tx));
    assert(xnctn_free((void**)&ctn));
    assert(xndb_free(db));
}

void ctnitr_skips_deleted() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    struct xntx *tx;
    assert(xntx_create(&tx, db, XNTXMODE_WR));
    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
    struct xnpg page;
    assert(xnpgr_allocate_page(&meta_page, tx, &page));

    struct xnctn *ctn;
    assert(xnctn_create(&ctn, tx, page)); 

    assert(xnctn_init(ctn));


    struct xnitemid id1;
    struct xnitemid id2;
    struct xnitemid id3;
    char in_buf[10];
    {
        memset(in_buf, 'x', 10);
        assert(xnctn_insert(ctn, in_buf, 10, &id1));
    }
    {
        memset(in_buf, 'y', 10);
        assert(xnctn_insert(ctn, in_buf, 10, &id2));
    }
    {
        memset(in_buf, 'z', 10);
        assert(xnctn_insert(ctn, in_buf, 10, &id3));
    }

    assert(xnctn_delete(ctn, id1));
    assert(xnctn_delete(ctn, id3));

    struct xnctnitr *itr;
    assert(xnctnitr_create(&itr, ctn));
    bool valid;
    int count = 0;
    while (true) {
        assert(xnctnitr_next(itr, &valid));
        if (!valid)
            break;

        count++;
        struct xnitemid id;
        assert(xnctnitr_itemid(itr, &id));
        size_t size;
        assert(xnctn_get_size(ctn, id, &size));
        assert(size == 10);
        char *out_buf = malloc(size);
        assert(xnctn_get(ctn, id, out_buf, size));
        free(out_buf);
    }

    assert(count == 1);

    assert(xnctnitr_free((void**)&itr));

    assert(xntx_commit(tx));
    assert(xnctn_free((void**)&ctn));
    assert(xndb_free(db));
}

void containeritr_tests() {
    append_test(ctnitr_create_free);
    append_test(ctnitr_basic_iterate);
    append_test(ctnitr_skips_deleted);
}
