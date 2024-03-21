#pragma once

#include "test.h"
#include "container.h"
/*

void container_create_free() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    struct xntx *tx;
    assert(xntx_create(&tx, db, XNTXMODE_WR));
    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
    struct xnpg page;
    assert(xnpgr_allocate_page(&meta_page, tx, &page));

    struct xnctn *ctn;
    assert(xnctn_create(&ctn, tx, page)); 
    assert(xnctn_free((void**)&ctn));

    assert(xntx_commit(tx));
    assert(xndb_free(db));
}

void container_init() {
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

    assert(xnctn_free((void**)&ctn));

    assert(xntx_commit(tx));
    assert(xndb_free(db));
}

void container_insert_get() {
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

    {
        size_t size;
        assert(xnctn_get_size(ctn, id1, &size));
        assert(size == 10);
        char *out_buf = malloc(size);
        assert(xnctn_get(ctn, id1, out_buf, size));
        assert(memcmp("xxxxxxxxxx", out_buf, size) == 0);
        free(out_buf);
    }
    {
        size_t size;
        assert(xnctn_get_size(ctn, id1, &size));
        assert(size == 10);
        char *out_buf = malloc(size);
        assert(xnctn_get(ctn, id3, out_buf, size));
        assert(memcmp("zzzzzzzzzz", out_buf, size) == 0);
        free(out_buf);
    }
    {
        size_t size;
        assert(xnctn_get_size(ctn, id1, &size));
        assert(size == 10);
        char *out_buf = malloc(size);
        assert(xnctn_get(ctn, id2, out_buf, size));
        assert(memcmp("yyyyyyyyyy", out_buf, size) == 0);
        free(out_buf);
    }

    assert(xntx_commit(tx));
    assert(xnctn_free((void**)&ctn));
    assert(xndb_free(db));
}

void container_full() {
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

    uint8_t *buf = malloc(1024);
    memset(buf, 'x', 1024);
    bool result;

    for (int i = 0; i < 3; i++) {
        assert(xnctn_can_fit(ctn, 1024, &result));
        assert(result);
        struct xnitemid id;
        assert(xnctn_insert(ctn, buf, 1024, &id));
    }

    assert(xnctn_can_fit(ctn, 1024, &result));
    assert(!result);


    assert(xntx_commit(tx));
    assert(xnctn_free((void**)&ctn));
    assert(xndb_free(db));
}


void container_delete() {
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

    char out_buf[10];
    assert(xnctn_get(ctn, id1, out_buf, 10) == false);
    assert(xnctn_get(ctn, id2, out_buf, 10));
    assert(xnctn_get(ctn, id3, out_buf, 10) == false);

    assert(xntx_commit(tx));
    assert(xnctn_free((void**)&ctn));
    assert(xndb_free(db));
}


void container_update() {
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

    struct xnitemid new_id1;
    struct xnitemid new_id2;
    struct xnitemid new_id3;
    assert(xnctn_update(ctn, id1, in_buf, 10, &new_id1)); //update with same-sized data
    char smaller_buf[9];
    memset(smaller_buf, 'a', 9);
    assert(xnctn_update(ctn, id2, smaller_buf, 9, &new_id2));
    char bigger_buf[11];
    memset(bigger_buf, 'b', 11);
    assert(xnctn_update(ctn, id3, bigger_buf, 11, &new_id3));

    assert(new_id1.pg_idx == id1.pg_idx && new_id1.arr_idx == id1.arr_idx);
    assert(!(new_id2.pg_idx == id2.pg_idx && new_id2.arr_idx == id2.arr_idx));
    assert(!(new_id3.pg_idx == id3.pg_idx && new_id3.arr_idx == id3.arr_idx));

    size_t size;
    assert(xnctn_get_size(ctn, new_id1, &size));
    assert(size == 10);
    assert(xnctn_get_size(ctn, new_id2, &size));
    assert(size == 9);
    assert(xnctn_get_size(ctn, new_id3, &size));
    assert(size == 11);

    char out_buf1[10];
    char out_buf2[9];
    char out_buf3[11];
    assert(xnctn_get(ctn, new_id1, out_buf1, 10));
    assert(memcmp(out_buf1, in_buf, 10) == 0);
    assert(xnctn_get(ctn, new_id2, out_buf2, 9));
    assert(memcmp(smaller_buf, out_buf2, 9) == 0);
    assert(xnctn_get(ctn, new_id3, out_buf3, 11));
    assert(memcmp(bigger_buf, out_buf3, 11) == 0);

    assert(xntx_commit(tx));
    assert(xnctn_free((void**)&ctn));
    assert(xndb_free(db));
}

void container_tests() {
    append_test(container_create_free);
    append_test(container_init);
    append_test(container_insert_get);
    append_test(container_full);
    append_test(container_delete);
    append_test(container_update);
}*/
