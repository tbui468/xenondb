#pragma once

#include "test.h"
#include "tx.h"
/*

void *wrtx_fcn(void *arg) {
    struct xndb *db = (struct xndb*)arg;
    void *result = malloc(sizeof(bool));
    *((bool*)result) = false;

    struct xntx *tx;
    if (!xntx_create(&tx, db, XNTXMODE_WR))
        return result;

    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
    struct xnpg page;
    if (!xnpgr_allocate_page(&meta_page, tx, &page))
        return result;

    if (!xntx_commit(tx))
        return result;

    *((bool*)result) = true;
    return result;
}

void wrtx_concurrent_wrtx_blocks() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));

    pthread_t writer1;
    pthread_t writer2;

    pthread_create(&writer1, NULL, wrtx_fcn, db);
    pthread_create(&writer2, NULL, wrtx_fcn, db);

    {
        void *status;
        pthread_join(writer1, &status);
        bool ok = *((bool*)status);
        free(status);
        assert(ok);
    }
    {
        void *status;
        pthread_join(writer2, &status);
        bool ok = *((bool*)status);
        free(status);
        assert(ok);
    }

    assert(xndb_free(db));
}


void wrtx_tests() {
    append_test(wrtx_concurrent_wrtx_blocks);
}*/
