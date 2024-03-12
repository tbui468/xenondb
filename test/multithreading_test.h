#pragma once

#include "tx.h"
#include "test.h"

#include <stdlib.h>
#include <pthread.h>

struct data {
    struct xndb *db;
    int i;
};

void *fcn(void *arg) {
    struct data* d = (struct data*)arg;
    int i = d->i;
    struct xndb *db = d->db;
    free(arg);

    void *result = malloc(sizeof(bool));
    *((bool*)result) = false;

    if (i % 2 == 0) {
        struct xntx *tx;
        if (!xntx_create(db, XNTXMODE_WR, &tx))
            return result;

        struct xnpg page;

        struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
        if (!xnpgr_allocate_page(&meta_page, tx, &page))
            return result;
        if (!xntx_commit(tx))
            return result;
    } else {
        struct xntx *tx;
        if (!xntx_create(db, XNTXMODE_RD, &tx))
            return result;
        struct xnpg page = { .file_handle = tx->db->file_handle, .idx = 0 };

        uint8_t *buf = malloc(XNPG_SZ);
        if (!xnpg_read(&page, tx, buf, 0, XNPG_SZ))
            return result;
        free(buf);
        if (!xntx_rollback(tx)) //TODO should have to call free or close to close tx rather than commit/rollback
            return result;
    }

    *((bool*)result) = true;
    return result;
}

void multithreading_page_allocation() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));

    //15 writes and 15 read threads
    //write threads allocate a page
    const int THREAD_COUNT = 30;
    pthread_t threads[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; i++) {
        struct data *p = malloc(sizeof(struct data));
        p->db = db;
        p->i = i;
        pthread_create(&threads[i], NULL, fcn, p);
    }

    for (int i = 0; i < THREAD_COUNT; i++) {
        void *status;
        pthread_join(threads[i], &status);
        bool ok = *((bool*)status);
        free(status);
        assert(ok);
    }

    struct xnfile *handle;
    assert(xnfile_create("dummy", false, false, &handle));
    uint8_t buf;
    assert(xnfile_read(handle, &buf, 0, sizeof(uint8_t)));
    assert(buf == 255);
    assert(xnfile_read(handle, &buf, sizeof(uint8_t), sizeof(uint8_t)));
    assert(buf == 255);
    assert(xnfile_read(handle, &buf, sizeof(uint8_t) * 2, sizeof(uint8_t)));
    assert(buf == 0);
    assert(xnfile_close(handle));

    assert(xndb_free(db));
}


void multithreading_tests() {
    append_test(multithreading_page_allocation);
}
