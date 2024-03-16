#pragma once

#include "test.h"
#include "db.h"
#include <unistd.h>

void tx_empty_commit_rollback() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));

    {
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_WR));
        assert(xntx_commit(tx));
    }

    {
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_WR));
        assert(xntx_rollback((void**)&tx));
    }

    assert(xndb_free(db));
}

void tx_commit() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    {
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_WR));
        struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
        struct xnpg page;
        for (int i = 0; i < 2; i++) {
            assert(xnpgr_allocate_page(&meta_page, tx, &page));
        }
        assert(xntx_commit(tx));
    }

    {
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_RD));
        struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
        uint8_t byte;
        assert(xnpg_read(&meta_page, tx, &byte, 0, sizeof(uint8_t)));
        assert(byte == 7);
        assert(xntx_close((void**)&tx));
    }

    assert(xndb_free(db));
}

void tx_rollback() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    {
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_WR));
        struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
        struct xnpg page;
        for (int i = 0; i < 2; i++) {
            assert(xnpgr_allocate_page(&meta_page, tx, &page));
        }
        assert(xntx_rollback((void**)&tx));
    }

    {
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_RD));
        struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
        uint8_t byte;
        assert(xnpg_read(&meta_page, tx, &byte, 0, sizeof(uint8_t)));
        assert(byte == 1);
        assert(xntx_close((void**)&tx));
    }
    assert(xndb_free(db));
}

void tx_read_own_writes() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    {
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_WR));
        struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
        struct xnpg page;
        for (int i = 0; i < 2; i++) {
            assert(xnpgr_allocate_page(&meta_page, tx, &page));
        }

        //read own uncommitted writes
        uint8_t byte;
        assert(xnpg_read(&meta_page, tx, &byte, 0, sizeof(uint8_t)));
        assert(byte == 7);

        assert(xntx_rollback((void**)&tx));
    }

    //write tx rollbacked, so data unchanged
    {
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_RD));
        struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
        uint8_t byte;
        assert(xnpg_read(&meta_page, tx, &byte, 0, sizeof(uint8_t)));
        assert(byte == 1);
        assert(xntx_close((void**)&tx));
    }
    assert(xndb_free(db));
}

void* reader1_fcn(void *arg) {
    struct xndb *db = (struct xndb*)arg;
    void *result = malloc(sizeof(bool));
    *((bool*)result) = false;

    struct xntx *tx;
    if (!xntx_create(&tx, db, XNTXMODE_RD))
        return result;

    usleep(400000);

    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
    uint8_t byte;
    if (!xnpg_read(&meta_page, tx, &byte, 0, sizeof(uint8_t)))
        return result;
    if (byte != 1)
        return result;
    if (!xntx_close((void**)&tx))
        return result;

    *((bool*)result) = true;
    return result;
}

void* reader2_fcn(void *arg) {
    struct xndb *db = (struct xndb*)arg;
    void *result = malloc(sizeof(bool));
    *((bool*)result) = false;

    usleep(200000);

    struct xntx *tx;
    if (!xntx_create(&tx, db, XNTXMODE_RD))
        return result;

    usleep(200000);

    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
    uint8_t byte;
    if (!xnpg_read(&meta_page, tx, &byte, 0, sizeof(uint8_t)))
        return result;
    if (byte != 1)
        return result;
    if (!xntx_close((void**)&tx))
        return result;

    *((bool*)result) = true;
    return result;
}

void* writer_fcn(void *arg) {
    struct xndb *db = (struct xndb*)arg;
    void *result = malloc(sizeof(bool));
    *((bool*)result) = false;

    usleep(100000);

    struct xntx *tx;
    if (!xntx_create(&tx, db, XNTXMODE_WR))
        return result;

    usleep(200000);

    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
    struct xnpg page;
    for (int i = 0; i < 2; i++) {
        if (!xnpgr_allocate_page(&meta_page, tx, &page))
            return result;
    }

    if (!xntx_commit(tx))
        return result;

    *((bool*)result) = true;
    return result;
}


void cc_txs_before_commit_snapshot() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));

    pthread_t reader1;
    pthread_t reader2;
    pthread_t writer;
    
    pthread_create(&reader1, NULL, reader1_fcn, db);
    pthread_create(&reader2, NULL, reader2_fcn, db);
    pthread_create(&writer, NULL, writer_fcn, db);


    void *status;
    pthread_join(reader1, &status);
    bool ok = *((bool*)status);
    free(status);
    assert(ok);

    pthread_join(reader2, &status);
    ok = *((bool*)status);
    free(status);
    assert(ok);

    pthread_join(writer, &status);
    ok = *((bool*)status);
    free(status);
    assert(ok);

    //make sure write was successful
    {
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_RD));
        struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
        uint8_t byte;
        assert(xnpg_read(&meta_page, tx, &byte, 0, sizeof(uint8_t)));
        assert(byte == 7);
        assert(xntx_close((void**)&tx));
    }

    assert(xndb_free(db));
}

void* after_commit_reader1_fcn(void *arg) {
    struct xndb *db = (struct xndb*)arg;
    void *result = malloc(sizeof(bool));
    *((bool*)result) = false;

    struct xntx *tx;
    if (!xntx_create(&tx, db, XNTXMODE_RD))
        return result;

    usleep(200000);

    //verify that old data is still read
    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
    uint8_t byte;
    if (!xnpg_read(&meta_page, tx, &byte, 0, sizeof(uint8_t)))
        return result;
    if (byte != 1)
        return result;

    usleep(100000);

    if (!xntx_close((void**)&tx))
        return result;

    *((bool*)result) = true;
    return result;
}

void* after_commit_reader2_fcn(void *arg) {
    struct xndb *db = (struct xndb*)arg;
    void *result = malloc(sizeof(bool));
    *((bool*)result) = false;

    usleep(200000);

    struct xntx *tx;
    if (!xntx_create(&tx, db, XNTXMODE_RD))
        return result;

    //verify that it reads new data
    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
    uint8_t byte;
    if (!xnpg_read(&meta_page, tx, &byte, 0, sizeof(uint8_t)))
        return result;
    if (byte != 7)
        return result;

    usleep(200000);

    //verify that it still reads new data
    //TODO this read fails
    if (!xnpg_read(&meta_page, tx, &byte, 0, sizeof(uint8_t)))
        return result;
    if (byte != 7)
        return result;

    if (!xntx_close((void**)&tx))
        return result;

    *((bool*)result) = true;
    return result;
}

void* after_commit_reader3_fcn(void *arg) {
    struct xndb *db = (struct xndb*)arg;
    void *result = malloc(sizeof(bool));
    *((bool*)result) = false;

    usleep(500000);

    struct xntx *tx;
    if (!xntx_create(&tx, db, XNTXMODE_RD))
        return result;

    //verify that it reads new data
    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
    uint8_t byte;
    if (!xnpg_read(&meta_page, tx, &byte, 0, sizeof(uint8_t)))
        return result;
    if (byte != 7)
        return result;

    if (!xntx_close((void**)&tx))
        return result;

    *((bool*)result) = true;
    return result;
}

void* after_commit_writer_fcn(void *arg) {
    struct xndb *db = (struct xndb*)arg;
    void *result = malloc(sizeof(bool));
    *((bool*)result) = false;

    usleep(100000);

    struct xntx *tx;
    if (!xntx_create(&tx, db, XNTXMODE_WR))
        return result;

    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
    struct xnpg page;
    for (int i = 0; i < 2; i++) {
        if (!xnpgr_allocate_page(&meta_page, tx, &page))
            return result;
    }

    if (!xntx_commit(tx))
        return result;

    *((bool*)result) = true;
    return result;
}


/*
 * 0 make reader 1
 * 1 make writer, and commit
 * 2 make reader 2 and verify it sees new data.  Reader 1 still sees old data.
 * 3 Close reader 1.  writer should flush at this point, but NOT free itself
 * 4 reader 2 still sees new data.  close reader 2
 * 5 make reader 3.  reader 3 also sees new data. close reader 3
*/

void cc_txs_after_commit_snapshot() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));

    pthread_t reader1;
    pthread_t reader2;
    pthread_t reader3;
    pthread_t writer;

    pthread_create(&reader1, NULL, after_commit_reader1_fcn, db);
    pthread_create(&reader2, NULL, after_commit_reader2_fcn, db);
    pthread_create(&reader3, NULL, after_commit_reader3_fcn, db);
    pthread_create(&writer, NULL, after_commit_writer_fcn, db);

    {
        void *status;
        pthread_join(reader1, &status);
        bool ok = *((bool*)status);
        free(status);
        assert(ok);
    }
    {
        void *status;
        pthread_join(reader2, &status);
        bool ok = *((bool*)status);
        free(status);
        assert(ok);
    }
    {
        void *status;
        pthread_join(reader3, &status);
        bool ok = *((bool*)status);
        free(status);
        assert(ok);
    }
    {
        void *status;
        pthread_join(writer, &status);
        bool ok = *((bool*)status);
        free(status);
        assert(ok);
    }


    assert(xndb_free(db));
}

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
        if (!xntx_create(&tx, db, XNTXMODE_WR))
            return result;

        struct xnpg page;

        struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
        if (!xnpgr_allocate_page(&meta_page, tx, &page))
            return result;
        if (!xntx_commit(tx))
            return result;
    } else {
        struct xntx *tx;
        if (!xntx_create(&tx, db, XNTXMODE_RD))
            return result;
        struct xnpg page = { .file_handle = tx->db->file_handle, .idx = 0 };

        uint8_t *buf = malloc(XNPG_SZ);
        if (!xnpg_read(&page, tx, buf, 0, XNPG_SZ))
            return result;
        free(buf);
        if (!xntx_close((void**)&tx))
            return result;
    }

    *((bool*)result) = true;
    return result;
}

void cc_many_writers_and_readers() {
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
    assert(xnfile_create(&handle, "dummy", false, false));
    uint8_t buf;
    assert(xnfile_read(handle, &buf, 0, sizeof(uint8_t)));
    assert(buf == 255);
    assert(xnfile_read(handle, &buf, sizeof(uint8_t), sizeof(uint8_t)));
    assert(buf == 255);
    assert(xnfile_read(handle, &buf, sizeof(uint8_t) * 2, sizeof(uint8_t)));
    assert(buf == 0);
    assert(xnfile_close((void**)&handle));

    assert(xndb_free(db));
}


void tx_tests() {
    append_test(tx_empty_commit_rollback);
    append_test(tx_commit);
    append_test(tx_rollback);
    append_test(tx_read_own_writes);
    append_test(cc_txs_before_commit_snapshot);
    append_test(cc_txs_after_commit_snapshot);
    append_test(cc_many_writers_and_readers);
}
