#pragma once

#include "test.h"
#include "heap.h"

void heap_create_free() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    struct xntx *tx;
    assert(xntx_create(&tx, db, XNTXMODE_WR));
    {
        struct xnrs rs;
        assert(xnrs_open(&rs, db, "data", true, XNRST_HEAP, tx));
        assert(xnrs_close(rs));
    }
    {
        struct xnrs rs;
        assert(xnrs_open(&rs, db, "data2", true, XNRST_HEAP, tx));
        assert(xnrs_close(rs));
    }

    {
        struct xnrs rs;
        assert(xnrs_open(&rs, db, "data2", false, XNRST_HEAP, tx));
        assert(xnrs_close(rs));
    }

    {
        struct xnrs rs;
        assert(xnrs_open(&rs, db, "data2", false, XNRST_HEAP, tx));
        assert(xnrs_close(rs));
    }
    
    assert(xntx_commit(tx));
    assert(xndb_free(db));
}

void heap_put() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    struct xntx *tx;
    assert(xntx_create(&tx, db, XNTXMODE_WR));
    {
        struct xnrs rs;
        assert(xnrs_open(&rs, db, "data", true, XNRST_HEAP, tx));

        size_t size = 8;
        uint8_t *buf = malloc(size);
        struct xnitemid id;

        {
            memset(buf, 'x', size);
            assert(xnrs_put(rs, tx, size, buf, &id));
        }
        {
            memset(buf, 'y', size);
            assert(xnrs_put(rs, tx, size, buf, &id));
        }
        {
            memset(buf, 'z', size);
            assert(xnrs_put(rs, tx, size, buf, &id));
        }

        assert(xnrs_close(rs));
        free(buf);
    }
    
    assert(xntx_commit(tx));
    assert(xndb_free(db));
}

void heap_iterator() {

}


void heap_tests() {
    append_test(heap_create_free);
    append_test(heap_put);
}
