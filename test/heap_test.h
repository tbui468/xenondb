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
        assert(xndb_open_resource(db, &rs, "data", true, XNRST_HEAP, tx));
        assert(xndb_close_resource(rs));
    }
    {
        struct xnrs rs;
        assert(xndb_open_resource(db, &rs, "data2", true, XNRST_HEAP, tx));
        assert(xndb_close_resource(rs));
    }

    {
        struct xnrs rs;
        assert(xndb_open_resource(db, &rs, "data2", false, XNRST_HEAP, tx));
        assert(xndb_close_resource(rs));
    }

    {
        struct xnrs rs;
        assert(xndb_open_resource(db, &rs, "data2", false, XNRST_HEAP, tx));
        assert(xndb_close_resource(rs));
    }
    
    assert(xntx_commit(tx));
    assert(xndb_free(db));
}


void heap_tests() {
    append_test(heap_create_free);
}
