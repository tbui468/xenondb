#pragma once

#include "test.h"
#include "heap.h"

void heap_create_free() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    struct xntx *tx;
    assert(xntx_create(&tx, db, XNTXMODE_WR));
    
    struct xnhp *hp;
    assert(xnhp_open(&hp, "dummy/heapfile", true, tx));
    assert(xntx_commit(tx));
    assert(xnhp_free((void**)&hp));
    assert(xndb_free(db));
}


void heap_tests() {
    append_test(heap_create_free);
}
