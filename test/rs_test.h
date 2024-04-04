#pragma once

#include "test.h"

void rs_put_get() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    struct xntx *tx;
    assert(xntx_create(&tx, db, XNTXMODE_WR));
    {
        struct xnrs rs;
        assert(xnrs_open(&rs, db, "data", true, XNRST_HEAP, tx));


        {
			float vectors[2][3] = { { 1.0f, 2.0f, 3.0f },
									{ 4.0f, 5.0f, 6.0f } };
        	struct xnitemid id;
            assert(xnrs_put(rs, sizeof(vectors), (uint8_t*)vectors, &id));
			size_t size;
			assert(xnrs_get_size(rs, id, &size));
			assert(sizeof(vectors) == size);
			uint8_t *buf = malloc(size);
			assert(xnrs_get(rs, id, buf, size));
			assert(memcmp(vectors, buf, size) == 0);
			free(buf);
        }

    }
    
    assert(xntx_commit(tx));
    assert(xndb_free(db));
}

void rs_scan() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
    struct xntx *tx;
    assert(xntx_create(&tx, db, XNTXMODE_WR));

    struct xnrs rs;
    assert(xnrs_open(&rs, db, "data", true, XNRST_HEAP, tx));

    {
        float vectors[2][3] = { { 1.0f, 2.0f, 3.0f },
                                { 4.0f, 5.0f, 6.0f } };
        struct xnitemid id;
        assert(xnrs_put(rs, sizeof(vectors), (uint8_t*)vectors, &id));
    }

    {
        float vectors[2][3] = { { 1.0f, 2.0f, 3.0f },
                                { 4.0f, 5.0f, 6.0f } };
        struct xnitemid id;
        assert(xnrs_put(rs, sizeof(vectors), (uint8_t*)vectors, &id));
    }


    struct xnrsscan scan;
    assert(xnrsscan_open(&scan, rs));
    bool more;
    int count = 0;
    while (true) {
        assert(xnrsscan_next(&scan, &more));
        if (!more)
            break; 
        count++;
    }
    assert(count == 2);
    
    assert(xntx_commit(tx));
    assert(xndb_free(db));
}

void rs_tests() {
    append_test(rs_put_get);
    append_test(rs_scan);
}
