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
    }
    {
        struct xnrs rs;
        assert(xnrs_open(&rs, db, "data2", true, XNRST_HEAP, tx));
    }

    {
        struct xnrs rs;
        assert(xnrs_open(&rs, db, "data2", false, XNRST_HEAP, tx));
    }

    {
        struct xnrs rs;
        assert(xnrs_open(&rs, db, "data2", false, XNRST_HEAP, tx));
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
            assert(xnrs_put(rs, size, buf, &id));
        }
        {
            memset(buf, 'y', size);
            assert(xnrs_put(rs, size, buf, &id));
        }
        {
            memset(buf, 'z', size);
            assert(xnrs_put(rs, size, buf, &id));
        }

        free(buf);
    }
    
    assert(xntx_commit(tx));
    assert(xndb_free(db));
}

void heap_scan() {
    struct xndb *db;
    assert(xndb_create("dummy", true, &db));
	//insert data
	{
		struct xntx *tx;
		assert(xntx_create(&tx, db, XNTXMODE_WR));

		struct xnrs rs;
		assert(xnrs_open(&rs, db, "data", true, XNRST_HEAP, tx));

		size_t size = 8;
		uint8_t *buf = malloc(size);
		struct xnitemid id;

		{
			memset(buf, 'x', size);
			assert(xnrs_put(rs, size, buf, &id));
		}
		{
			memset(buf, 'y', size);
			assert(xnrs_put(rs, size, buf, &id));
		}
		{
			memset(buf, 'z', size);
			assert(xnrs_put(rs, size, buf, &id));
		}

		free(buf);
		
		assert(xntx_commit(tx));
	}
	//iterate through data
	{
		int rec_count = 0;
		struct xntx *tx;
		assert(xntx_create(&tx, db, XNTXMODE_RD));
		struct xnrs rs;
		assert(xnrs_open(&rs, db, "data", false, XNRST_HEAP, tx));
		struct xnrsscan scan;
		assert(xnrsscan_open(&scan, rs)); //tx should be part of rs and directly passed to scan
		bool more;
		while (true) {
			assert(xnrsscan_next(&scan, &more));
			if (!more)
				break;
			rec_count++;
		    struct xnitemid id;
			assert(xnrsscan_itemid(&scan, &id));
			size_t size;
			assert(xnrs_get_size(rs, id, &size));
			uint8_t buf[size];
			assert(xnrs_get(rs, id, buf, size));
			int s = size;
			//printf("%.*s\n", s, buf);
		}
		assert(rec_count == 3);
		assert(xntx_close((void**)&tx));
	}
    assert(xndb_free(db));
}


void heap_tests() {
    append_test(heap_create_free);
    append_test(heap_put);
    append_test(heap_scan);
}
