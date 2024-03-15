#pragma once

#include "log.h"
#include "test.h"

void logitr_create_on_empty_log() {
    struct xnlog *log;
    assert(xnlog_create(&log, "dummy", true));
    
    struct xnlogitr *itr;
    assert(xnlogitr_create(log, &itr));
    assert(xnlogitr_free(itr));

    assert(xnlog_free((void**)&log));
}

void logitr_create_on_nonempty_log() {
    struct xnlog *log;
    assert(xnlog_create(&log, "dummy", true));

    size_t rec_size = xnlog_record_size(0);
    uint8_t *buf = malloc(rec_size);
    assert(xnlog_serialize_record(1, XNLOGT_START, 0, NULL, buf));

    //append 3 records
    int recs = 0;
    while (recs < 3) {
        assert(xnlog_append(log, buf, rec_size));
        recs++;
    }

    //manually flush log buffer
    assert(xnlog_flush(log));

    struct xnlogitr *itr;
    assert(xnlogitr_create(log, &itr));
    assert(xnlogitr_free(itr));

    free(buf);
    assert(xnlog_free((void**)&log));
}

void logitr_iterate_from_beginning() {
    struct xnlog *log;
    assert(xnlog_create(&log, "dummy", true));

    size_t rec_size = xnlog_record_size(0);
    uint8_t *buf = malloc(rec_size);
    assert(xnlog_serialize_record(1, XNLOGT_START, 0, NULL, buf));

    //append 3 records
    int recs = 0;
    while (recs < 3) {
        assert(xnlog_append(log, buf, rec_size));
        recs++;
    }

    //manually flush log buffer
    assert(xnlog_flush(log));

    struct xnlogitr *itr;
    assert(xnlogitr_create(log, &itr));
    int count = 0;
    bool valid;
    while (true) {
        assert(xnlogitr_next(itr, &valid));
        if (!valid)
            break;
        count++;
    }
    assert(count == 3);
    assert(xnlogitr_free(itr));

    free(buf);
    assert(xnlog_free((void**)&log));
}

void logitr_iterate_from_seek() {
    struct xnlog *log;
    assert(xnlog_create(&log, "dummy", true));

    size_t rec_size = xnlog_record_size(0);
    uint8_t *buf = malloc(rec_size);
    assert(xnlog_serialize_record(1, XNLOGT_START, 0, NULL, buf));

    //append 3 records
    int recs = 0;
    while (recs < 3) {
        assert(xnlog_append(log, buf, rec_size));
        recs++;
    }

    //manually flush log buffer
    assert(xnlog_flush(log));

    struct xnlogitr *itr;
    assert(xnlogitr_create(log, &itr));

    bool valid;

    //save location after first record
    assert(xnlogitr_next(itr, &valid));
    int64_t pg_idx = itr->page.idx;
    int pg_off = itr->page_off;

    assert(xnlogitr_next(itr, &valid));
    assert(xnlogitr_next(itr, &valid));

    assert(xnlogitr_seek(itr, pg_idx, pg_off));

    int count = 0;
    while (true) {
        assert(xnlogitr_next(itr, &valid));
        if (!valid)
            break;
        count++;
    }

    assert(count == 2);
    assert(xnlogitr_free(itr));

    free(buf);
    assert(xnlog_free((void**)&log));
}

void logitr_tests() {
    append_test(logitr_create_on_empty_log);
    append_test(logitr_create_on_nonempty_log);
    append_test(logitr_iterate_from_beginning);
    append_test(logitr_iterate_from_seek);
}
