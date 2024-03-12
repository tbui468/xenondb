#pragma once

#include "log.h"
#include "test.h"

void log_create_free() {
    struct xnlog *log;
    assert(xnlog_create("dummy", &log));
    assert(xnlog_free(log));
}

void log_make_nonupdate_record() {
    size_t rec_size = xnlog_record_size(0);
    uint8_t *buf;
    assert(xn_malloc(rec_size, (void**)&buf));

    assert(xnlog_serialize_record(1, XNLOGT_START, 0, NULL, buf));
    assert(xnlog_serialize_record(1, XNLOGT_COMMIT, 0, NULL, buf)); //overwriting old record since we don't need it
    free(buf);
}

void log_make_update_record() {
    const char *msg = "hello";
    size_t rec_size = xnlog_record_size(strlen(msg) + 1);
    uint8_t *buf;
    assert(xn_malloc(rec_size, (void**)&buf));

    assert(xnlog_serialize_record(1, XNLOGT_UPDATE, strlen(msg) + 1, (uint8_t*)msg, buf));
    free(buf);
}

void log_append_record() {
    struct xnlog *log;
    assert(xnlog_create("dummy", &log));

    //make dummy record
    size_t rec_size = xnlog_record_size(0);
    uint8_t *buf;
    assert(xn_malloc(rec_size, (void**)&buf));
    assert(xnlog_serialize_record(1, XNLOGT_START, 0, NULL, buf));

    //append one record
    assert(xnlog_append(log, buf, rec_size));
    assert(log->page_off == rec_size);

    //append another record
    assert(xnlog_append(log, buf, rec_size));
    assert(log->page_off == 2 * rec_size);

    assert(xnlog_free(log));
}

void log_append_record_overflow() {
    struct xnlog *log;
    assert(xnlog_create("dummy", &log));
    //make dummy record
    size_t rec_size = xnlog_record_size(0);
    uint8_t *buf;
    assert(xn_malloc(rec_size, (void**)&buf));
    assert(xnlog_serialize_record(1, XNLOGT_START, 0, NULL, buf));

    //append records until overflow log buffer
    int recs = 0;
    while (log->page_off < XNPG_SZ && log->page.idx < 1) {
        assert(xnlog_append(log, buf, rec_size));
        recs++;
    }

    assert((recs * rec_size) % XNPG_SZ == log->page_off);

    free(buf);
    assert(xnlog_free(log));
}

void log_flush_buffer() {
    struct xnlog *log;
    assert(xnlog_create("dummy", &log));
    //make dummy record
    size_t rec_size = xnlog_record_size(0);
    uint8_t *buf;
    assert(xn_malloc(rec_size, (void**)&buf));
    assert(xnlog_serialize_record(1, XNLOGT_START, 0, NULL, buf));

    //append records until overflow log buffer
    int recs = 0;
    while (log->page_off < XNPG_SZ && log->page.idx < 1) {
        assert(xnlog_append(log, buf, rec_size));
        //manually flush buffer
        assert(xnlog_flush(log));
        recs++;
    }

    assert((recs * rec_size) % XNPG_SZ == log->page_off);

    free(buf);
    assert(xnlog_free(log));
}

void log_tests() {
    append_test(log_create_free);
    append_test(log_make_nonupdate_record);
    append_test(log_make_update_record);
    append_test(log_append_record);
    append_test(log_append_record_overflow);
    append_test(log_flush_buffer);
}


