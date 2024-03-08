#pragma once

#include "page.h"

enum xnlogt {
    XNLOGT_START,
    XNLOGT_UPDATE,
    XNLOGT_COMMIT
};

struct xnlog {
    uint8_t *buf;
    int page_off;

    int highest_tx_flushed;
    struct xnpg page;
};

struct xnlogitr {
    struct xnpg page;
    int page_off;
    uint8_t *buf;
};

xnresult_t xnlog_create(const char *log_path, struct xnlog **out_log);
xnresult_t xnlog_flush(struct xnlog *log);
xnresult_t xnlog_append(struct xnlog *log, const uint8_t *log_record, size_t size);
size_t xnlog_record_size(size_t data_size);
xnresult_t xnlog_serialize_record(int tx_id, enum xnlogt type, size_t data_size, uint8_t *data, uint8_t *buf);

xnresult_t xnlogitr_create(struct xnlog *log, struct xnlogitr **out_itr);
xnresult_t xnlogitr_seek(struct xnlogitr *itr, uint64_t page_idx, int page_off);
xnresult_t xnlogitr_read_span(const struct xnlogitr *itr, uint8_t *buf, off_t off, size_t size);
xnresult_t xnlogitr_read_data(struct xnlogitr *itr, uint8_t *buf, size_t size);
xnresult_t xnlogitr_read_header(const struct xnlogitr *itr, int *tx_id, enum xnlogt *type, size_t *data_size);
xnresult_t xnlogitr_next(struct xnlogitr *itr, bool* valid);
xnresult_t xnlogitr_free(struct xnlogitr *itr);
