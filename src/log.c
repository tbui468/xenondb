#include "log.h"

#include <stdlib.h>
#include <string.h>

xnresult_t xnlog_create(struct xnlog **out_log, const char *log_path, bool create) {
    xnmm_init();
    struct xnlog *log;
    xn_ensure(xn_malloc((void**)&log, sizeof(struct xnlog)));

    xn_ensure(xnfile_create(&log->page.file_handle, log_path, create, true));
    xn_ensure(xnfile_set_size(log->page.file_handle, 32 * XNPG_SZ)); //TODO temporary size - will need to add code to resize file if needed

    //make iterator here to get last record offset
    struct xnlogitr *itr;
    xn_ensure(xnlogitr_create(log, &itr));
    bool valid = true;
    while (valid)
        xn_ensure(xnlogitr_next(itr, &valid));
    log->page.idx = itr->page.idx;
    log->page_off = itr->page_off;
    xn_ensure(xnlogitr_free(itr));

    xn_ensure(xn_aligned_malloc((void**)&log->buf, XNPG_SZ));
    xn_ensure(xnpg_copy(&log->page, log->buf));
    
    log->highest_tx_flushed = -1;

    *out_log = log;
    return xn_ok();
}


xnresult_t xnlog_free(void **l) {
    xnmm_init();
    struct xnlog *log = (struct xnlog*)(*l);
    xn_ensure(xnfile_close((void**)&log->page.file_handle));
    free(log->buf);
    free(log);
    return xn_ok();
}

xnresult_t xnlog_flush(struct xnlog *log) {
    xnmm_init();
    if (log->page.idx >= log->page.file_handle->size / XNPG_SZ)
        xn_ensure(xnfile_grow(log->page.file_handle));
    xn_ensure(xnpg_flush(&log->page, log->buf));
    //don't need to call xnfile_sync since log files are opend with O_DATASYNC flag
    return xn_ok();
}

xnresult_t xnlog_append(struct xnlog *log, const uint8_t *log_record, size_t size) {
    xnmm_init();
    size_t written = 0;
    while (written < size) {
        size_t to_write = size - written;
        size_t remaining = XNPG_SZ - log->page_off;
        size_t s = remaining < to_write ? remaining : to_write;
        memcpy(log->buf + log->page_off, log_record + written, s);

        log->page_off += s;
        written += s;

        if (log->page_off == XNPG_SZ) {
            xn_ensure(xnlog_flush(log));
            log->page.idx++;
            log->page_off = 0;
        }
    }

    return xn_ok();
}

size_t xnlog_record_size(size_t data_size) {
    size_t size = sizeof(int);      //tx id
    size += sizeof(enum xnlogt);    //log type
    size += sizeof(size_t);         //data size 
    size += data_size;              //data
    size += sizeof(uint32_t);       //checksum
    return size;
}

xnresult_t xnlog_serialize_record(int tx_id, 
                                  enum xnlogt type, 
                                  size_t data_size, 
                                  uint8_t *data, 
                                  uint8_t *buf) {
    xnmm_init();
    off_t off = 0;
    memcpy(buf + off, &tx_id, sizeof(int));
    off += sizeof(int);
    memcpy(buf + off, &type, sizeof(enum xnlogt));
    off += sizeof(enum xnlogt);
    memcpy(buf + off, &data_size, sizeof(size_t));
    off += sizeof(size_t);
    memcpy(buf + off, data, data_size);
    off += data_size;
    uint32_t checksum = xn_hash(buf, off);
    memcpy(buf + off, &checksum, sizeof(uint32_t));
    off += sizeof(uint32_t);

    return xn_ok();
}

xnresult_t xnlogitr_create(struct xnlog *log, struct xnlogitr **out_itr) {
    xnmm_init();
    struct xnlogitr *itr;
    xn_ensure(xn_malloc((void**)&itr, sizeof(struct xnlogitr)));
    xn_ensure(xn_aligned_malloc((void**)&itr->buf, XNPG_SZ));
    itr->page = log->page;
    itr->page.idx = 0;
    itr->page_off = -1;
    xn_ensure(xnpg_copy(&itr->page, itr->buf));

    *out_itr = itr;
    return xn_ok();
}

xnresult_t xnlogitr_seek(struct xnlogitr *itr, uint64_t page_idx, int page_off) {
    xnmm_init();
    itr->page.idx = page_idx;
    itr->page_off = page_off;
    xn_ensure(xnpg_copy(&itr->page, itr->buf));
    return xn_ok();
}

xnresult_t xnlogitr_read_span(const struct xnlogitr *itr, uint8_t *buf, off_t off, size_t size) {
    xnmm_init();
    struct xnpg page = itr->page;
    xnmm_scoped_alloc(scoped_ptr, xn_free, xn_aligned_malloc, &scoped_ptr, XNPG_SZ);
    uint8_t *page_buf = (uint8_t*)scoped_ptr;

    int page_off = itr->page_off + off;
    if (page_off >= XNPG_SZ) {
        page_off -= XNPG_SZ;
        page.idx++;
    }

    xn_ensure(xnpg_copy(&page, page_buf));

    size_t nread = 0;
    while (nread < size) {
        size_t to_read = size - nread;
        size_t remaining = XNPG_SZ - page_off;
        size_t s = to_read < remaining ? to_read : remaining;
        memcpy(buf + nread, page_buf + page_off, s);

        nread += s;
        page_off += s;

        if (page_off == XNPG_SZ) {
            page_off = 0;
            page.idx++;
            xn_ensure(xnpg_copy(&page, page_buf));
        }
    }

    return xn_ok();
}

xnresult_t xnlogitr_read_data(struct xnlogitr *itr, uint8_t *buf, size_t size) {
    xnmm_init();
    const off_t header_size = sizeof(int) + sizeof(enum xnlogt) + sizeof(size_t);
    xn_ensure(xnlogitr_read_span(itr, buf, header_size, size));

    return xn_ok();
}

xnresult_t xnlogitr_read_header(const struct xnlogitr *itr, int *tx_id, enum xnlogt *type, size_t *data_size) {
    xnmm_init();
    const size_t header_size = sizeof(int) + sizeof(enum xnlogt) + sizeof(size_t);
    uint8_t hdr_buf[header_size];

    xn_ensure(xnlogitr_read_span(itr, hdr_buf, 0, header_size));

    *tx_id = *((int*)hdr_buf);
    *type = *((enum xnlogt*)(hdr_buf + sizeof(int)));
    *data_size = *((size_t*)(hdr_buf + sizeof(int) + sizeof(enum xnlogt)));

    return xn_ok();
}

size_t xnlog_record_size(size_t data_size);

xnresult_t xnlogitr_next(struct xnlogitr *itr, bool* valid) {
    xnmm_init();
    if (itr->page_off == -1) {
        itr->page_off = 0;
    } else {
        int tx_id;
        enum xnlogt type;
        size_t data_size;
        xn_ensure(xnlogitr_read_header(itr, &tx_id, &type, &data_size));

        itr->page_off += xnlog_record_size(data_size);
        while (itr->page_off >= XNPG_SZ) {
            itr->page_off -= XNPG_SZ;
            itr->page.idx++;
        }
        xn_ensure(xnpg_copy(&itr->page, itr->buf));
    }

    int tx_id;
    enum xnlogt type;
    size_t data_size;
    xn_ensure(xnlogitr_read_header(itr, &tx_id, &type, &data_size));
    *valid = tx_id != 0;
    return xn_ok();
}

xnresult_t xnlogitr_free(struct xnlogitr *itr) {
    xnmm_init();
    free(itr->buf);
    free(itr);
    return xn_ok();
}
