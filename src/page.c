#include "page.h"
#include "file.h"
#include "util.h"
#include "tx.h"
#include "log.h"
#include "db.h"

#include <string.h>

xnresult_t xnpg_flush(struct xnpg *page, const uint8_t *buf) {
    xnmm_init();
    xn_ensure((page->idx + 1) * XNPG_SZ <= page->file_handle->size);
    xn_ensure(xnfile_write(page->file_handle, buf, page->idx * XNPG_SZ, XNPG_SZ)); 
    return xn_ok();
}

xnresult_t xnpg_copy(struct xnpg *page, uint8_t *buf) {
    xnmm_init();
    xn_ensure((page->idx + 1) * XNPG_SZ <= page->file_handle->size);
    xn_ensure(xnfile_read(page->file_handle, buf, page->idx * XNPG_SZ, XNPG_SZ));
    return xn_ok();
}

xnresult_t xnpg_mmap(struct xnpg *page, uint8_t **ptr) {
    xnmm_init();
    xn_ensure((page->idx + 1) * XNPG_SZ <= page->file_handle->size);
    xn_ensure(xnfile_mmap(page->file_handle, page->idx * XNPG_SZ, XNPG_SZ, (void**)ptr));
    return xn_ok();
}

xnresult_t xnpg_munmap(uint8_t *ptr) {
    xnmm_init();
    xn_ensure(xnfile_munmap((void*)ptr, XNPG_SZ));
    return xn_ok();
}


xnresult_t xnpg_write(struct xnpg *page, struct xntx *tx, const uint8_t *buf, int offset, size_t size, bool log) {
    xnmm_init();
    xn_ensure(tx->mode == XNTXMODE_WR);

    uint8_t *cpy;

    if (!(cpy = xntbl_find(tx->mod_pgs, page))) {
        xnmm_alloc(xn_free, xn_malloc, (void**)&cpy, XNPG_SZ);
        xn_ensure(xnpg_copy(page, cpy));
        xn_ensure(xntbl_insert(tx->mod_pgs, page, cpy));
    }

    memcpy(cpy + offset, buf, size);

    if (log) {
        size_t data_size = size + + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(int);; //including resource id, page index and offset
        xnmm_scoped_alloc(scoped_ptr1, xn_free, xn_malloc, &scoped_ptr1, data_size);
        uint8_t *update_data = (uint8_t*)scoped_ptr1;
        memcpy(update_data, &page->file_handle->id, sizeof(uint64_t));
        memcpy(update_data + sizeof(uint64_t), &page->idx, sizeof(uint64_t));
        memcpy(update_data + sizeof(uint64_t) * 2, &offset, sizeof(int));
        memcpy(update_data + sizeof(uint64_t) * 2 + sizeof(int), buf, size);

        size_t rec_size = xnlog_record_size(data_size);
        xnmm_scoped_alloc(scoped_ptr2, xn_free, xn_malloc, &scoped_ptr2, rec_size);
        uint8_t *rec = (uint8_t*)scoped_ptr2;
        xn_ensure(xnlog_serialize_record(tx->id, XNLOGT_UPDATE, data_size, update_data, rec));
        xn_ensure(xnlog_append(tx->db->log, rec, rec_size));
    }

    return xn_ok();
}

xnresult_t xnpg_read(struct xnpg *page, struct xntx *tx, uint8_t *buf, int offset, size_t size) {
    xnmm_init();
    if (tx->mode == XNTXMODE_WR || tx->mod_pgs) {
        uint8_t *cpy;

        if ((cpy = xntbl_find(tx->mod_pgs, page))) {
            memcpy(buf, cpy + offset, size);
            return xn_ok();
        }
    }

    uint8_t *ptr;
    if (!(ptr = xntbl_find(tx->db->pg_tbl, page))) {
        xn_ensure(xnpg_mmap(page, &ptr));
        xn_ensure(xntbl_insert(tx->db->pg_tbl, page, ptr));
    }

    memcpy(buf, ptr + offset, size);

    return xn_ok();
}

