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
        xn_ensure(xn_malloc(XNPG_SZ, (void**)&cpy));
        xn_ensure(xnpg_copy(page, cpy));
        xn_ensure(xntbl_insert(tx->mod_pgs, page, cpy));
    }

    memcpy(cpy + offset, buf, size);

    if (log) {
        uint8_t *update_data;
        size_t data_size = size + sizeof(uint64_t) + sizeof(int);; //including page index and offset
        xn_ensure(xn_malloc(data_size, (void**)&update_data));
        xnmm_defer(update_data);
        memcpy(update_data, &page->idx, sizeof(uint64_t));
        memcpy(update_data + sizeof(uint64_t), &offset, sizeof(int));
        memcpy(update_data + sizeof(uint64_t) + sizeof(int), buf, size);

        uint8_t *rec;
        size_t rec_size = xnlog_record_size(data_size);
        xn_ensure(xn_malloc(rec_size, (void**)&rec));
        xnmm_defer(rec);
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

xnresult_t xnpgr_find_free_page(struct xnpg *meta_page, struct xntx *tx, struct xnpg *new_page) {
    xnmm_init();
    int page_count = tx->db->file_handle->size / XNPG_SZ;
    int i, j;
    for (i = 0; i < page_count; i++) {
        uint8_t byte;
        xn_ensure(xnpg_read(meta_page, tx, &byte, i * sizeof(uint8_t), sizeof(uint8_t)));
        for (j = 0; j < 8; j++) {
            uint8_t mask = 1 << j;
            uint8_t bit = (mask & byte) >> j;

            if (bit == 0)
                goto found_free_bit;
        }
    }

    //TODO should this be xn_ensure(false)?
    return false;

found_free_bit:
    new_page->file_handle = meta_page->file_handle;
    new_page->idx = i * 8 + j;
    return xn_ok();
}

int xnpgr_bitmap_byte_offset(uint64_t page_idx) {
    return page_idx / 8;
}

xnresult_t xnpgr_free_page(struct xnpg *meta_page, struct xntx *tx, struct xnpg page) {
    xnmm_init();

    //set bit to 'free'
    uint8_t byte;
    xn_ensure(xnpg_read(meta_page, tx, &byte, xnpgr_bitmap_byte_offset(page.idx), sizeof(uint8_t)));
    byte &= ~(1 << (page.idx % 8));
    xn_ensure(xnpg_write(meta_page, tx, &byte, xnpgr_bitmap_byte_offset(page.idx), sizeof(uint8_t), true));
    return xn_ok();
}

xnresult_t xnpgr_allocate_page(struct xnpg *meta_page, struct xntx *tx, struct xnpg *page) {
    xnmm_init();

    xn_ensure(xnpgr_find_free_page(meta_page, tx, page));

    //zero out new page data
    uint8_t *buf;
    xn_ensure(xn_malloc(XNPG_SZ, (void**)&buf));
    xnmm_defer(buf);

    memset(buf, 0, XNPG_SZ);
    xn_ensure(xnpg_write(page, tx, buf, 0, XNPG_SZ, true));

    //set bit to 'used'
    uint8_t byte;
    xn_ensure(xnpg_read(meta_page, tx, &byte, xnpgr_bitmap_byte_offset(page->idx), sizeof(uint8_t)));
    byte |= 1 << (page->idx % 8);
    xn_ensure(xnpg_write(meta_page, tx, &byte, xnpgr_bitmap_byte_offset(page->idx), sizeof(uint8_t), true));

    return xn_ok();
}
