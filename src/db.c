#include "db.h"
#include "container.h"
#include "heap.h"
#include <stdlib.h>
#include <string.h>
#include <limits.h>

xnresult_t xndb_create(const char *dir_path, bool create, struct xndb **out_db) {
    xnmm_init();

    struct xndb *db;
    xnmm_alloc(xn_free, xn_malloc, (void**)&db, sizeof(struct xndb));

    //initialize locks and protected data
    xnmm_alloc(xnmtx_free, xnmtx_create, &db->wrtx_lock);
    xnmm_alloc(xnmtx_free, xnmtx_create, &db->rdtx_count_lock);
    xnmm_alloc(xnmtx_free, xnmtx_create, &db->committed_wrtx_lock);
    xnmm_alloc(xnmtx_free, xnmtx_create, &db->tx_id_counter_lock);
    db->rdtx_count = 0;
    db->committed_wrtx = NULL;
    db->tx_id_counter = 1;

    if (create) {
        xn_ensure(xn_mkdir(dir_path, 0700));
    }

    //log file does not use a container/heap structure,
    //and uses the file api directly
    char log_path[PATH_MAX];
    log_path[0] = '\0';
    strcat(log_path, dir_path);
    strcat(log_path, "/log");
    xnmm_alloc(xnlog_free, xnlog_create, &db->log, log_path, create);

    //need table initialized for tx to work
    xnmm_alloc(xntbl_free, xntbl_create, &db->pg_tbl, true);

    //resource catalog
    char catalog_path[PATH_MAX];
    catalog_path[0] = '\0';
    strcat(catalog_path, dir_path);
    strcat(catalog_path, "/catalog");

    struct xntx *tx;
    xnmm_alloc(xntx_rollback, xntx_create, &tx, db, XNTXMODE_WR);

    xnmm_scoped_alloc(scoped_ptr, xnhp_free, xnhp_open, (struct xnhp**)&scoped_ptr, catalog_path, create, tx);
    struct xnhp *hp = (struct xnhp*)scoped_ptr;

    if (create) {
        size_t path_size = strlen(hp->meta.file_handle->path);
        size_t size = path_size + sizeof(uint64_t);
        uint64_t resource_id = 42; //TODO temporary placeholder until tx ids are implemented. This needs to be stored in catalog too
        xnmm_scoped_alloc(scoped_ptr, xn_free, xn_malloc, &scoped_ptr, size);
        uint8_t *buf = (uint8_t*)scoped_ptr;
        memcpy(buf, &resource_id, sizeof(uint64_t));
        memcpy(buf + sizeof(uint64_t), hp->meta.file_handle->path, path_size);

        struct xnitemid id;
        xn_ensure(xnhp_insert(hp, tx, buf, size, &id));
    }

    xn_ensure(xntx_commit(tx));

    //TODO commenting out recovery for now to fix major bugs caused by removing db->file_handle
    //xn_ensure(xndb_recover(db));

   *out_db = db;
    return xn_ok();
}

xnresult_t xndb_free(struct xndb *db) {
    xnmm_init();
    xn_ensure(xnmtx_free((void**)&db->wrtx_lock));
    xn_ensure(xnmtx_free((void**)&db->rdtx_count_lock));
    xn_ensure(xnmtx_free((void**)&db->committed_wrtx_lock));
    xn_ensure(xnmtx_free((void**)&db->tx_id_counter_lock));
    xn_ensure(xntbl_free((void**)&db->pg_tbl));
    free(db);
    return xn_ok();
}



static xnresult_t xndb_redo(struct xndb *db, struct xntx *tx, uint64_t page_idx, int page_off, int tx_id) {
    xnmm_init();

    xnmm_scoped_alloc(scoped_ptr, xnlogitr_free, xnlogitr_create, (struct xnlogitr**)&scoped_ptr, db->log);
    struct xnlogitr *itr = (struct xnlogitr*)scoped_ptr;

    xn_ensure(xnlogitr_seek(itr, page_idx, page_off));

    bool valid = true;
    while (true) {
        xn_ensure(xnlogitr_next(itr, &valid));
        if (!valid) break;

        int cur_tx_id;
        enum xnlogt type;
        size_t data_size;
        xn_ensure(xnlogitr_read_header(itr, &cur_tx_id, &type, &data_size));
        if (type == XNLOGT_COMMIT && cur_tx_id == tx_id) {
            break;
        } else if (type == XNLOGT_UPDATE && cur_tx_id == tx_id) {
            xnmm_scoped_alloc(scoped_ptr, xn_free, xn_malloc, &scoped_ptr, data_size);
            uint8_t *buf = (uint8_t*)scoped_ptr;

            xn_ensure(xnlogitr_read_data(itr, buf, data_size));

            //writing changes back to file (but not logging)
            //TODO: need to replace tx->db->file_handle with the actual resource that needs to be redone
            struct xnpg page = { .file_handle = NULL, .idx = *((uint64_t*)buf) };
            size_t data_hdr_size = sizeof(uint64_t) + sizeof(int);
            int off = *((int*)(buf + sizeof(uint64_t)));
            size_t size = data_size - data_hdr_size;
            xn_ensure(xnpg_write(&page, tx, buf + data_hdr_size, off, size, false));
        }
    }

    return xn_ok();
}

xnresult_t xndb_recover(struct xndb *db) {
    xnmm_init();

    xnmm_scoped_alloc(scoped_ptr, xnlogitr_free, xnlogitr_create, (struct xnlogitr**)&scoped_ptr, db->log);
    struct xnlogitr *itr = (struct xnlogitr*)scoped_ptr;

    struct xntx *tx;
    xnmm_alloc(xntx_rollback, xntx_create, &tx, db, XNTXMODE_WR);

    uint64_t start_pageidx;
    int start_pageoff;
    int start_txid = 0;

    bool valid;
    while (true) {
        xn_ensure(xnlogitr_next(itr, &valid));
        if (!valid) break;
        int tx_id;
        enum xnlogt type;
        size_t data_size;
        xn_ensure(xnlogitr_read_header(itr, &tx_id, &type, &data_size));
        if (type == XNLOGT_START) {
            start_pageidx = itr->page.idx;
            start_pageoff = itr->page_off;
            start_txid = tx_id;
        } else if (type == XNLOGT_COMMIT && start_txid == tx_id) {
            xn_ensure(xndb_redo(db, tx, start_pageidx, start_pageoff, start_txid));
        }
    }

    xn_ensure(xntx_commit(tx));
    return xn_ok();
}
