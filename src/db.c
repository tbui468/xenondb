#include "db.h"
#include "container.h"
#include <stdlib.h>
#include <string.h>
#include <limits.h>

static xnresult_t xndb_init_resource(struct xndb *db, struct xnfile *file) {
    xnmm_init();

    struct xntx *tx;
    xnmm_alloc(xntx_rollback, xntx_create, &tx, db, XNTXMODE_WR);

    //metadata page
    struct xnpg meta_page = { .file_handle = file, .idx = 0 };

    //initialize metadata page
    {
        //zero out page
        xnmm_scoped_alloc(scoped_ptr, xn_free, xn_malloc, &scoped_ptr, XNPG_SZ);
        uint8_t *buf = (uint8_t*)scoped_ptr;
        memset(buf, 0, XNPG_SZ);
        xn_ensure(xnpg_write(&meta_page, tx, buf, 0, XNPG_SZ, true));

        //set bit for metadata page to 'used'
        uint8_t page0_used = 1;
        xn_ensure(xnpg_write(&meta_page, tx, &page0_used, 0, sizeof(uint8_t), true));
    }

    xn_ensure(xntx_commit(tx));

    return xn_ok();
}

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


    //TODO log records need to include a resource manager id to attach it to a file
    //log file
    char log_path[PATH_MAX];
    log_path[0] = '\0';
    strcat(log_path, dir_path);
    strcat(log_path, "/log");
    xnmm_alloc(xnlog_free, xnlog_create, &db->log, log_path, create);

    //system catalog
    char catalog_path[PATH_MAX];
    catalog_path[0] = '\0';
    strcat(catalog_path, dir_path);
    strcat(catalog_path, "/catalog");
    xnmm_alloc(xnfile_close, xnfile_create, &db->file_handle, catalog_path, create, false);
    xn_ensure(xnfile_set_size(db->file_handle, XNPG_SZ * 32));

    //TODO page table needs to be rewritten to allow multiple files
    xnmm_alloc(xntbl_free, xntbl_create, &db->pg_tbl, true);

    //read in metadata page and set bitmap if new database
    //TODO refactor this - opening a new database will simply create the directory and a catalog file
    //the actual files need to be created with xnhp_create or with some other persistent data structure
    if (create) {
        //TODO: init catalog
        //add catalog resource to catalog
        xn_ensure(xndb_init_resource(db, db->file_handle));


        //update catalog to include new resource
        {
            struct xntx *tx;
            xnmm_alloc(xntx_rollback, xntx_create, &tx, db, XNTXMODE_WR);
            struct xnpg meta_page = { .file_handle = tx->db->file_handle, .idx = 0 };
            struct xnpg page;
            xn_ensure(xnpgr_allocate_page(&meta_page, tx, &page));

            xnmm_scoped_alloc(scoped_ptr, xnctn_free, xnctn_create, (struct xnctn**)&scoped_ptr, tx, page);
            struct xnctn *ctn = (struct xnctn*)scoped_ptr;
            xn_ensure(xnctn_init(ctn));

            struct xnitemid id;
            //TODO write fileid and filename to catalog
            const char* buf = "xxxxxxxxxx";
            xn_ensure(xnctn_insert(ctn, buf, strlen(buf), &id));
            xn_ensure(xntx_commit(tx));
        }

        //xn_ensure(xndb_add_resource(db));
        /*
        //allocate a page and make a single container for now - will replace with heap file later
        //insert this resource: id = 1, name = "catalog
        struct xnpg page;
        xn_ensure(xnpgr_allocate_page(&meta_page, tx, &page));

        xnmm_scoped_alloc(scoped_ptr, xnctn_free, xnctn_create, (struct xnctn**)&scoped_ptr, tx, page);
        struct xnctn *ctn = (struct xnctn*)scoped_ptr;

        struct xnitemid id;
        //TODO write fileid and filename to catalog
        const char* buf = "xxxxxxxxxx";
        printf("inserting into container\n"); //
        xn_ensure(xnctn_insert(ctn, buf, strlen(buf), &id));
        printf("item idx: %ld, %d\n", id.pg_idx, id.arr_idx);

        xn_ensure(xntx_commit(tx));*/
    }

    xn_ensure(xndb_recover(db));

   *out_db = db;
    return xn_ok();
}

xnresult_t xndb_free(struct xndb *db) {
    xnmm_init();
    xn_ensure(xnmtx_free((void**)&db->wrtx_lock));
    xn_ensure(xnmtx_free((void**)&db->rdtx_count_lock));
    xn_ensure(xnmtx_free((void**)&db->committed_wrtx_lock));
    xn_ensure(xnmtx_free((void**)&db->tx_id_counter_lock));
    xn_ensure(xnfile_close((void**)&db->file_handle));
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
            struct xnpg page = { .file_handle = tx->db->file_handle, .idx = *((uint64_t*)buf) };
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
