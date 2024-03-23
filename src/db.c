#include "db.h"
#include "container.h"
#include "heap.h"
#include <stdlib.h>
#include <string.h>
#include <limits.h>

static void xndb_make_path(char *out, const char *path1, const char *path2) {
    out[0] = '\0';
    strcat(out, path1);
    strcat(out, "/");
    strcat(out, path2);
}

static xnresult_t xndb_get_file(struct xndb *db, struct xnfile **out_file, const char *filename, bool create, bool direct) {
    xnmm_init();

    char path[PATH_MAX];
    xndb_make_path(path, db->dir_path, "log");
    struct xnfile *file = NULL;

    for (int i = 0; i < db->file_id_counter; i++) {
        if (strcmp(db->files[i]->path, path) == 0) {
            file = db->files[i];
            break;
        }
    }

    if (!file) {
        uint64_t file_id = db->file_id_counter++;
        xnmm_alloc(xnfile_close, xnfile_create, &db->files[file_id], path, file_id, create, direct);
        file = db->files[file_id];
    }


    *out_file = file;
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
    db->dir_path = dir_path;
    db->rdtx_count = 0;
    db->committed_wrtx = NULL;
    db->tx_id_counter = 1;
    db->file_id_counter = 0;

    if (create) {
        xn_ensure(xn_mkdir(dir_path, 0700));
    }

    //log file uses file API directly rather than relying on container/heap/other persistent data structures
    struct xnfile *log_file;
    xn_ensure(xndb_get_file(db, &log_file, "log", create, true));
    xn_ensure(xnfile_set_size(log_file, 32 * XNPG_SZ));
    xnmm_alloc(xnlog_free, xnlog_create, &db->log, log_file, create);

    //need root page table initialized before transactions can be created
    xnmm_alloc(xntbl_free, xntbl_create, &db->pg_tbl, true);

    struct xntx *tx;
    xnmm_alloc(xntx_rollback, xntx_create, &tx, db, XNTXMODE_WR);

    //resource catalog
    struct xnfile *catalog_file;
    xn_ensure(xndb_get_file(db, &catalog_file, "catalog", create, false));
    xnmm_scoped_alloc(scoped_ptr, xnhp_free, xnhp_open, (struct xnhp**)&scoped_ptr, catalog_file, create, tx);
    struct xnhp *hp = (struct xnhp*)scoped_ptr;

    if (create) {
        //insert log file 
        {
            size_t path_size = strlen(log_file->path);
            size_t size = path_size + sizeof(uint64_t);
            xnmm_scoped_alloc(scoped_ptr, xn_free, xn_malloc, &scoped_ptr, size);
            uint8_t *buf = (uint8_t*)scoped_ptr;
            memcpy(buf, &log_file->id, sizeof(uint64_t));
            memcpy(buf + sizeof(uint64_t), log_file->path, path_size);

            struct xnitemid id;
            xn_ensure(xnhp_insert(hp, tx, buf, size, &id));
        }
        //insert catalog file
        {
            size_t path_size = strlen(catalog_file->path);
            size_t size = path_size + sizeof(uint64_t);
            xnmm_scoped_alloc(scoped_ptr, xn_free, xn_malloc, &scoped_ptr, size);
            uint8_t *buf = (uint8_t*)scoped_ptr;
            memcpy(buf, &catalog_file->id, sizeof(uint64_t));
            memcpy(buf + sizeof(uint64_t), catalog_file->path, path_size);

            struct xnitemid id;
            xn_ensure(xnhp_insert(hp, tx, buf, size, &id));
        }
    }

    //TODO load in all files into memory for quick access from array

    xn_ensure(xntx_commit(tx));

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
    xn_ensure(xntbl_free((void**)&db->pg_tbl));
    for (int i = 0; i < db->file_id_counter; i++) {
        xn_ensure(xnfile_close((void**)&db->files[i]));
    }
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
            uint64_t file_id = *((uint64_t*)buf);
            uint64_t pg_idx = *((uint64_t*)(buf + sizeof(uint64_t)));
            int off = *((int*)(buf + sizeof(uint64_t) * 2));

            struct xnpg page = { .file_handle = db->files[file_id], .idx = pg_idx };
            size_t data_hdr_size = sizeof(file_id) + sizeof(pg_idx) + sizeof(off);
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

xnresult_t xndb_open_resource(struct xndb *db, struct xnrs **rs, const char *filename, bool create, enum xnrst type, struct xntx *tx) {
    xnmm_init();
    switch (type) {
        case XNRST_HEAP: {
            struct xnfile *file;
            xn_ensure(xndb_get_file(db, &file, filename, create, false));
            struct xnhp *hp;
            xn_ensure(xnhp_open(&hp, file, create, tx));
            *rs = (struct xnrs*)hp;
            break;
        }
        default:
            xn_ensure(false);
            break;
    }

    return xn_ok();
}
