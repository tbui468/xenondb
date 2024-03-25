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
    xndb_make_path(path, db->dir_path, filename);
    struct xnfile *file = NULL;

    for (int i = 0; i < db->file_counter; i++) {
        if (strcmp(db->files[i]->path, path) == 0) {
            file = db->files[i];
            break;
        }
    }

    if (!file) {
        int idx = db->file_counter++;
        xnmm_alloc(xnfile_close, xnfile_create, &db->files[idx], path, 0, create, direct);
        file = db->files[idx];
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
    db->rdtx_count = 0;
    db->committed_wrtx = NULL;
    db->tx_id_counter = 1;
    db->file_counter = 0;

    if (create) {
        xn_ensure(xn_mkdir(dir_path, 0700));
    }

    char abs_path[PATH_MAX];
    xn_ensure(xn_realpath(dir_path, abs_path));
    db->dir_path = strdup(abs_path);

    struct xnfile *log_file;
    xn_ensure(xndb_get_file(db, &log_file, "log", create, true));
    xn_ensure(xnfile_set_size(log_file, 32 * XNPG_SZ));
    xnmm_alloc(xnlog_free, xnlog_create, &db->log, log_file, create);

    if (create) {
        xnmm_scoped_alloc(scoped_ptr, xn_free, xn_aligned_malloc, &scoped_ptr, XNPG_SZ);
        uint8_t *buf = (uint8_t*)scoped_ptr;
        memset(buf, 0, XNPG_SZ);
        for (int i = 0; i < 32; i++) {
            xn_ensure(xnfile_write(log_file, buf, i * XNPG_SZ, XNPG_SZ));
        }
    }

    //need root page table initialized before transactions can be created
    xnmm_alloc(xntbl_free, xntbl_create, &db->pg_tbl, true);

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
    for (int i = 0; i < db->file_counter; i++) {
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
            uint64_t path_size = *((uint64_t*)buf);
            char path[path_size + 1];
            memcpy(path, buf + sizeof(uint64_t), path_size);
            path[path_size] = '\0';
            uint64_t pg_idx = *((uint64_t*)(buf + sizeof(uint64_t) + path_size));
            int off = *((int*)(buf + sizeof(uint64_t) * 2 + path_size));

            xnmm_scoped_alloc(scoped_ptr2, xnfile_close, xnfile_create, (struct xnfile**)&scoped_ptr2, path, 0, false, false);
            struct xnfile* file = (struct xnfile*)scoped_ptr2;

            //TODO open file here
            struct xnpg page = { .file_handle = file, .idx = pg_idx };
            size_t data_hdr_size = sizeof(path_size) + path_size + sizeof(pg_idx) + sizeof(off);
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

xnresult_t xnrs_open(struct xnrs *rs, struct xndb *db, const char *filename, bool create, enum xnrst type, struct xntx *tx) {
    xnmm_init();

    rs->type = type;
    switch (type) {
        case XNRST_HEAP: {
            xn_ensure(xndb_get_file(db, &rs->file, filename, create, false));
            xnmm_alloc(xnhp_free, xnhp_open, (struct xnhp**)&rs->as.hp, rs->file, create, tx);
            break;
        }
        default:
            xn_ensure(false);
            break;
    }

    return xn_ok();
}

xnresult_t xnrs_close(struct xnrs rs) {
    xnmm_init();
    switch (rs.type) {
        case XNRST_HEAP:
            xnhp_free((void**)&rs.as.hp);
            break;
        default:
            xn_ensure(false);
            break;
    }

    return xn_ok();
}

xnresult_t xnrs_put(struct xnrs rs, struct xntx *tx, size_t val_size, uint8_t *val, struct xnitemid *out_id) {
    xnmm_init();

    switch (rs.type) {
        case XNRST_HEAP: {
            xn_ensure(xnhp_insert(rs.as.hp, tx, val, val_size, out_id));
            break;
        }
        default:
            xn_ensure(false);
            break;
    }

    return xn_ok();
}
