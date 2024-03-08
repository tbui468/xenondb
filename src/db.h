#pragma once


#include "log.h"
#include "file.h"
#include "table.h"
#include "tx.h"

struct xndb {
    struct xnlog *log;
    pthread_mutex_t wrtx_lock;
    struct xnfile *file_handle;
    struct xntbl *pg_tbl;

    pthread_mutex_t committed_wrtx_lock;
    struct xntx *committed_wrtx;

    pthread_mutex_t rdtx_count_lock;
    int rdtx_count;

    pthread_mutex_t tx_id_counter_lock;
    int tx_id_counter;
};


xnresult_t xndb_create(const char *dir_path, struct xndb **out_db);
xnresult_t xndb_free(struct xndb *db);
xnresult_t xndb_redo(struct xndb *db, struct xntx *tx, uint64_t page_idx, int page_off, int tx_id);
xnresult_t xndb_recover(struct xndb *db);
