#pragma once


#include "log.h"
#include "file.h"
#include "table.h"
#include "tx.h"

struct xndb {
    struct xnlog *log;
    pthread_mutex_t *wrtx_lock;
    struct xntbl *pg_tbl;

    pthread_mutex_t *committed_wrtx_lock;
    struct xntx *committed_wrtx;

    pthread_mutex_t *rdtx_count_lock;
    int rdtx_count;

    pthread_mutex_t *tx_id_counter_lock;
    int tx_id_counter;
};


xnresult_t xndb_create(const char *dir_path, bool create, struct xndb **out_db);
xnresult_t xndb_free(struct xndb *db);
xnresult_t xndb_recover(struct xndb *db);
xnresult_t xndb_init_resource(struct xnfile *file, struct xntx *tx);
