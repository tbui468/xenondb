#pragma once

#include "util.h"
#include "table.h"

enum xntxmode {
    XNTXMODE_RD,
    XNTXMODE_WR
};

struct xndb;

struct xntx {
    enum xntxmode mode;
    struct xntbl *mod_pgs;
    struct xndb *db;

    pthread_mutex_t *rdtx_count_lock;
    int rdtx_count;
    
    int id;
};


xnresult_t xntx_create(struct xntx **out_tx, struct xndb *db, enum xntxmode mode);
xnresult_t xntx_flush_writes(struct xntx *tx);
xnresult_t xntx_commit(struct xntx *tx);
xnresult_t xntx_rollback(void **t);
xnresult_t xntx_close(void **t);
