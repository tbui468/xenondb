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

    pthread_mutex_t rdtx_count_lock;
    int rdtx_count;
    
    int id;
};


xnresult_t xntx_create(struct xndb *db, enum xntxmode mode, struct xntx **out_tx);
xnresult_t xntx_flush_writes(struct xntx *tx);
xnresult_t xntx_free(struct xntx *tx);
xnresult_t xntx_commit(struct xntx *tx);
xnresult_t xntx_rollback(struct xntx *tx);
