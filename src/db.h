#pragma once


#include "log.h"
#include "file.h"
#include "table.h"
#include "tx.h"

struct xndb {
    const char* dir_path;
    struct xnlog *log;
    struct xnfile *files[32]; //TODO should not hard-code the masimum number of resources
    int file_id_counter;

    pthread_mutex_t *wrtx_lock;
    struct xntbl *pg_tbl;

    pthread_mutex_t *committed_wrtx_lock;
    struct xntx *committed_wrtx;

    pthread_mutex_t *rdtx_count_lock;
    int rdtx_count;

    pthread_mutex_t *tx_id_counter_lock;
    int tx_id_counter;
};

enum xnrst {
    XNRST_HEAP,
    //XNRST_BTREE,
    //XNRST_HASH,
    //XNRS_IVFFLAT
};

struct xnrs {
    enum xnrst type;
    union {
        struct xnhp *hp;
        //struct xnbtree
        //struct xnhash
        //struct xnivfflat
    } as;
};

xnresult_t xndb_create(const char *dir_path, bool create, struct xndb **out_db);
xnresult_t xndb_free(struct xndb *db);
xnresult_t xndb_recover(struct xndb *db);
xnresult_t xndb_init_resource(struct xnfile *file, struct xntx *tx);
xnresult_t xndb_open_resource(struct xndb *db, struct xnrs *out_rs, const char *filename, bool create, enum xnrst type, struct xntx *tx);
xnresult_t xndb_close_resource(struct xnrs rs);
