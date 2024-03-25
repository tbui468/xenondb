#pragma once


#include "log.h"
#include "file.h"
#include "table.h"
#include "tx.h"
#include "heap.h"

struct xndb {
    const char* dir_path;
    struct xnlog *log;
    int file_counter;
    struct xnfile *files[32];

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
    struct xnfile *file;
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
xnresult_t xnrs_open(struct xnrs *rs, struct xndb *db, const char *filename, bool create, enum xnrst type, struct xntx *tx);
xnresult_t xnrs_close(struct xnrs rs);
xnresult_t xnrs_put(struct xnrs rs, struct xntx *tx, size_t val_size, uint8_t *val, struct xnitemid *out_id);
