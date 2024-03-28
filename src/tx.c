#include "tx.h"
#include "log.h"
#include "db.h"

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>


pthread_cond_t disk_rdtxs_cv = PTHREAD_COND_INITIALIZER;
pthread_cond_t mem_rdtxs_cv = PTHREAD_COND_INITIALIZER;

xnresult_t xntx_create(struct xntx **out_tx, struct xndb *db, enum xntxmode mode) {
    xnmm_init();
    struct xntx *tx;
    xnmm_alloc(xn_free, xn_malloc, (void**)&tx, sizeof(struct xntx));

    xn_ensure(xn_mutex_lock(db->tx_id_counter_lock));
    tx->id = db->tx_id_counter++;
    xn_ensure(xn_mutex_unlock(db->tx_id_counter_lock));

    tx->db = db;
    tx->rdtx_count = 0;
    xnmm_alloc(xnmtx_free, xnmtx_create, &tx->rdtx_count_lock);
    if (mode == XNTXMODE_WR) {
        xn_ensure(xn_mutex_lock(db->wrtx_lock)); //TODO need to unlock if function fails - putting responsibility on caller is confusing and to complex
        xnmm_alloc(xntbl_free, xntbl_create, &tx->mod_pgs, false);

        size_t rec_size = xnlog_record_size(0);
        xnmm_scoped_alloc(scoped_ptr, xn_free, xn_malloc, &scoped_ptr, rec_size);
        uint8_t *rec = (uint8_t*)scoped_ptr;

        xn_ensure(xnlog_serialize_record(tx->id, XNLOGT_START, 0, NULL, rec));
        xn_ensure(xnlog_append(db->log, rec, rec_size));
    } else { //XNTXMODE_RD
        xn_ensure(xn_mutex_lock(db->committed_wrtx_lock)); //TODO need to unlock if function fails
        if (db->committed_wrtx) {
            tx->mod_pgs = db->committed_wrtx->mod_pgs;
            xn_ensure(xn_atomic_increment(&db->committed_wrtx->rdtx_count, db->committed_wrtx->rdtx_count_lock));
        } else {
            tx->mod_pgs = NULL;
            xn_ensure(xn_atomic_increment(&db->rdtx_count, db->rdtx_count_lock));
        }
        xn_ensure(xn_mutex_unlock(db->committed_wrtx_lock));
    }
    tx->mode = mode;
    *out_tx = tx;
    return xn_ok();
}

xnresult_t xntx_flush_writes(struct xntx *tx) {
    xnmm_init();
    xn_ensure(tx->mode == XNTXMODE_WR);

    //struct xnpg page = {.file_handle = tx->db->file_handle, .idx = -1};
    for (int i = 0; i < XNTBL_MAX_BUCKETS; i++) {
        struct xnentry *cur = tx->mod_pgs->entries[i];
        while (cur) {
            //page.idx = cur->page.idx;
            //TODO should this use mmap and mprotect???
            xn_ensure(xnpg_flush(&cur->page, cur->val));
            cur = cur->next;
        }
    }
    //TODO need to sync ALL files that were modified
    //xn_ensure(xnfile_sync(tx->db->file_handle));

    return xn_ok();
}

//called by xntx_commit and xntx_rollback to free write txs
static xnresult_t xntx_free(struct xntx *tx) {
    xnmm_init();

    xn_ensure(tx->mode == XNTXMODE_WR);

    xn_ensure(xntbl_free((void**)&tx->mod_pgs));
    xn_ensure(xnmtx_free((void**)&tx->rdtx_count_lock));
    free(tx);

    return xn_ok();
}

//close and free read txs
xnresult_t xntx_close(void **t) {
    xnmm_init();
    struct xntx *tx = (struct xntx*)(*t);
    assert(tx->mode == XNTXMODE_RD);

    if (tx->mod_pgs) {
        xn_ensure(xn_atomic_decrement_and_signal(&tx->db->committed_wrtx->rdtx_count, tx->db->committed_wrtx->rdtx_count_lock, &mem_rdtxs_cv));
    } else {
        xn_ensure(xn_atomic_decrement_and_signal(&tx->db->rdtx_count, tx->db->rdtx_count_lock, &disk_rdtxs_cv));
    }
    free(tx);
    return xn_ok();
}

xnresult_t xntx_commit(struct xntx *tx) {
    xnmm_init();
    assert(tx->mode == XNTXMODE_WR);

    //append commit log record and flush log
    {
        xn_ensure(xn_mutex_lock(tx->db->committed_wrtx_lock)); //TODO need to unlock if function fails

        size_t rec_size = xnlog_record_size(0);
        xnmm_scoped_alloc(scoped_ptr, xn_free, xn_malloc, &scoped_ptr, rec_size);
        uint8_t *rec = (uint8_t*)scoped_ptr;

        xn_ensure(xnlog_serialize_record(tx->id, XNLOGT_COMMIT, 0, NULL, rec));
        xn_ensure(xnlog_append(tx->db->log, rec, rec_size));
        tx->db->committed_wrtx = tx;
        xn_ensure(xnlog_flush(tx->db->log));

        xn_ensure(xn_mutex_unlock(tx->db->committed_wrtx_lock));
    }

    //Single-writer mutex can be unlocked here to improve concurrency, but will make handling overlapping writer txs a bit more complex.
    //Only a single writer can be modifying data at a time, but this allows writers in the commit/flushed stages to not interfere with
    //write tx in the writing stage.  Reader txs handling is also more complex since they will reference the most recently committed
    //writer tx snapshot.
    //write tx stages: [creating][writing][committed][flushed][gc]
    //Only the 'writing' stage allows a single write tx - all stages before and after can have multiple write txs active.
  
    //write to disk when there are no more downstream reader txs
    {
        xn_ensure(xn_wait_until_zero(&tx->db->rdtx_count, tx->db->rdtx_count_lock, &disk_rdtxs_cv));
        xn_ensure(xntx_flush_writes(tx));
    }

    //free tx when there are no more upstream readers, and any upstream writer has committed
    {
        xn_ensure(xn_wait_until_zero(&tx->rdtx_count, tx->rdtx_count_lock, &mem_rdtxs_cv));
        xn_ensure(xn_mutex_lock(tx->db->committed_wrtx_lock)); //TODO need to unlock if function fails
        tx->db->committed_wrtx = NULL;
        xn_ensure(xn_mutex_unlock(tx->db->committed_wrtx_lock));

        //unlocking single-writer mutex here to keep things simple for now
        xn_ensure(xn_mutex_unlock(tx->db->wrtx_lock));
        xn_ensure(xntx_free(tx));
    }

    return xn_ok();
}

xnresult_t xntx_rollback(void **t) {
    xnmm_init();
    struct xntx *tx = (struct xntx*)(*t);
    assert(tx->mode == XNTXMODE_WR);
    xn_ensure(xn_mutex_unlock(tx->db->wrtx_lock));
    xn_ensure(xntx_free(tx));

    return xn_ok();
}
