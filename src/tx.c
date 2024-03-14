#include "tx.h"
#include "log.h"
#include "db.h"

#include <stdint.h>
#include <stdlib.h>


pthread_cond_t disk_rdtxs_cv = PTHREAD_COND_INITIALIZER;
pthread_cond_t mem_rdtxs_cv = PTHREAD_COND_INITIALIZER;

xnresult_t xntx_create(struct xndb *db, enum xntxmode mode, struct xntx **out_tx) {
    xnmm_init();
    struct xntx *tx;
    xn_ensure(xn_malloc(sizeof(struct xntx), (void**)&tx));

    xn_ensure(xn_mutex_lock(db->tx_id_counter_lock));
    tx->id = db->tx_id_counter++;
    xn_ensure(xn_mutex_unlock(db->tx_id_counter_lock));

    tx->db = db;
    tx->rdtx_count = 0;
    xnmm_alloc(&tx->rdtx_count_lock, xn_ensure(xnmtx_create(&tx->rdtx_count_lock)), xnmtx_free);
    if (mode == XNTXMODE_WR) {
        xn_ensure(xn_mutex_lock(db->wrtx_lock));
        xn_ensure(xntbl_create(&tx->mod_pgs));

        size_t rec_size = xnlog_record_size(0);
        xnmm_scoped_alloc(scoped_ptr, xn_ensure(xn_malloc(rec_size, &scoped_ptr)), xn_free);
        uint8_t *rec = (uint8_t*)scoped_ptr;

        xn_ensure(xnlog_serialize_record(tx->id, XNLOGT_START, 0, NULL, rec));
        xn_ensure(xnlog_append(db->log, rec, rec_size));
    } else { //XNTXMODE_RD
        xn_ensure(xn_mutex_lock(db->committed_wrtx_lock));
        if (db->committed_wrtx) {
            tx->mod_pgs = db->committed_wrtx->mod_pgs;
            xn_ensure(xn_atomic_increment(&tx->rdtx_count, tx->rdtx_count_lock));
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

    struct xnpg page = {.file_handle = tx->db->file_handle, .idx = -1};
    for (int i = 0; i < XNTBL_MAX_BUCKETS; i++) {
        struct xnentry *cur = tx->mod_pgs->entries[i];
        while (cur) {
            page.idx = cur->pg_idx;
            //TODO should this use mmap and mprotect???
            xn_ensure(xnpg_flush(&page, cur->val));
            cur = cur->next;
        }
    }
    xn_ensure(xnfile_sync(tx->db->file_handle));

    return xn_ok();
}

static xnresult_t xntx_free(struct xntx *tx) {
    xnmm_init();
    if (tx->mode == XNTXMODE_RD) {
        if (tx->mod_pgs) {
            xn_ensure(xn_atomic_decrement_and_signal(&tx->rdtx_count, tx->rdtx_count_lock, &mem_rdtxs_cv));
        } else {
            xn_ensure(xn_atomic_decrement_and_signal(&tx->db->rdtx_count, tx->db->rdtx_count_lock, &disk_rdtxs_cv));
        }
        free(tx);
        return xn_ok();
    }

    //XNTXMODE_WR
    xn_ensure(xn_mutex_unlock(tx->db->wrtx_lock));
    xn_ensure(xntbl_free(tx->mod_pgs, false));
    xn_ensure(xnmtx_free((void**)&tx->rdtx_count_lock));
    free(tx);
    return xn_ok();
}

xnresult_t xntx_commit(struct xntx *tx) {
    xnmm_init();
    xn_ensure(tx->mode == XNTXMODE_WR);

    xn_ensure(xn_mutex_lock(tx->db->committed_wrtx_lock));

    size_t rec_size = xnlog_record_size(0);
    xnmm_scoped_alloc(scoped_ptr, xn_ensure(xn_malloc(rec_size, &scoped_ptr)), xn_free);
    uint8_t *rec = (uint8_t*)scoped_ptr;

    xn_ensure(xnlog_serialize_record(tx->id, XNLOGT_COMMIT, 0, NULL, rec));
    xn_ensure(xnlog_append(tx->db->log, rec, rec_size));
    tx->db->committed_wrtx = tx;

    xn_ensure(xn_mutex_unlock(tx->db->committed_wrtx_lock));

    //TODO remaining code should run asynchronously (eg, no more readers in next tbl)
  
    //TODO writing data to disk when 1. next table has no more readers (it will have one writer - this current tx itself.  But this tx is committed already) 
    xn_ensure(xn_wait_until_zero(&tx->db->rdtx_count, tx->db->rdtx_count_lock, &disk_rdtxs_cv));
    xn_ensure(xnlog_flush(tx->db->log));
    xn_ensure(xntx_flush_writes(tx));

    xn_ensure(xn_mutex_lock(tx->db->committed_wrtx_lock));
    tx->db->committed_wrtx = NULL;
    xn_ensure(xn_mutex_unlock(tx->db->committed_wrtx_lock));

    //TODO page table should be freed when no more readers and writer txs are reading from this page table

    //TODO freeing this table should only occur when 1. no more readers and 2. no more writer (up to one writer) referencing it
    xn_ensure(xn_wait_until_zero(&tx->rdtx_count, tx->rdtx_count_lock, &mem_rdtxs_cv));
    //TODO xntx_free is releasing the write lock - this can occur right after this tx commits (tx->db->committed_wrtx = tx)
    //If a new write tx starts, it will prevent this write transaction from being freed
    xn_ensure(xntx_free(tx));


    return xn_ok();
}

xnresult_t xntx_rollback(struct xntx *tx) {
    xnmm_init();
    xn_ensure(xntx_free(tx));

    return xn_ok();
}
