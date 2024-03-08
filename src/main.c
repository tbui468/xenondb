#define _GNU_SOURCE

#include "file.h"
#include "util.h"
#include "log.h"
#include "page.h"

//TODO these headers should be moved
#define XNPGID_METADATA 0
#define XNTBL_MAX_BUCKETS 128
#define XNLOG_BUF_SZ XNPG_SZ
#include <stdlib.h> //free
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>

pthread_cond_t disk_rdtxs_cv = PTHREAD_COND_INITIALIZER;
pthread_cond_t mem_rdtxs_cv = PTHREAD_COND_INITIALIZER;

struct xnentry {
    uint64_t pg_idx;
    uint8_t *val;
    struct xnentry *next;
};

struct xntbl {
    struct xnentry **entries;
    int count;
    int capacity;
};

xnresult_t xntbl_create(struct xntbl **out_tbl) {
    xnmm_init();
    struct xntbl *tbl;
    xn_ensure(xn_malloc(sizeof(struct xntbl), (void**)&tbl));
    xn_ensure(xn_malloc(sizeof(struct xnentry*) * XNTBL_MAX_BUCKETS, (void**)&tbl->entries));
    memset(tbl->entries, 0, sizeof(struct xnentry*) * XNTBL_MAX_BUCKETS);

    *out_tbl = tbl;
    return xn_ok();
}

xnresult_t xntbl_free(struct xntbl *tbl, bool unmap) {
    xnmm_init();
    for (int i = 0; i < XNTBL_MAX_BUCKETS; i++) {
        struct xnentry *cur = tbl->entries[i];
        while (cur) {
            struct xnentry *next = cur->next;
            if (unmap) {
                xn_ensure(xnpg_munmap(cur->val));
            } else {
                free(cur->val);
            }
            free(cur);
            cur = next;
        }
    }
    free(tbl->entries);
    free(tbl);

    return xn_ok();
}

uint8_t* xntbl_find(struct xntbl *tbl, struct xnpg *page) {
    uint32_t bucket = page->idx % XNTBL_MAX_BUCKETS;
    struct xnentry* cur = tbl->entries[bucket];

    while (cur) {
        if (cur->pg_idx == page->idx)
            return cur->val;

        cur = cur->next;
    }

    return NULL;
}

xnresult_t xntbl_insert(struct xntbl *tbl, struct xnpg *page, uint8_t *val) {
    xnmm_init();
    uint32_t bucket = page->idx % XNTBL_MAX_BUCKETS;
    struct xnentry* cur = tbl->entries[bucket];

    while (cur) {
        if (cur->pg_idx == page->idx) {
            cur->val = val;
            return true;
        }
            
        cur = cur->next;
    }

    //insert at beginning of linked-list
    struct xnentry* head = tbl->entries[bucket];
    struct xnentry* entry;
    xn_ensure(xn_malloc(sizeof(struct xnentry), (void**)&entry));
    entry->next = head;
    entry->pg_idx = page->idx;
    entry->val = val;
    tbl->entries[bucket] = entry;

    return xn_ok();
}

enum xntxmode {
    XNTXMODE_RD,
    XNTXMODE_WR
};

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





xnresult_t xndb_recover(struct xndb *db);

xnresult_t xndb_create(const char *dir_path, struct xndb **out_db) {
    xnmm_init();
    struct xndb *db;
    xn_ensure(xn_malloc(sizeof(struct xndb), (void**)&db));
    xn_ensure(xn_mutex_init(&db->wrtx_lock));
    xn_ensure(xn_mutex_init(&db->rdtx_count_lock));
    xn_ensure(xn_mutex_init(&db->committed_wrtx_lock));

    xn_ensure(xnlog_create("log", &db->log));

    xn_ensure(xnfile_create(dir_path, false, &db->file_handle));
    xn_ensure(xnfile_set_size(db->file_handle, XNPG_SZ * 32));
    uint8_t buf;
    xn_ensure(xnfile_read(db->file_handle, &buf, 0, sizeof(uint8_t)));
    uint8_t hdr_bit = 1;
    buf |= hdr_bit;
    xn_ensure(xnfile_write(db->file_handle, &buf, 0, sizeof(uint8_t)));
    xn_ensure(xnfile_sync(db->file_handle));

    xn_ensure(xntbl_create(&db->pg_tbl));

    db->committed_wrtx = NULL;
    db->rdtx_count = 0;

    xn_ensure(xn_mutex_init(&db->tx_id_counter_lock));
    db->tx_id_counter = 1;

    xn_ensure(xndb_recover(db));

   *out_db = db;
    return xn_ok();
}

xnresult_t xndb_free(struct xndb *db) {
    xnmm_init();
    xn_ensure(xn_mutex_destroy(&db->wrtx_lock));
    xn_ensure(xn_mutex_destroy(&db->rdtx_count_lock));
    xn_ensure(xn_mutex_destroy(&db->committed_wrtx_lock));
    xn_ensure(xn_mutex_destroy(&db->tx_id_counter_lock));
    xn_ensure(xnfile_close(db->file_handle));
    xn_ensure(xntbl_free(db->pg_tbl, true));
    free(db);
    return xn_ok();
}


//TODO move to own files later
struct xntx {
    enum xntxmode mode;
    struct xntbl *mod_pgs;
    struct xndb *db;

    pthread_mutex_t rdtx_count_lock;
    int rdtx_count;
    
    int id;
};

xnresult_t xntx_create(struct xndb *db, enum xntxmode mode, struct xntx **out_tx) {
    xnmm_init();
    struct xntx *tx;
    xn_ensure(xn_malloc(sizeof(struct xntx), (void**)&tx));

    xn_ensure(xn_mutex_lock(&db->tx_id_counter_lock));
    tx->id = db->tx_id_counter++;
    xn_ensure(xn_mutex_unlock(&db->tx_id_counter_lock));

    tx->db = db;
    tx->rdtx_count = 0;
    xn_ensure(xn_mutex_init(&tx->rdtx_count_lock));
    if (mode == XNTXMODE_WR) {
        xn_ensure(xn_mutex_lock(&db->wrtx_lock));
        xn_ensure(xntbl_create(&tx->mod_pgs));

        size_t rec_size = xnlog_record_size(0);
        uint8_t *rec;
        xn_ensure(xn_malloc(rec_size, (void**)&rec));
        xnmm_defer(rec);

        xn_ensure(xnlog_serialize_record(tx->id, XNLOGT_START, 0, NULL, rec));
        xn_ensure(xnlog_append(db->log, rec, rec_size));
    } else {
        xn_ensure(xn_mutex_lock(&db->committed_wrtx_lock));
        if (db->committed_wrtx) {
            tx->mod_pgs = db->committed_wrtx->mod_pgs;
            xn_ensure(xn_atomic_increment(&tx->rdtx_count, &tx->rdtx_count_lock));
        } else {
            tx->mod_pgs = NULL;
            xn_ensure(xn_atomic_increment(&db->rdtx_count, &db->rdtx_count_lock));
        }
        xn_ensure(xn_mutex_unlock(&db->committed_wrtx_lock));
    }
    tx->mode = mode;
    *out_tx = tx;
    return xn_ok();
}

xnresult_t xn_wait_until_zero(int *count, pthread_mutex_t *lock, pthread_cond_t *cv) {
    xnmm_init();
    xn_ensure(xn_mutex_lock(lock));
    while (*count > 0) {
        xn_ensure(xn_cond_wait(cv, lock));
    }
    xn_ensure(xn_mutex_unlock(lock));

    return xn_ok();
}

xnresult_t xntx_flush_writes(struct xntx *tx) {
    xnmm_init();
    assert(tx->mode == XNTXMODE_WR);

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

xnresult_t xntx_free(struct xntx *tx) {
    xnmm_init();
    if (tx->mode == XNTXMODE_RD) {
        if (tx->mod_pgs) {
            xn_ensure(xn_atomic_decrement_and_signal(&tx->rdtx_count, &tx->rdtx_count_lock, &mem_rdtxs_cv));
        } else {
            xn_ensure(xn_atomic_decrement_and_signal(&tx->db->rdtx_count, &tx->db->rdtx_count_lock, &disk_rdtxs_cv));
        }
        free(tx);
        return xn_ok();
    }

    //XNTXMODE_WR
    xn_ensure(xn_mutex_unlock(&tx->db->wrtx_lock));
    xn_ensure(xntbl_free(tx->mod_pgs, false));
    xn_ensure(xn_mutex_destroy(&tx->rdtx_count_lock));
    free(tx);
    return xn_ok();
}

xnresult_t xntx_commit(struct xntx *tx) {
    xnmm_init();
    assert(tx->mode == XNTXMODE_WR);

    xn_ensure(xn_mutex_lock(&tx->db->committed_wrtx_lock));

    size_t rec_size = xnlog_record_size(0);
    uint8_t *rec;
    xn_ensure(xn_malloc(rec_size, (void**)&rec));
    xnmm_defer(rec);

    xn_ensure(xnlog_serialize_record(tx->id, XNLOGT_COMMIT, 0, NULL, rec));
    xn_ensure(xnlog_append(tx->db->log, rec, rec_size));
    tx->db->committed_wrtx = tx;

    xn_ensure(xn_mutex_unlock(&tx->db->committed_wrtx_lock));

    //TODO remaining code should run asynchronously (eg, no more readers in next tbl)
  
    //TODO writing data to disk when 1. next table has no more readers (it will have one writer - this current tx itself.  But this tx is committed already) 
    xn_ensure(xn_wait_until_zero(&tx->db->rdtx_count, &tx->db->rdtx_count_lock, &disk_rdtxs_cv));
    xn_ensure(xnlog_flush(tx->db->log));
    xn_ensure(xntx_flush_writes(tx));

    xn_ensure(xn_mutex_lock(&tx->db->committed_wrtx_lock));
    tx->db->committed_wrtx = NULL;
    xn_ensure(xn_mutex_unlock(&tx->db->committed_wrtx_lock));

    //TODO page table should be freed when no more readers and writer txs are reading from this page table

    //TODO freeing this table should only occur when 1. no more readers and 2. no more writer (up to one writer) referencing it
    xn_ensure(xn_wait_until_zero(&tx->rdtx_count, &tx->rdtx_count_lock, &mem_rdtxs_cv));
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

xnresult_t xnpg_write(struct xnpg *page, struct xntx *tx, const uint8_t *buf, int offset, size_t size, bool log) {
    xnmm_init();
    assert(tx->mode == XNTXMODE_WR);

    uint8_t *cpy;

    if (!(cpy = xntbl_find(tx->mod_pgs, page))) {
        xn_ensure(xn_malloc(XNPG_SZ, (void**)&cpy));
        xn_ensure(xnpg_copy(page, cpy));
        xn_ensure(xntbl_insert(tx->mod_pgs, page, cpy));
    }

    memcpy(cpy + offset, buf, size);

    if (log) {
        uint8_t *update_data;
        size_t data_size = size + sizeof(uint64_t) + sizeof(int);; //including page index and offset
        xn_ensure(xn_malloc(data_size, (void**)&update_data));
        xnmm_defer(update_data);
        memcpy(update_data, &page->idx, sizeof(uint64_t));
        memcpy(update_data + sizeof(uint64_t), &offset, sizeof(int));
        memcpy(update_data + sizeof(uint64_t) + sizeof(int), buf, size);

        uint8_t *rec;
        size_t rec_size = xnlog_record_size(data_size);
        xn_ensure(xn_malloc(rec_size, (void**)&rec));
        xnmm_defer(rec);
        xn_ensure(xnlog_serialize_record(tx->id, XNLOGT_UPDATE, data_size, update_data, rec));
        xn_ensure(xnlog_append(tx->db->log, rec, rec_size));
    }

    return xn_ok();
}

xnresult_t xnpg_read(struct xnpg *page, struct xntx *tx, uint8_t *buf, int offset, size_t size) {
    xnmm_init();
    if (tx->mode == XNTXMODE_WR || tx->mod_pgs) {
        uint8_t *cpy;

        if ((cpy = xntbl_find(tx->mod_pgs, page))) {
            memcpy(buf, cpy + offset, size);
            return xn_ok();
        }
    }

    uint8_t *ptr;
    if (!(ptr = xntbl_find(tx->db->pg_tbl, page))) {
        xn_ensure(xnpg_mmap(page, &ptr));
        xn_ensure(xntbl_insert(tx->db->pg_tbl, page, ptr));
    }

    memcpy(buf, ptr + offset, size);

    return xn_ok();
}

xnresult_t xnpgr_find_free_page(struct xnpg *meta_page, struct xntx *tx, struct xnpg *new_page) {
    xnmm_init();
    int page_count = tx->db->file_handle->size / XNPG_SZ;
    int i, j;
    for (i = 0; i < page_count; i++) {
        uint8_t byte;
        xn_ensure(xnpg_read(meta_page, tx, &byte, i * sizeof(uint8_t), sizeof(uint8_t)));
        for (j = 0; j < 8; j++) {
            uint8_t mask = 1 << j;
            uint8_t bit = (mask & byte) >> j;

            if (bit == 0)
                goto found_free_bit;
        }
    }

    //TODO should this be xn_ensure(false)?
    return false;

found_free_bit:
    new_page->file_handle = meta_page->file_handle;
    new_page->idx = i * 8 + j;
    return xn_ok();
}

int xnpgr_bitmap_byte_offset(uint64_t page_idx) {
    return page_idx / 8;
}

xnresult_t xnpgr_free_page(struct xnpg *meta_page, struct xntx *tx, struct xnpg page) {
    xnmm_init();

    //set bit to 'free'
    uint8_t byte;
    xn_ensure(xnpg_read(meta_page, tx, &byte, xnpgr_bitmap_byte_offset(page.idx), sizeof(uint8_t)));
    byte &= ~(1 << (page.idx % 8));
    xn_ensure(xnpg_write(meta_page, tx, &byte, xnpgr_bitmap_byte_offset(page.idx), sizeof(uint8_t), true));
    return xn_ok();
}

xnresult_t xnpgr_allocate_page(struct xnpg *meta_page, struct xntx *tx, struct xnpg *page) {
    xnmm_init();

    xn_ensure(xnpgr_find_free_page(meta_page, tx, page));

    //zero out new page data
    uint8_t *buf;
    xn_ensure(xn_malloc(XNPG_SZ, (void**)&buf));
    xnmm_defer(buf);

    memset(buf, 0, XNPG_SZ);
    xn_ensure(xnpg_write(page, tx, buf, 0, XNPG_SZ, true));

    //set bit to 'used'
    uint8_t byte;
    xn_ensure(xnpg_read(meta_page, tx, &byte, xnpgr_bitmap_byte_offset(page->idx), sizeof(uint8_t)));
    byte |= 1 << (page->idx % 8);
    xn_ensure(xnpg_write(meta_page, tx, &byte, xnpgr_bitmap_byte_offset(page->idx), sizeof(uint8_t), true));

    return xn_ok();
}

xnresult_t xndb_redo(struct xndb *db, struct xntx *tx, uint64_t page_idx, int page_off, int tx_id) {
    xnmm_init();
    struct xnlogitr *itr;
    xn_ensure(xnlogitr_create(db->log, &itr));
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
            uint8_t *buf;
            xn_ensure(xn_malloc(data_size, (void**)&buf));
            xnmm_defer(buf);
            xn_ensure(xnlogitr_read_data(itr, buf, data_size));

            //writing changes back to file (not the log)
            struct xnpg page = { .file_handle = tx->db->file_handle, .idx = *((uint64_t*)buf) };
            size_t data_hdr_size = sizeof(uint64_t) + sizeof(int);
            int off = *((int*)(buf + sizeof(uint64_t)));
            size_t size = data_size - data_hdr_size;
            printf("redoing tx: %d, size: %ld, page idx: %ld, page off: %d\n", tx_id, size, page.idx, off);
            xn_ensure(xnpg_write(&page, tx, buf + data_hdr_size, off, size, false));
        }
    }
    xn_ensure(xnlogitr_free(itr));
    return xn_ok();
}

xnresult_t xndb_recover(struct xndb *db) {
    xnmm_init();
    struct xnlogitr *itr;
    xn_ensure(xnlogitr_create(db->log, &itr));
    struct xntx *tx;
    xn_ensure(xntx_create(db, XNTXMODE_WR, &tx));

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
            printf("setting start tx: %d\n", tx_id);
            start_pageidx = itr->page.idx;
            start_pageoff = itr->page_off;
            start_txid = tx_id;
        } else if (type == XNLOGT_COMMIT && start_txid == tx_id) {
            printf("redoing tx: %d\n", tx_id);
            xn_ensure(xndb_redo(db, tx, start_pageidx, start_pageoff, start_txid));
        }
    }

    xn_ensure(xnlogitr_free(itr));
    xn_ensure(xntx_flush_writes(tx));
    xn_ensure(xntx_commit(tx));
    return xn_ok();
}


struct data {
    struct xndb *db;
    int i;
};

void *fcn(void *arg) {
    struct data* d = (struct data*)arg;
    int i = d->i;
    struct xndb *db = d->db;
    free(arg);

    if (i % 2 == 0) {
        struct xntx *tx;
        if (!xntx_create(db, XNTXMODE_WR, &tx))
            printf("failed\n");
        struct xnpg page;

        struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = XNPGID_METADATA};
        if (!xnpgr_allocate_page(&meta_page, tx, &page))
            printf("failed\n");
        if (!xntx_commit(tx))
            printf("failed\n");
    } else {
        struct xntx *tx;
        if (!xntx_create(db, XNTXMODE_RD, &tx))
            printf("failed\n");
        struct xnpg page = { .file_handle = tx->db->file_handle, .idx = 0 };

        uint8_t *buf = malloc(XNPG_SZ);
        if (!xnpg_read(&page, tx, buf, 0, XNPG_SZ))
            printf("failed\n");
        printf("pages: %d\n", *buf);
        free(buf);
        if (!xntx_free(tx))
            printf("failed\n");
    }
}

int main(int argc, char** argv) {
    struct xndb *db;
    if (!xndb_create("students", &db)) {
        printf("xndb_create failed\n");
        exit(1);
    }
    /*
    //multi-threaded test
    const int THREAD_COUNT = 24;
    pthread_t threads[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; i++) {
        struct data *p = malloc(sizeof(struct data));
        p->db = db;
        p->i = i;
        pthread_create(&threads[i], NULL, fcn, p);
    }

    for (int i = 0; i < THREAD_COUNT; i++) {
        pthread_join(threads[i], NULL);
    }*/

    //single threaded test
    {
        struct xntx *tx;
        if (!xntx_create(db, XNTXMODE_WR, &tx)) {
            printf("xntx_create failed\n");
            exit(1);
        }
        struct xnpg page;
        struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = XNPGID_METADATA};
        if (!xnpgr_allocate_page(&meta_page, tx, &page)) {
            printf("xntx_allocate_page failed\n");
            exit(1);
        }
        if (!xnpgr_free_page(&meta_page, tx, page)) {
            printf("xntx_free_page failed\n");
            exit(1);
        }
        if (!xnpgr_allocate_page(&meta_page, tx, &page)) {
            printf("xntx_allocate_page failed\n");
            exit(1);
        }
        if (!xntx_commit(tx)) {
            printf("xntx_commit failed\n");
            exit(1);
        }
    }

    /*
    { 
        struct xntx *tx;
        if (!xntx_create(db, XNTXMODE_WR, &tx))
            printf("failed\n");
        struct xnpg page;
        if (!xntx_allocate_page(tx, &page))
            printf("failed\n");
        if (!xntx_commit(tx))
            printf("failed\n");
    }*/

    if (!xndb_free(db)) {
        printf("xndb_free failed\n");
        exit(1);
    }
    return 0;
}
