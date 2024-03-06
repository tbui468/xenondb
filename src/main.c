#define _GNU_SOURCE

//TODO these headers should be moved
#define XNPGID_METADATA 0
#define XNTBL_MAX_BUCKETS 128
#define XNPG_SZ 4096
#define XNLOG_BUF_SZ XNPG_SZ
#include <stdlib.h> //free
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/mman.h>

#include "common.h"
#include "file.h"

pthread_cond_t disk_rdtxs_cv = PTHREAD_COND_INITIALIZER;
pthread_cond_t mem_rdtxs_cv = PTHREAD_COND_INITIALIZER;

struct xnpg {
    struct xnfile *file_handle;
    uint64_t idx;
};

__attribute__((warn_unused_result)) bool xnpg_write(struct xnpg *page, const uint8_t *buf) {
    xn_ensure(xnfile_write(page->file_handle, buf, page->idx * XNPG_SZ, XNPG_SZ)); 
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xnpg_read(struct xnpg *page, uint8_t *buf) {
    xn_ensure(xnfile_read(page->file_handle, buf, page->idx * XNPG_SZ, XNPG_SZ));
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xnpg_mmap(struct xnpg *page, uint8_t **ptr) {
    xn_ensure(xnfile_mmap(0, XNPG_SZ, MAP_SHARED, PROT_READ, page->file_handle->fd, page->idx * XNPG_SZ, (void**)ptr));
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xnpg_munmap(uint8_t *ptr) {
    xn_ensure(xnfile_munmap((void*)ptr, XNPG_SZ));
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xn_atomic_increment(int *i, pthread_mutex_t *lock) {
    xn_ensure(pthread_mutex_lock(lock) == 0);
    (*i)++;
    xn_ensure(pthread_mutex_unlock(lock) == 0);
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xn_atomic_decrement_and_signal(int *i, pthread_mutex_t *lock, pthread_cond_t *cv) {
    xn_ensure(pthread_mutex_lock(lock) == 0);
    (*i)--;
    if (*i == 0)
        xn_ensure(pthread_cond_signal(cv) == 0);
    xn_ensure(pthread_mutex_unlock(lock) == 0);
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xn_atomic_decrement(int *i, pthread_mutex_t *lock) {
    xn_ensure(pthread_mutex_lock(lock) == 0);
    (*i)--;
    xn_ensure(pthread_mutex_unlock(lock) == 0);
    return xn_ok();
}

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

__attribute__((warn_unused_result)) bool xntbl_create(struct xntbl **out_tbl) {
    struct xntbl *tbl;
    xn_ensure((tbl = malloc(sizeof(struct xntbl))) != NULL);
    xn_ensure((tbl->entries = malloc(sizeof(struct xnentry*) * XNTBL_MAX_BUCKETS)) != NULL);
    memset(tbl->entries, 0, sizeof(struct xnentry*) * XNTBL_MAX_BUCKETS);

    *out_tbl = tbl;
    return true;
}

__attribute__((warn_unused_result)) bool xntbl_free(struct xntbl *tbl, bool unmap) {
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

    return true;
}

//hash function from 'Crafting Interpreters'
static uint32_t xn_hash(const uint8_t *buf, int length) {
    uint32_t hash = 2166136261u;
    for (int i = 0; i < length; i++) {
        hash ^= buf[i];
        hash *= 16777619;
    }
    return hash;
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

__attribute__((warn_unused_result)) bool xntbl_insert(struct xntbl *tbl, struct xnpg *page, uint8_t *val) {
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
    xn_ensure((entry = malloc(sizeof(struct xnentry))) != NULL);
    entry->next = head;
    entry->pg_idx = page->idx;
    entry->val = val;
    tbl->entries[bucket] = entry;

    return true;
}


static void xn_cleanup_free(void *p) {
    free(*(void**) p);
}

enum xntxmode {
    XNTXMODE_RD,
    XNTXMODE_WR
};

enum xnlogt {
    XNLOGT_START,
    XNLOGT_UPDATE,
    XNLOGT_COMMIT
};

struct xnlog {
    uint8_t *buf;
    int page_off;

    int highest_tx_flushed;
    struct xnpg page;
};

struct xnlogitr {
    struct xnpg page;
    int page_off;
    uint8_t *buf;
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

__attribute__((warn_unused_result)) bool xnlogitr_create(struct xnlog *log, struct xnlogitr **out_itr) {
    struct xnlogitr *itr;
    xn_ensure(xn_malloc(sizeof(struct xnlogitr), (void**)&itr));
    xn_ensure(xn_aligned_malloc(XNPG_SZ, (void**)&itr->buf));
    itr->page = log->page;
    itr->page.idx = 0;
    itr->page_off = -1;
    xn_ensure(xnpg_read(&itr->page, itr->buf));

    *out_itr = itr;
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xnlogitr_seek(struct xnlogitr *itr, uint64_t page_idx, int page_off) {
    itr->page.idx = page_idx;
    itr->page_off = page_off;
    xn_ensure(xnpg_read(&itr->page, itr->buf));
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xnlogitr_read_span(const struct xnlogitr *itr, uint8_t *buf, off_t off, size_t size) {
    struct xnpg page = itr->page;
    int page_off = itr->page_off;
    uint8_t *page_buf;
    xn_ensure(xn_aligned_malloc(XNPG_SZ, (void**)&page_buf));
    xn_ensure(xnpg_read(&page, page_buf));

    size_t nread = 0;
    while (nread < size) {
        size_t to_read = size - nread;
        size_t remaining = XNPG_SZ - page_off;
        size_t s = to_read < remaining ? to_read : remaining;
        memcpy(buf + nread, page_buf + page_off, s);

        nread += s;
        page_off += s;

        if (page_off >= XNPG_SZ) {
            page_off = 0;
            page.idx++;
            xn_ensure(xnpg_read(&page, page_buf));
        }
    }

    free(page_buf);

    return xn_ok();
}

__attribute__((warn_unused_result)) bool xnlogitr_read_data(struct xnlogitr *itr, uint8_t *buf, size_t size) {
    const off_t header_size = sizeof(int) + sizeof(enum xnlogt) + sizeof(size_t);
    xn_ensure(xnlogitr_read_span(itr, buf, header_size, size));

    return xn_ok();
}

__attribute__((warn_unused_result)) bool xnlogitr_read_header(const struct xnlogitr *itr, int *tx_id, enum xnlogt *type, size_t *data_size) {
    const size_t header_size = sizeof(int) + sizeof(enum xnlogt) + sizeof(size_t);
    uint8_t hdr_buf[header_size];

    xn_ensure(xnlogitr_read_span(itr, hdr_buf, 0, header_size));

    *tx_id = *((int*)hdr_buf);
    *type = *((enum xnlogt*)(hdr_buf + sizeof(int)));
    *data_size = *((size_t*)(hdr_buf + sizeof(int) + sizeof(enum xnlogt)));

    return xn_ok();
}

size_t xnlog_record_size(size_t data_size);

bool xnlogitr_next(struct xnlogitr *itr) {
    if (itr->page_off == -1) {
        itr->page_off = 0;
    } else {
        int tx_id;
        enum xnlogt type;
        size_t data_size;
        xn_ensure(xnlogitr_read_header(itr, &tx_id, &type, &data_size));

        itr->page_off += xnlog_record_size(data_size);
        while (itr->page_off >= XNPG_SZ) {
            itr->page_off -= XNPG_SZ;
            itr->page.idx++;
        }
        xn_ensure(xnpg_read(&itr->page, itr->buf));
    }

    int tx_id;
    enum xnlogt type;
    size_t data_size;
    xn_ensure(xnlogitr_read_header(itr, &tx_id, &type, &data_size));
    return tx_id != 0; //tx_id of 0 means no more log records left
}

__attribute__((warn_unused_result)) bool xnlogitr_free(struct xnlogitr *itr) {
    free(itr->buf);
    free(itr);
    return xn_ok();
}


__attribute__((warn_unused_result)) bool xnlog_create(const char *log_path, struct xnlog **out_log) {
    struct xnlog *log;
    xn_ensure(xn_malloc(sizeof(struct xnlog), (void**)&log));

    xn_ensure(xnfile_create("log", true, &log->page.file_handle));
    xn_ensure(xnfile_set_size(log->page.file_handle, 32 * XNPG_SZ)); //TODO temporary size - will need to add code to resize file if needed

    //make iterator here to get last record offset
    struct xnlogitr *itr;
    xn_ensure(xnlogitr_create(log, &itr));
    while (xnlogitr_next(itr))
        ;
    log->page.idx = itr->page.idx;
    log->page_off = itr->page_off;
    xn_ensure(xnlogitr_free(itr));

    xn_ensure(xn_aligned_malloc(XNPG_SZ, (void**)&log->buf));
    xn_ensure(xnpg_read(&log->page, log->buf));
    
    log->highest_tx_flushed = -1;

    *out_log = log;
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xnlog_flush(struct xnlog *log) {
    xn_ensure(xnpg_write(&log->page, log->buf));
    //don't need to call xnfile_sync since log files are opend with O_DATASYNC flag
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xnlog_append(struct xnlog *log, const uint8_t *log_record, size_t size) {
    size_t written = 0;
    while (written < size) {
        size_t to_write = size - written;
        size_t remaining = XNPG_SZ - log->page_off;
        size_t s = remaining < to_write ? remaining : to_write;
        memcpy(log->buf + log->page_off, log_record + written, s);

        log->page_off += s;
        written += s;

        //log buffer full
        if (log->page_off == XNPG_SZ) {
            xn_ensure(xnlog_flush(log));
            log->page.idx++;
            log->page_off = 0;
        }
    }

    return xn_ok();
}

size_t xnlog_record_size(size_t data_size) {
    size_t size = sizeof(int);      //tx id
    size += sizeof(enum xnlogt);    //log type
    size += sizeof(size_t);         //data size 
    size += data_size;              //data
    size += sizeof(uint32_t);       //checksum
    return size;
}

__attribute__((warn_unused_result)) bool xnlog_serialize_record(int tx_id, 
                                                                enum xnlogt type, 
                                                                size_t data_size, 
                                                                uint8_t *data, 
                                                                uint8_t *buf) {
    off_t off = 0;
    memcpy(buf + off, &tx_id, sizeof(int));
    off += sizeof(int);
    memcpy(buf + off, &type, sizeof(enum xnlogt));
    off += sizeof(enum xnlogt);
    memcpy(buf + off, &data_size, sizeof(size_t));
    off += sizeof(size_t);
    memcpy(buf + off, data, data_size);
    off += data_size;
    uint32_t checksum = xn_hash(buf, off);
    memcpy(buf + off, &checksum, sizeof(uint32_t));
    off += sizeof(uint32_t);

    return xn_ok();
}


__attribute__((warn_unused_result)) bool xndb_recover(struct xndb *db);

__attribute__((warn_unused_result)) bool xndb_create(const char *dir_path, struct xndb **out_db) {
    struct xndb *db;
    xn_ensure(xn_malloc(sizeof(struct xndb), (void**)&db));
    xn_ensure(pthread_mutex_init(&db->wrtx_lock, NULL) == 0);
    xn_ensure(pthread_mutex_init(&db->rdtx_count_lock, NULL) == 0);
    xn_ensure(pthread_mutex_init(&db->committed_wrtx_lock, NULL) == 0);

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

    xn_ensure(pthread_mutex_init(&db->tx_id_counter_lock, NULL) == 0);
    db->tx_id_counter = 1;

    xn_ensure(xndb_recover(db));

    *out_db = db;
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xndb_free(struct xndb *db) {
    xn_ensure(pthread_mutex_destroy(&db->wrtx_lock) == 0);
    xn_ensure(pthread_mutex_destroy(&db->rdtx_count_lock) == 0);
    xn_ensure(pthread_mutex_destroy(&db->committed_wrtx_lock) == 0);
    xn_ensure(pthread_mutex_destroy(&db->tx_id_counter_lock) == 0);
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

__attribute__((warn_unused_result)) bool xntx_create(struct xndb *db, enum xntxmode mode, struct xntx **out_tx) {
    struct xntx *tx;
    xn_ensure((tx = malloc(sizeof(struct xntx))) != NULL);

    xn_ensure(pthread_mutex_lock(&db->tx_id_counter_lock) == 0);
    tx->id = db->tx_id_counter++;
    xn_ensure(pthread_mutex_unlock(&db->tx_id_counter_lock) == 0);

    tx->db = db;
    tx->rdtx_count = 0;
    xn_ensure(pthread_mutex_init(&tx->rdtx_count_lock, NULL) == 0);
    if (mode == XNTXMODE_WR) {
        xn_ensure(pthread_mutex_lock(&db->wrtx_lock) == 0);
        xn_ensure(xntbl_create(&tx->mod_pgs));

        uint8_t *rec;
        size_t rec_size = xnlog_record_size(0);
        xn_ensure(xn_malloc(rec_size, (void**)&rec));
        xn_ensure(xnlog_serialize_record(tx->id, XNLOGT_START, 0, NULL, rec));
        xn_ensure(xnlog_append(db->log, rec, rec_size));
        free(rec);
    } else {
        xn_ensure(pthread_mutex_lock(&db->committed_wrtx_lock));
        if (db->committed_wrtx) {
            tx->mod_pgs = db->committed_wrtx->mod_pgs;
            xn_ensure(xn_atomic_increment(&tx->rdtx_count, &tx->rdtx_count_lock));
        } else {
            tx->mod_pgs = NULL;
            xn_ensure(xn_atomic_increment(&db->rdtx_count, &db->rdtx_count_lock));
        }
        xn_ensure(pthread_mutex_unlock(&db->committed_wrtx_lock));
    }
    tx->mode = mode;
    *out_tx = tx;
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xn_wait_until_zero(int *count, pthread_mutex_t *lock, pthread_cond_t *cv) {
    xn_ensure(pthread_mutex_lock(lock) == 0);
    while (*count > 0) {
        xn_ensure(pthread_cond_wait(cv, lock) == 0);
    }
    xn_ensure(pthread_mutex_unlock(lock) == 0);

    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_flush_writes(struct xntx *tx) {
    assert(tx->mode == XNTXMODE_WR);

    struct xnpg page = {.file_handle = tx->db->file_handle, .idx = -1};
    for (int i = 0; i < XNTBL_MAX_BUCKETS; i++) {
        struct xnentry *cur = tx->mod_pgs->entries[i];
        while (cur) {
            page.idx = cur->pg_idx;
            //TODO should this use mmap and mprotect???
            xn_ensure(xnpg_write(&page, cur->val));
            cur = cur->next;
        }
    }
    xn_ensure(xnfile_sync(tx->db->file_handle));

    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_free(struct xntx *tx) {
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
    xn_ensure(pthread_mutex_unlock(&tx->db->wrtx_lock) == 0);
    xn_ensure(xntbl_free(tx->mod_pgs, false));
    xn_ensure(pthread_mutex_destroy(&tx->rdtx_count_lock) == 0);
    free(tx);
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_commit(struct xntx *tx) {
    assert(tx->mode == XNTXMODE_WR);

    xn_ensure(pthread_mutex_lock(&tx->db->committed_wrtx_lock));

    uint8_t *rec;
    size_t rec_size = xnlog_record_size(0);
    xn_ensure(xn_malloc(rec_size, (void**)&rec));
    xn_ensure(xnlog_serialize_record(tx->id, XNLOGT_COMMIT, 0, NULL, rec));
    xn_ensure(xnlog_append(tx->db->log, rec, rec_size));
    free(rec);
    tx->db->committed_wrtx = tx;

    xn_ensure(pthread_mutex_unlock(&tx->db->committed_wrtx_lock));

    //TODO remaining code should run asynchronously (eg, no more readers in next tbl)
  
    //TODO writing data to disk when 1. next table has no more readers (it will have one writer - this current tx itself.  But this tx is committed already) 
    xn_ensure(xn_wait_until_zero(&tx->db->rdtx_count, &tx->db->rdtx_count_lock, &disk_rdtxs_cv));
    xn_ensure(xnlog_flush(tx->db->log));
    xn_ensure(xntx_flush_writes(tx));

    xn_ensure(pthread_mutex_lock(&tx->db->committed_wrtx_lock));
    tx->db->committed_wrtx = NULL;
    xn_ensure(pthread_mutex_unlock(&tx->db->committed_wrtx_lock));

    //TODO page table should be freed when no more readers and writer txs are reading from this page table

    //TODO freeing this table should only occur when 1. no more readers and 2. no more writer (up to one writer) referencing it
    xn_ensure(xn_wait_until_zero(&tx->rdtx_count, &tx->rdtx_count_lock, &mem_rdtxs_cv));
    //TODO xntx_free is releasing the write lock - this can occur right after this tx commits (tx->db->committed_wrtx = tx)
    //If a new write tx starts, it will prevent this write transaction from being freed
    xn_ensure(xntx_free(tx));


    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_rollback(struct xntx *tx) {
    xn_ensure(xntx_free(tx));

    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_write(struct xntx *tx, struct xnpg *page, const uint8_t *buf, int offset, size_t size, bool log) {
    assert(tx->mode == XNTXMODE_WR);

    uint8_t *cpy;

    if (!(cpy = xntbl_find(tx->mod_pgs, page))) {
        xn_ensure((cpy = malloc(XNPG_SZ)) != NULL);
        xn_ensure(xnpg_read(page, cpy));
        xn_ensure(xntbl_insert(tx->mod_pgs, page, cpy));
    }

    memcpy(cpy + offset, buf, size);

    if (log) {
        uint8_t *update_data;
        size_t data_size = size + sizeof(uint64_t) + sizeof(int);; //including page index and offset
        xn_ensure(xn_malloc(data_size, (void**)&update_data));
        memcpy(update_data, &page->idx, sizeof(uint64_t));
        memcpy(update_data + sizeof(uint64_t), &offset, sizeof(int));
        memcpy(update_data + sizeof(uint64_t) + sizeof(int), buf, size);
        printf("logging: page idx: %ld, page offset: %d\n", page->idx, offset);

        uint8_t *rec;
        size_t rec_size = xnlog_record_size(data_size);
        xn_ensure(xn_malloc(rec_size, (void**)&rec));
        xn_ensure(xnlog_serialize_record(tx->id, XNLOGT_UPDATE, data_size, update_data, rec));
        xn_ensure(xnlog_append(tx->db->log, rec, rec_size));

        free(rec);
        free(update_data);
    }

    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_read_ptr(struct xntx *tx, struct xnpg *page, uint8_t **out_ptr) {
    if (tx->mode == XNTXMODE_WR || tx->mod_pgs) {
        uint8_t *cpy;

        if ((cpy = xntbl_find(tx->mod_pgs, page))) {
            *out_ptr = cpy;
            return xn_ok();
        }
    }

    uint8_t *ptr;
    if (!(ptr = xntbl_find(tx->db->pg_tbl, page))) {
        xn_ensure(xnpg_mmap(page, &ptr));
        xn_ensure(xntbl_insert(tx->db->pg_tbl, page, ptr));
    }

    *out_ptr = ptr;

    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_read(struct xntx *tx, struct xnpg *page, uint8_t *buf, int offset, size_t size) {
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

__attribute__((warn_unused_result)) bool xntx_find_free_page(struct xntx *tx, struct xnpg *meta_page, struct xnpg *new_page) {
    int page_count = tx->db->file_handle->size / XNPG_SZ;
    int i, j;
    for (i = 0; i < page_count; i++) {
        uint8_t byte;
        xn_ensure(xntx_read(tx, meta_page, &byte, i * sizeof(uint8_t), sizeof(uint8_t)));
        for (j = 0; j < 8; j++) {
            uint8_t mask = 1 << j;
            uint8_t bit = (mask & byte) >> j;

            if (bit == 0)
                goto found_free_bit;
        }
    }
    return false;

found_free_bit:
    new_page->file_handle = meta_page->file_handle;
    new_page->idx = i * 8 + j;
    return xn_ok();
}

int xnpgr_bitmap_byte_offset(uint64_t page_idx) {
    return page_idx / 8;
}

__attribute__((warn_unused_result)) bool xntx_free_page(struct xntx *tx, struct xnpg page) {
    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = XNPGID_METADATA};

    //set bit to 'free'
    uint8_t byte;
    xn_ensure(xntx_read(tx, &meta_page, &byte, xnpgr_bitmap_byte_offset(page.idx), sizeof(uint8_t)));
    byte &= ~(1 << (page.idx % 8));
    xn_ensure(xntx_write(tx, &meta_page, &byte, xnpgr_bitmap_byte_offset(page.idx), sizeof(uint8_t), true));
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_allocate_page(struct xntx *tx, struct xnpg *page) {
    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = XNPGID_METADATA};

    xn_ensure(xntx_find_free_page(tx, &meta_page, page));
    printf("new page idx: %ld\n", page->idx);

    //zero out new page data
    __attribute__((cleanup(xn_cleanup_free))) uint8_t *buf;
    xn_ensure((buf = malloc(XNPG_SZ)) != NULL);
    memset(buf, 0, XNPG_SZ);
    xn_ensure(xntx_write(tx, page, buf, 0, XNPG_SZ, true));

    //set bit to 'used'
    uint8_t byte;
    xn_ensure(xntx_read(tx, &meta_page, &byte, xnpgr_bitmap_byte_offset(page->idx), sizeof(uint8_t)));
    byte |= 1 << (page->idx % 8);
    xn_ensure(xntx_write(tx, &meta_page, &byte, xnpgr_bitmap_byte_offset(page->idx), sizeof(uint8_t), true));
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xnlog_redo(struct xnlog *log, struct xntx *tx, uint64_t page_idx, int page_off, int tx_id) {
    struct xnlogitr *itr;
    xn_ensure(xnlogitr_create(log, &itr));
    xn_ensure(xnlogitr_seek(itr, page_idx, page_off));
    while (xnlogitr_next(itr)) {
        int cur_tx_id;
        enum xnlogt type;
        size_t data_size;
        xn_ensure(xnlogitr_read_header(itr, &cur_tx_id, &type, &data_size));
        if (type == XNLOGT_COMMIT && cur_tx_id == tx_id) {
            break;
        } else if (type == XNLOGT_UPDATE && cur_tx_id == tx_id) {
            uint8_t *buf;
            xn_ensure(xn_malloc(data_size, (void**)&buf));
            xn_ensure(xnlogitr_read_data(itr, buf, data_size));
            struct xnpg page = { .file_handle = log->page.file_handle, .idx = *((uint64_t*)buf) };
            size_t data_hdr_size = sizeof(uint64_t) + sizeof(int);
            int off = *((int*)(buf + sizeof(uint64_t)));
            size_t size = data_size - data_hdr_size;
            printf("redoing tx: %d, size: %ld, page idx: %ld, page off: %d\n", tx_id, size, page.idx, off);
            xn_ensure(xntx_write(tx, &page, buf + data_hdr_size, off, size, false));
            free(buf);
        }
    }
    xn_ensure(xnlogitr_free(itr));
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xndb_recover(struct xndb *db) {
    struct xnlogitr *itr;
    xn_ensure(xnlogitr_create(db->log, &itr));
    struct xntx *tx;
    xn_ensure(xntx_create(db, XNTXMODE_WR, &tx));

    uint64_t start_pageidx;
    int start_pageoff;
    int start_txid = 0;

    while (xnlogitr_next(itr)) {
        int tx_id;
        enum xnlogt type;
        size_t data_size;
        xn_ensure(xnlogitr_read_header(itr, &tx_id, &type, &data_size));
        if (type == XNLOGT_START) {
            start_pageidx = itr->page.idx;
            start_pageoff = itr->page_off;
            start_txid = tx_id;
        } else if (type == XNLOGT_COMMIT && start_txid == tx_id) {
            xn_ensure(xnlog_redo(db->log, tx, start_pageidx, start_pageoff, start_txid));
        }
    }

    xn_ensure(xnlogitr_free(itr));
    xn_ensure(xntx_flush_writes(tx));
    xn_ensure(xntx_free(tx));
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
        if (!xntx_allocate_page(tx, &page))
            printf("failed\n");
        if (!xntx_commit(tx))
            printf("failed\n");
    } else {
        struct xntx *tx;
        if (!xntx_create(db, XNTXMODE_RD, &tx))
            printf("failed\n");
        struct xnpg page = { .file_handle = tx->db->file_handle, .idx = 0 };

        uint8_t *buf = malloc(XNPG_SZ);
        if (!xntx_read(tx, &page, buf, 0, XNPG_SZ))
            printf("failed\n");
        printf("pages: %d\n", *buf);
        free(buf);
        if (!xntx_free(tx))
            printf("failed\n");
    }
}

int main(int argc, char** argv) {
    struct xndb *db;
    if (!xndb_create("students", &db))
        printf("failed\n");

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
        if (!xntx_create(db, XNTXMODE_WR, &tx))
            printf("failed\n");
        struct xnpg page;
        if (!xntx_allocate_page(tx, &page))
            printf("failed\n");
        if (!xntx_free_page(tx, page))
            printf("failed\n");
        if (!xntx_allocate_page(tx, &page))
            printf("failed\n");
        if (!xntx_commit(tx))
            printf("failed\n");
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

    if (!xndb_free(db))
        printf("failed\n");
    return 0;
}
