#define _GNU_SOURCE

//TODO these headers should be moved
#define XNPGID_METADATA 0
#define XNTBL_MAX_BUCKETS 128
#define XNPG_SZ 4096
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

__attribute__((warn_unused_result)) bool xnpg_mmap(struct xnpg *page, uint8_t **ptr) {
    xn_ensure(xnfile_mmap(0, XNPG_SZ, MAP_SHARED, PROT_READ, page->file_handle->fd, page->idx * XNPG_SZ, (void**)ptr));
    return true;
}

__attribute__((warn_unused_result)) bool xnpg_munmap(uint8_t *ptr) {
    xn_ensure(xnfile_munmap((void*)ptr, XNPG_SZ));
    return true;
}

__attribute__((warn_unused_result)) bool xn_atomic_increment(int *i, pthread_mutex_t *lock) {
    xn_ensure(pthread_mutex_lock(lock) == 0);
    (*i)++;
    xn_ensure(pthread_mutex_unlock(lock) == 0);
}

__attribute__((warn_unused_result)) bool xn_atomic_decrement(int *i, pthread_mutex_t *lock) {
    xn_ensure(pthread_mutex_lock(lock) == 0);
    (*i)--;
    xn_ensure(pthread_mutex_unlock(lock) == 0);
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

//TODO move to own files later
struct xndb {
    pthread_rwlock_t wrtx_lock;
    struct xnfile *file_handle;
    struct xntbl *pg_tbl;
    bool wrtx_committed;
    pthread_mutex_t rdtx_count_lock;
    int rdtx_count;
};

__attribute__((warn_unused_result)) bool xndb_create(const char *dir_path, struct xndb **out_db) {
    struct xndb *db;
    xn_ensure((db = malloc(sizeof(struct xndb))) != NULL);
    xn_ensure(pthread_rwlock_init(&db->wrtx_lock, NULL) == 0);
    xn_ensure(pthread_mutex_init(&db->rdtx_count_lock, NULL) == 0);

    xn_ensure(xnfile_create(dir_path, false, &db->file_handle));
    xn_ensure(xnfile_set_size(db->file_handle, XNPG_SZ * 8));
    uint8_t buf = 1;
    xn_ensure(xnfile_write(db->file_handle, &buf, 0, sizeof(uint8_t)));
    xn_ensure(xnfile_sync(db->file_handle));

    xn_ensure(xntbl_create(&db->pg_tbl));

    db->wrtx_committed = false;
    db->rdtx_count = 0;

    *out_db = db;
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xndb_free(struct xndb *db) {
    xn_ensure(pthread_rwlock_destroy(&db->wrtx_lock) == 0);
    xn_ensure(pthread_mutex_destroy(&db->rdtx_count_lock) == 0);
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
};

__attribute__((warn_unused_result)) bool xntx_create(struct xndb *db, enum xntxmode mode, struct xntx **out_tx) {
    printf("creating tx\n");
    struct xntx *tx;
    xn_ensure((tx = malloc(sizeof(struct xntx))) != NULL);
    tx->db = db;
    tx->rdtx_count = 0;
    xn_ensure(pthread_mutex_init(&tx->rdtx_count_lock, NULL) == 0);
    if (mode == XNTXMODE_WR) {
        xn_ensure(pthread_rwlock_wrlock(&db->wrtx_lock) == 0);
        xn_ensure(xntbl_create(&tx->mod_pgs));
    } else {
        xn_ensure(xn_atomic_increment(&tx->db->rdtx_count, &db->rdtx_count_lock));
    }
    tx->mode = mode;
    *out_tx = tx;
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xnpg_write(struct xnpg *page, const uint8_t *buf) {
    xn_ensure(xnfile_write(page->file_handle, buf, page->idx * XNPG_SZ, XNPG_SZ)); 
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xnpg_read(struct xnpg *page, uint8_t *buf) {
    xn_ensure(xnfile_read(page->file_handle, buf, page->idx * XNPG_SZ, XNPG_SZ));
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
    if (XNTXMODE_RD) {
        xn_ensure(xn_atomic_decrement(&tx->db->rdtx_count, &tx->db->rdtx_count_lock));
        free(tx);
        return xn_ok();
    }

    //XNTXMODE_WR
    xn_ensure(pthread_rwlock_unlock(&tx->db->wrtx_lock) == 0);
    xn_ensure(xntbl_free(tx->mod_pgs, false));
    xn_ensure(pthread_mutex_destroy(&tx->rdtx_count_lock) == 0);
    free(tx);
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_commit(struct xntx *tx) {
    assert(tx->mode == XNTXMODE_WR);
    tx->db->wrtx_committed = true;
    
    xn_ensure(xn_wait_until_zero(&tx->db->rdtx_count, &tx->db->rdtx_count_lock, &disk_rdtxs_cv));
    xn_ensure(xntx_flush_writes(tx));
    tx->db->wrtx_committed = false;

    xn_ensure(xn_wait_until_zero(&tx->rdtx_count, &tx->rdtx_count_lock, &mem_rdtxs_cv));
    xn_ensure(xntx_free(tx));

    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_rollback(struct xntx *tx) {
    xn_ensure(xntx_free(tx));

    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_write(struct xntx *tx, struct xnpg *page, const uint8_t *buf, int offset, size_t size) {
    assert(tx->mode == XNTXMODE_WR);

    uint8_t *cpy;

    if (!(cpy = xntbl_find(tx->mod_pgs, page))) {
        xn_ensure((cpy = malloc(XNPG_SZ)) != NULL);
        xn_ensure(xnpg_read(page, cpy));
        xn_ensure(xntbl_insert(tx->mod_pgs, page, cpy));
    }

    memcpy(cpy + offset, buf, size);
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_read(struct xntx *tx, struct xnpg *page, uint8_t *buf, int offset, size_t size) {
    if (tx->mode == XNTXMODE_WR) {
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
    xn_ensure(xntx_write(tx, &meta_page, &byte, xnpgr_bitmap_byte_offset(page.idx), sizeof(uint8_t)));
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
    xn_ensure(xntx_write(tx, page, buf, 0, XNPG_SZ));

    //set bit to 'used'
    uint8_t byte;
    xn_ensure(xntx_read(tx, &meta_page, &byte, xnpgr_bitmap_byte_offset(page->idx), sizeof(uint8_t)));
    byte |= 1 << (page->idx % 8);
    xn_ensure(xntx_write(tx, &meta_page, &byte, xnpgr_bitmap_byte_offset(page->idx), sizeof(uint8_t)));
    return xn_ok();
}

int main(int argc, char** argv) {
    struct xndb *db;
    if (!xndb_create("students", &db))
        printf("failed\n");

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

    { 
        struct xntx *tx;
        if (!xntx_create(db, XNTXMODE_WR, &tx))
            printf("failed\n");
        struct xnpg page;
        if (!xntx_allocate_page(tx, &page))
            printf("failed\n");
        if (!xntx_commit(tx))
            printf("failed\n");
    }

    if (!xndb_free(db))
        printf("failed\n");
    return 0;
}
