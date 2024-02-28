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

#include "common.h"
#include "file.h"



struct xnpg {
    struct xnfile *file_handle;
    uint64_t idx;
};

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

void xntbl_free(struct xntbl *tbl) {
    for (int i = 0; i < XNTBL_MAX_BUCKETS; i++) {
        struct xnentry *cur = tbl->entries[i];
        while (cur) {
            struct xnentry *next = cur->next;
            free(cur->val);
            free(cur);
            cur = next;
        }
    }
    free(tbl->entries);
    free(tbl);
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
};

__attribute__((warn_unused_result)) bool xndb_create(const char *dir_path, struct xndb **out_db) {
    struct xndb *db;
    xn_ensure((db = malloc(sizeof(struct xndb))) != NULL);
    xn_ensure(pthread_rwlock_init(&db->wrtx_lock, NULL) == 0);

    xn_ensure(xnfile_create(dir_path, false, &db->file_handle));
    xn_ensure(xnfile_set_size(db->file_handle, XNPG_SZ * 8));
    uint8_t buf = 1;
    xn_ensure(xnfile_write(db->file_handle, &buf, 0, sizeof(uint8_t)));
    xn_ensure(xnfile_sync(db->file_handle));

    xn_ensure(xntbl_create(&db->pg_tbl));

    *out_db = db;
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xndb_free(struct xndb *db) {
    xn_ensure(pthread_rwlock_destroy(&db->wrtx_lock) == 0);
    xn_ensure(xnfile_close(db->file_handle));
    //TODO unmap pages before freeing page table
    xntbl_free(db->pg_tbl);
    free(db);
    return xn_ok();
}


//TODO move to own files later
struct xntx {
    enum xntxmode mode;
    struct xntbl *mod_pgs;
    struct xndb *db;
};

__attribute__((warn_unused_result)) bool xntx_create(struct xndb *db, enum xntxmode mode, struct xntx **out_tx) {
    struct xntx *tx;
    xn_ensure((tx = malloc(sizeof(struct xntx))) != NULL);
    tx->db = db;
    xn_ensure(xntbl_create(&tx->mod_pgs));
    if (mode == XNTXMODE_WR) {
        xn_ensure(pthread_rwlock_wrlock(&db->wrtx_lock) == 0);
    }
    tx->mode = mode;
    *out_tx = tx;
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xnpg_write(struct xnpg *page, const uint8_t *buf) {
    xn_ensure(xnfile_write(page->file_handle, buf, page->idx * XNPG_SZ, XNPG_SZ)); 
    return true;
}

__attribute__((warn_unused_result)) bool xnpg_read(struct xnpg *page, uint8_t *buf) {
    xn_ensure(xnfile_read(page->file_handle, buf, page->idx * XNPG_SZ, XNPG_SZ));
    return true;
}

__attribute__((warn_unused_result)) bool xntx_commit(struct xntx *tx) {
    struct xnpg page = {.file_handle = tx->db->file_handle, .idx = -1};
    for (int i = 0; i < XNTBL_MAX_BUCKETS; i++) {
        struct xnentry *cur = tx->mod_pgs->entries[i];
        while (cur) {
            page.idx = cur->pg_idx;
            xn_ensure(xnpg_write(&page, cur->val));
            cur = cur->next;
        }
    }
    //TODO write logs to durable storage, and sync
    if (tx->mode == XNTXMODE_WR)
        xn_ensure(pthread_rwlock_unlock(&tx->db->wrtx_lock) == 0);

    //TODO free local buffers before freeing modded page table
    xntbl_free(tx->mod_pgs);
    free(tx);
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_rollback(struct xntx *tx) {

    if (tx->mode == XNTXMODE_WR)
        xn_ensure(pthread_rwlock_unlock(&tx->db->wrtx_lock) == 0);

    //TODO free local buffers before freeing modded page table
    xntbl_free(tx->mod_pgs);
    free(tx);
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
    return true;
}

__attribute__((warn_unused_result)) bool xntx_read(struct xntx *tx, struct xnpg *page, uint8_t *buf, int offset, size_t size) {
    if (tx->mode == XNTXMODE_WR) {
        uint8_t *cpy;

        if ((cpy = xntbl_find(tx->mod_pgs, page))) {
            memcpy(buf, cpy + offset, size);
            return true;
        }
    }

    uint8_t *ptr;
    if (!(ptr = xntbl_find(tx->db->pg_tbl, page))) {
        xn_ensure((ptr = malloc(XNPG_SZ)) != NULL);
        xn_ensure(xnpg_read(page, ptr));
        xn_ensure(xntbl_insert(tx->db->pg_tbl, page, ptr));
    }

    //TODO need to update the potentially stale version in cache
    //this should not be necessary once we switch over to mmapped version
    xn_ensure(xnpg_read(page, ptr));

    memcpy(buf, ptr + offset, size);

    return true;
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
    return true;
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
        if (!xntx_rollback(tx))
            printf("failed\n");
    }

    if (!xndb_free(db))
        printf("failed\n");
    return 0;
}
