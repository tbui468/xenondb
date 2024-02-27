#define _GNU_SOURCE

//TODO these headers should be moved
#define XNPGID_METADATA 0
#define XNPG_SZ 4096
#include <stdlib.h> //free
#include <errno.h>
#include <pthread.h>
#include <stdint.h>

#include "common.h"
#include "file.h"

enum xntxmode {
    XNTXMODE_RD,
    XNTXMODE_WR
};

//TODO move to own files later
struct xndb {
    pthread_rwlock_t wrtx_lock;
    struct xnfile *file_handle;
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

    *out_db = db;
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xndb_free(struct xndb *db) {
    xn_ensure(pthread_rwlock_destroy(&db->wrtx_lock) == 0);
    xn_ensure(xnfile_close(db->file_handle));
    free(db);
    return xn_ok();
}

struct xnpg {
    struct xnfile *file_handle;
    uint64_t idx;
};

uint8_t *xnpg_serialize(struct xnpg *page) {
    int slen = strlen(page->file_handle->path) + 1; //include null terminator
    uint8_t *buf;
    xn_ensure((buf = malloc(sizeof(uint64_t) + slen)) != NULL);

    memcpy(buf, &page->idx, sizeof(uint64_t));
    memcpy(buf + sizeof(uint64_t), page->file_handle->path, slen);
    return buf;
}

struct xnpgtbl {
    struct xnpgcpy *data;
    int count;
    int capacity;
};

__attribute__((warn_unused_result)) bool xnpgtbl_create(struct xnpgtbl **out_tbl) {
    struct xnpgtbl *tbl;
    xn_ensure((tbl = malloc(sizeof(struct xnpgtbl))) != NULL);
    tbl->data = NULL;
    tbl->count = 0;
    tbl->capacity = 0;

    *out_tbl = tbl;
    return true;
}

void xnpgtbl_free(struct xnpgtbl *tbl) {
    free(tbl->data);
    free(tbl);
}

//TODO move to own files later
struct xntx {
    enum xntxmode mode;
    pthread_rwlock_t *lock;
    struct xnpgtbl *mod_pgs;
    struct xndb *db;
};

__attribute__((warn_unused_result)) bool xntx_create(struct xndb *db, enum xntxmode mode, struct xntx **out_tx) {
    struct xntx *tx;
    xn_ensure((tx = malloc(sizeof(struct xntx))) != NULL);
    tx->lock = NULL;
    tx->db = db;
    xn_ensure(xnpgtbl_create(&tx->mod_pgs));
    if (mode == XNTXMODE_WR) {
        tx->lock = &db->wrtx_lock;
        xn_ensure(pthread_rwlock_wrlock(&db->wrtx_lock) == 0);
    }
    tx->mode = mode;
    *out_tx = tx;
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_free(struct xntx *tx) {
    if (tx->mode == XNTXMODE_WR)
        xn_ensure(pthread_rwlock_unlock(tx->lock) == 0);
    xnpgtbl_free(tx->mod_pgs);
    free(tx);
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_commit(struct xntx *tx) {
    //get a xlock on all modified page to prevent readers from reading partial data
    //write logs to durable storage, and sync
    return xn_ok();
}

__attribute__((warn_unused_result)) bool xntx_write(struct xntx *tx, struct xnpg *page, uint8_t *buf, int offset, size_t size) {
    uint8_t *cpy, *key;
    key = xnpg_serialize(page);
    xn_defer(free, key);

    if (!(cpy = xnpgtbl_find(tx->mod_pgs, key))) {
        xn_ensure((cpy = malloc(XNPG_SZ)) != NULL);
        xn_ensure(xnfile_read(page->file_handle, cpy, 0, XNPG_SZ));
        xn_ensure(xnpgtbl_insert(tx->mod_pgs, key, cpy));
    }

    memcpy(cpy + offset, buf, size);
    return true;
}

__attribute__((warn_unused_result)) bool xntx_read(struct xntx *tx, struct xnpg *page, uint8_t *buf, int offset, size_t size) {
    uint8_t *cpy, *key;
    key = xnpg_serialize(page);
    xn_defer(free, key);

    if ((cpy = xnpgtbl_find(tx->mod_pgs, key))) {
        memcpy(buf, cpy + offset, size);
    } else {
        xn_ensure(xnfile_read(page->file_handle, buf, offset, size));
    }

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

__attribute__((warn_unused_result)) bool xntx_allocate_page(struct xntx *tx, struct xnpg *page) {
    struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = XNPGID_METADATA};

    struct xnpg new_page;
    xn_ensure(xntx_find_free_page(tx, &meta_page, &new_page));
    printf("new page idx: %ld\n", new_page.idx);

    //zero out new page data
    uint8_t *buf;
    xn_ensure((buf = malloc(XNPG_SZ)) != NULL);
    memset(buf, 0, XNPG_SZ);
    xn_ensure(xntx_write(tx, &new_page, buf, 0, XNPG_SZ));
    free(buf);

    //set bit to 'used'
    uint8_t byte;
    xn_ensure(xntx_read(tx, &meta_page, &byte, xnpgr_bitmap_byte_offset(new_page.idx), sizeof(uint8_t)));
    byte |= 1 << (new_page.idx % 8);
    xn_ensure(xntx_write(tx, &meta_page, &byte, xnpgr_bitmap_byte_offset(new_page.idx), sizeof(uint8_t)));
    return xn_ok();
}

int main(int argc, char** argv) {
    struct xndb *db;
    if (!xndb_create("students", &db))
        printf("failed\n");

    struct xntx *tx;
    if (!xntx_create(db, XNTXMODE_WR, &tx))
        printf("failed\n");

    struct xnpg page;
    if (!xntx_allocate_page(tx, &page))
        printf("failed\n");
    //first page should have index 1, and second page index 2
    //xntx_free_page(txt, "data", 1);

    if (!xntx_commit(tx))
        printf("failed\n");
    if (!xntx_free(tx))
        printf("failed\n");

    if (!xndb_free(db))
        printf("failed\n");
    return 0;
}
