#include "table.h"

#include <stdlib.h>
#include <string.h>

xnresult_t xntbl_create(struct xntbl **out_tbl, bool mapped) {
    xnmm_init();

    struct xntbl *tbl;
    xnmm_alloc(xn_free, xn_malloc, (void**)&tbl, sizeof(struct xntbl));
    xnmm_alloc(xn_free, xn_malloc, (void**)&tbl->entries, sizeof(struct xnentry*) * XNTBL_MAX_BUCKETS);

    memset(tbl->entries, 0, sizeof(struct xnentry*) * XNTBL_MAX_BUCKETS);
    tbl->mapped = mapped;

    *out_tbl = tbl;
    return xn_ok();
}

xnresult_t xntbl_free(void **t) {
    xnmm_init();
    struct xntbl* tbl = (struct xntbl*)(*t);
    for (int i = 0; i < XNTBL_MAX_BUCKETS; i++) {
        struct xnentry *cur = tbl->entries[i];
        while (cur) {
            struct xnentry *next = cur->next;
            if (tbl->mapped) {
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
    xnmm_init();
    size_t path_size = strlen(page->file_handle->path);
    size_t size = path_size + sizeof(uint64_t);
    xnmm_scoped_alloc(scoped_ptr, xn_free, xn_malloc, &scoped_ptr, size);
    uint8_t *buf = (uint8_t*)scoped_ptr;  
    memcpy(buf, page->file_handle->path, path_size);
    memcpy(buf + path_size, &page->idx, sizeof(uint64_t));
    uint32_t hash = xn_hash(buf, size);
    uint32_t bucket = hash % XNTBL_MAX_BUCKETS;
    struct xnentry* cur = tbl->entries[bucket];

    while (cur) {
        if (cur->page.idx == page->idx && strcmp(cur->page.file_handle->path, page->file_handle->path) == 0) {
            xnmm_cleanup_all();
            return cur->val;
        }

        cur = cur->next;
    }

    xnmm_cleanup_all();
    return NULL;
}

xnresult_t xntbl_insert(struct xntbl *tbl, struct xnpg *page, uint8_t *val) {
    xnmm_init();
    size_t path_size = strlen(page->file_handle->path);
    size_t size = path_size + sizeof(uint64_t);
    xnmm_scoped_alloc(scoped_ptr, xn_free, xn_malloc, &scoped_ptr, size);
    uint8_t *buf = (uint8_t*)scoped_ptr;  
    memcpy(buf, page->file_handle->path, path_size);
    memcpy(buf + path_size, &page->idx, sizeof(uint64_t));
    uint32_t hash = xn_hash(buf, size);
    uint32_t bucket = hash % XNTBL_MAX_BUCKETS;
    struct xnentry* cur = tbl->entries[bucket];

    while (cur) {
        if (cur->page.idx == page->idx && strcmp(cur->page.file_handle->path, page->file_handle->path) == 0) {
            cur->val = val;
            return xn_ok();
        }
            
        cur = cur->next;
    }

    //insert at beginning of linked-list
    struct xnentry* head = tbl->entries[bucket];
    struct xnentry* entry;
    xnmm_alloc(xn_free, xn_malloc, (void**)&entry, sizeof(struct xnentry));

    entry->next = head;
    entry->page = *page;
    entry->val = val;
    tbl->entries[bucket] = entry;

    return xn_ok();
}
