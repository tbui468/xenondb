#include "table.h"

#include <stdlib.h>
#include <string.h>

xnresult_t xntbl_create(struct xntbl **out_tbl) {
    xnmm_init();
    struct xntbl *tbl;
    xn_ensure(xn_malloc((void**)&tbl, sizeof(struct xntbl)));
    xn_ensure(xn_malloc((void**)&tbl->entries, sizeof(struct xnentry*) * XNTBL_MAX_BUCKETS));
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
    xn_ensure(xn_malloc((void**)&entry, sizeof(struct xnentry)));
    entry->next = head;
    entry->pg_idx = page->idx;
    entry->val = val;
    tbl->entries[bucket] = entry;

    return xn_ok();
}
