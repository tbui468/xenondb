#pragma once

#include "page.h"

#define XNTBL_MAX_BUCKETS 128

struct xnentry {
    struct xnpg page;
    uint8_t *val;
    struct xnentry *next;
};

struct xntbl {
    struct xnentry **entries;
    int count;
    int capacity;
    bool mapped;
};

xnresult_t xntbl_create(struct xntbl **out_tbl, bool mapped);
xnresult_t xntbl_free(void **tbl);
uint8_t* xntbl_find(struct xntbl *tbl, struct xnpg *page);
xnresult_t xntbl_insert(struct xntbl *tbl, struct xnpg *page, uint8_t *val);
