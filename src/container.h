#pragma once

#include "util.h"
#include "page.h"
#include "tx.h"

#define XNCTN_HDR_SZ 32

struct xnctn {
    struct xntx *tx;
    struct xnpg pg;
};

struct xnitemid {
    uint64_t pg_idx;
    uint16_t arr_idx;
};

struct xnctnitr {
    int idx;
    struct xnctn* ctn;
};

xnresult_t xnctn_create(struct xnctn **ctn, struct xntx *tx, struct xnpg pg);
bool xnctn_free(void **c);
xnresult_t xnctn_init(struct xnctn *ctn);
xnresult_t xnctn_can_fit(struct xnctn *ctn, size_t size, bool *result);
xnresult_t xnctn_insert(struct xnctn *ctn, const uint8_t *buf, size_t size, struct xnitemid *out_id);
xnresult_t xnctn_get(struct xnctn *ctn, struct xnitemid id, uint8_t *buf, size_t size);
xnresult_t xnctn_get_size(struct xnctn *ctn, struct xnitemid id, size_t *size);
xnresult_t xnctn_delete(struct xnctn *ctn, struct xnitemid id);
xnresult_t xnctn_update(struct xnctn *ctn, struct xnitemid id, uint8_t *data, size_t size, struct xnitemid *new_id);

xnresult_t xnctnitr_create(struct xnctnitr **out_itr, struct xnctn* ctn);
xnresult_t xnctnitr_next(struct xnctnitr *itr, bool *valid);
xnresult_t xnctnitr_itemid(struct xnctnitr *itr, struct xnitemid *id);
bool xnctnitr_free(void **i);