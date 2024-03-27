#pragma once

#include "util.h"
#include "page.h"
#include "tx.h"

#define XNCTN_HDR_SZ 32

struct xnctn {
    struct xnpg pg;
};

struct xnitemid {
    uint64_t pg_idx;
    uint16_t arr_idx;
};

struct xnctnitr {
    int arr_idx;
    struct xnctn ctn;
    struct xntx *tx;
};

xnresult_t xnctn_init(struct xnctn *ctn, struct xntx *tx);
xnresult_t xnctn_can_fit(struct xnctn *ctn, struct xntx *tx, size_t size, bool *result);
xnresult_t xnctn_insert(struct xnctn *ctn, struct xntx *tx, const uint8_t *buf, size_t size, struct xnitemid *out_id);
xnresult_t xnctn_get(struct xnctn *ctn, struct xntx *tx, struct xnitemid id, uint8_t *buf, size_t size);
xnresult_t xnctn_get_size(struct xnctn *ctn, struct xntx *tx, struct xnitemid id, size_t *size);
xnresult_t xnctn_delete(struct xnctn *ctn, struct xntx *tx, struct xnitemid id);
xnresult_t xnctn_update(struct xnctn *ctn, struct xntx *tx, struct xnitemid id, uint8_t *data, size_t size, struct xnitemid *new_id);

xnresult_t xnctnitr_init(struct xnctnitr *itr, struct xnctn ctn, struct xntx *tx);
xnresult_t xnctnitr_next(struct xnctnitr *itr, bool *valid);
xnresult_t xnctnitr_itemid(struct xnctnitr *itr, struct xnitemid *id);
