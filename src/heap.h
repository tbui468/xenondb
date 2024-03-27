#pragma once

#include "file.h"
#include "page.h"
#include "util.h"
#include "tx.h"
#include "container.h"


struct xnhp {
    struct xnpg meta;
	struct xntx *tx;
};

struct xnhpscan {
    struct xnhp hp;
    uint64_t pg_idx;
    int arr_idx;
};

xnresult_t xnhp_open(struct xnhp *hp, struct xnfile *file, bool create, struct xntx *tx);
xnresult_t xnhp_put(struct xnhp *hp, uint8_t *buf, size_t size, struct xnitemid *id);
xnresult_t xnhp_get_size(struct xnhp *hp, struct xnitemid id, size_t *out_size);
xnresult_t xnhp_get(struct xnhp *hp, struct xnitemid id, uint8_t *val, size_t size);
xnresult_t xnhp_del(struct xnhp *hp, struct xnitemid id);

xnresult_t xnhpscan_open(struct xnhpscan *scan, struct xnhp hp);
/*
xnresult_t xnhpscan_close(struct xnhpscan *scan);
xnresult_t xnhpscan_next(struct xnhpscan *scan, bool *result);
xnresult_t xnhpscan_itemid(struct xnhpscan *scan, struct xnitemid *id);*/
