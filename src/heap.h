#pragma once

#include "file.h"
#include "page.h"
#include "util.h"
#include "tx.h"
#include "container.h"


struct xnhp {
    struct xnpg meta;
};

xnresult_t xnhp_open(struct xnhp **out_hp, struct xnfile *file, bool create, struct xntx *tx);
bool xnhp_free(void **h);
xnresult_t xnhp_insert(struct xnhp *hp, struct xntx *tx, uint8_t *buf, size_t size, struct xnitemid *id);


