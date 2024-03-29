#pragma once

#include "file.h"
#include <stdint.h>

#define XNPG_SZ 4096

struct xntx;
struct xnpg {
    struct xnfile *file_handle;
    uint64_t idx;
};

xnresult_t xnpg_flush(struct xnpg *page, const uint8_t *buf);
xnresult_t xnpg_copy(struct xnpg *page, uint8_t *buf);
xnresult_t xnpg_mmap(struct xnpg *page, uint8_t **ptr);
xnresult_t xnpg_munmap(uint8_t *ptr);
xnresult_t xnpg_write(struct xnpg *page, struct xntx *tx, const uint8_t *buf, int offset, size_t size, bool log);
xnresult_t xnpg_read(struct xnpg *page, struct xntx *tx, uint8_t *buf, int offset, size_t size);
