#include "container.h"
#include <string.h>

xnresult_t xnctn_create(struct xnctn **out_ctn, struct xntx *tx, struct xnpg pg) {
    xnmm_init();
    
    struct xnctn *ctn;

    xnmm_alloc(xn_free, xn_malloc, (void**)&ctn, sizeof(struct xnctn));
    ctn->tx = tx;
    ctn->pg = pg;

    *out_ctn = ctn;
    return xn_ok();
}

xnresult_t xnctn_free(void **c) {
    xnmm_init();
    free(*c);
    return xn_ok();
}

xnresult_t xnctn_init(struct xnctn *ctn) {
    xnmm_init();
    
    //zero out page
    xnmm_scoped_alloc(scoped_ptr, xn_free, xn_malloc, &scoped_ptr, XNPG_SZ);
    uint8_t *buf = (uint8_t*)scoped_ptr;
    memset(buf, 0, XNPG_SZ);
    xn_ensure(xnpg_write(&ctn->pg, ctn->tx, buf, 0, XNPG_SZ, true));

    //write header
    uint16_t item_count = 0;
    uint16_t floor = XNCTN_HDR_SZ;
    uint16_t ceil = XNPG_SZ;
    xn_ensure(xnpg_write(&ctn->pg, ctn->tx, (uint8_t*)&item_count, 0, sizeof(uint16_t), true));
    xn_ensure(xnpg_write(&ctn->pg, ctn->tx, (uint8_t*)&floor, sizeof(uint16_t), sizeof(uint16_t), true));
    xn_ensure(xnpg_write(&ctn->pg, ctn->tx, (uint8_t*)&ceil, sizeof(uint16_t) * 2, sizeof(uint16_t), true));

    return xn_ok();
}

xnresult_t xnctn_can_fit(struct xnctn *ctn, size_t data_size, bool *result) {
    xnmm_init();

    //read container metadata
    uint16_t floor;
    xn_ensure(xnpg_read(&ctn->pg, ctn->tx, (uint8_t*)&floor, sizeof(uint16_t), sizeof(uint16_t)));
    uint16_t ceil;
    xn_ensure(xnpg_read(&ctn->pg, ctn->tx, (uint8_t*)&ceil, sizeof(uint16_t) * 2, sizeof(uint16_t)));

    *result = ceil - floor >= data_size + sizeof(uint32_t);
    return xn_ok();
}

static inline uint32_t xnctn_set_ptr_fields(uint32_t used, uint32_t size, uint32_t off) {
    return (off << 16) | (size << 1) | used;
}

static inline void xnctn_get_ptr_fields(uint32_t ptr, uint32_t *used, uint32_t *size, uint32_t *off) {
    *off = *((uint16_t*)&ptr + 1);
    *size = *((uint16_t*)&ptr) >> 1;
    uint32_t used_mask = 1;
    *used = ptr & used_mask;
}

xnresult_t xnctn_insert(struct xnctn *ctn, const uint8_t *buf, size_t size, struct xnitemid *out_id) {
    xnmm_init();

    //read container metadata
    uint16_t item_count;
    xn_ensure(xnpg_read(&ctn->pg, ctn->tx, (uint8_t*)&item_count, 0, sizeof(uint16_t)));
    uint16_t floor;
    xn_ensure(xnpg_read(&ctn->pg, ctn->tx, (uint8_t*)&floor, sizeof(uint16_t), sizeof(uint16_t)));
    uint16_t ceil;
    xn_ensure(xnpg_read(&ctn->pg, ctn->tx, (uint8_t*)&ceil, sizeof(uint16_t) * 2, sizeof(uint16_t)));

    //make sure enough space in container to store data + array pointer
    xn_ensure(ceil - size > floor + sizeof(uint16_t) * 2);

    //write pointer
    uint32_t data_off = ceil - size;
    uint32_t data_size = size;
    uint32_t used = 1;
    uint32_t ptr = xnctn_set_ptr_fields(used, data_size, data_off);
    xn_ensure(xnpg_write(&ctn->pg, ctn->tx, (uint8_t*)&ptr , floor, sizeof(uint32_t), true));

    //write data
    xn_ensure(xnpg_write(&ctn->pg, ctn->tx, buf, data_off, size, true));

    out_id->pg_idx = ctn->pg.idx;
    out_id->arr_idx = item_count;

    //update container metadata
    item_count++;
    floor += sizeof(uint16_t) * 2;
    ceil -= size;
    xn_ensure(xnpg_write(&ctn->pg, ctn->tx, (uint8_t*)&item_count, 0, sizeof(uint16_t), true));
    xn_ensure(xnpg_write(&ctn->pg, ctn->tx, (uint8_t*)&floor, sizeof(uint16_t), sizeof(uint16_t), true));
    xn_ensure(xnpg_write(&ctn->pg, ctn->tx, (uint8_t*)&ceil, sizeof(uint16_t) * 2, sizeof(uint16_t), true));

    return xn_ok();
}


xnresult_t xnctn_get(struct xnctn *ctn, struct xnitemid id, uint8_t *buf, size_t size) {
    xnmm_init();

    xn_ensure(ctn->pg.idx == id.pg_idx);

    //read container metadata
    uint16_t item_count;
    xn_ensure(xnpg_read(&ctn->pg, ctn->tx, (uint8_t*)&item_count, 0, sizeof(uint16_t)));
    xn_ensure(id.arr_idx < item_count);

    off_t ptr_off = XNCTN_HDR_SZ + id.arr_idx * 2 * sizeof(uint16_t);
    uint32_t ptr;
    xn_ensure(xnpg_read(&ctn->pg, ctn->tx, (uint8_t*)&ptr, ptr_off, sizeof(uint32_t)));

    uint32_t used;
    uint32_t data_size;
    uint32_t data_off;
    xnctn_get_ptr_fields(ptr, &used, &data_size, &data_off);

    xn_ensure(size == data_size);

    xn_ensure(xnpg_read(&ctn->pg, ctn->tx, buf, data_off, data_size));

    return xn_ok();
}

