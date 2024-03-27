#include "heap.h"
#include <string.h>


xnresult_t xnhp_open(struct xnhp **out_hp, struct xnfile *file, bool create, struct xntx *tx) {
    xnmm_init();

    struct xnhp *hp;
    xnmm_alloc(xn_free, xn_malloc, (void**)&hp, sizeof(struct xnhp));
    hp->meta.file_handle = file;
    hp->meta.idx = 1;

    if (create) {
        xn_ensure(xnfile_set_size(hp->meta.file_handle, XNPG_SZ * 32));
        struct xnpg heap_meta_page;

        xn_ensure(xnfile_init(hp->meta.file_handle, tx));
        xn_ensure(xnfile_allocate_page(hp->meta.file_handle, tx, &heap_meta_page));

        //metadata container
        struct xnctn meta_ctn = { .pg = heap_meta_page };
        xn_ensure(xnctn_init(&meta_ctn, tx));

        //first data container
        struct xnpg data_page;
        xn_ensure(xnfile_allocate_page(hp->meta.file_handle, tx, &data_page)); 
        struct xnctn data_ctn = { .pg = data_page };
        xn_ensure(xnctn_init(&data_ctn, tx));

        //set heap file metadata (only 2 for now)
        uint64_t start_ctn_idx = data_page.idx;
        struct xnitemid start_ctn_id;
        xn_ensure(xnctn_insert(&meta_ctn, tx, (uint8_t*)&start_ctn_idx, sizeof(uint64_t), &start_ctn_id));

        uint64_t cur_ctn_idx = start_ctn_idx;
        struct xnitemid cur_ctn_id;
        xn_ensure(xnctn_insert(&meta_ctn, tx, (uint8_t*)&cur_ctn_idx, sizeof(uint64_t), &cur_ctn_id));
    }

    *out_hp = hp;
    return xn_ok();
}

bool xnhp_free(void **h) {
    xnmm_init();
    struct xnhp *hp = (struct xnhp*)(*h);
    xn_free(h);

    return xn_ok();
}

static xnresult_t xnhp_get_current_ctn(struct xnhp *hp, struct xntx *tx, struct xnctn* out_ctn) {
    xnmm_init();

    uint64_t cur_ctn_idx;

    struct xnctn meta_ctn = { .pg = hp->meta };

    uint64_t cur_page_idx;
    struct xnitemid id = { .pg_idx = 1 /*heap metadata page*/, .arr_idx = 1 /*current container idx*/ }; 
    xn_ensure(xnctn_get(&meta_ctn, tx, id, (uint8_t*)&cur_page_idx, sizeof(uint64_t)));

    out_ctn->pg.file_handle = hp->meta.file_handle;
    out_ctn->pg.idx = cur_page_idx;
    return xn_ok();
}

static xnresult_t xnhp_append_container(struct xnhp *hp, struct xntx *tx, struct xnctn *out_ctn) {
    xnmm_init();

    struct xnpg new_page;
    xn_ensure(xnfile_allocate_page(hp->meta.file_handle, tx, &new_page));
    out_ctn->pg = new_page;
    xn_ensure(xnctn_init(out_ctn, tx));

    return xn_ok();
}

static xnresult_t xnhp_get_ctn(struct xnhp *hp, struct xnctn *ctn, uint64_t pg_idx) {
    xnmm_init();

    ctn->pg.file_handle = hp->meta.file_handle;
    ctn->pg.idx = pg_idx;

    return xn_ok();
}

xnresult_t xnhp_put(struct xnhp *hp, struct xntx *tx, uint8_t *buf, size_t size, struct xnitemid *id) {
    xnmm_init();

    struct xnctn ctn;
    xn_ensure(xnhp_get_current_ctn(hp, tx, &ctn));

    bool can_fit;
    xn_ensure(xnctn_can_fit(&ctn, tx, size, &can_fit));
    if (!can_fit) {
        xn_ensure(xnhp_append_container(hp, tx, &ctn));
    }

    xn_ensure(xnctn_insert(&ctn, tx, buf, size, id));

    return xn_ok();
}

xnresult_t xnhp_get_size(struct xnhp *hp, struct xntx *tx, struct xnitemid id, size_t *out_size) {
    xnmm_init();

    struct xnctn ctn;
    xn_ensure(xnhp_get_ctn(hp, &ctn, id.pg_idx));
    xn_ensure(xnctn_get_size(&ctn, tx, id, out_size));

    return xn_ok();
}

xnresult_t xnhp_get(struct xnhp *hp, struct xntx *tx, struct xnitemid id, uint8_t *val, size_t size) {
    xnmm_init();

    struct xnctn ctn;
    xn_ensure(xnhp_get_ctn(hp, &ctn, id.pg_idx));
    xn_ensure(xnctn_get(&ctn, tx, id, val, size));

    return xn_ok();
}

xnresult_t xnhp_del(struct xnhp *hp, struct xntx *tx, struct xnitemid id) {
    xnmm_init();

    struct xnctn ctn;
    xn_ensure(xnhp_get_ctn(hp, &ctn, id.pg_idx));
    xn_ensure(xnctn_delete(&ctn, tx, id));

    return xn_ok();
}

/*
struct xnhpscan {
    struct xnhp hp;
    struct xntx *tx;
    uint64_t pg_idx;
    int arr_idx;
};*/

/*
xnresult_t xnhpscan_open(struct xnhpscan *scan, struct xnhp hp, struct xntx *tx) {
    scan->hp = hp;
    scan->tx = tx;

    //TODO; find first pg_idx in heap and save to scan->pg_idx
    //make a container iterator for it
}

xnresult_t xnhpscan_close(struct xnhpscan *scan) {
    //TODO don't really need to do anything right now
}

xnresult_t xnhpscan_next(struct xnhpscan *scan, bool *result) {
    //go container iterator to next position
}

xnresult_t xnhpscan_itemid(struct xnhpscan *scan, struct xnitemid *id) {
    //call directly to item id in container
}*/
