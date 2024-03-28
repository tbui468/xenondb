#include "heap.h"
#include <string.h>


xnresult_t xnhp_open(struct xnhp *hp, struct xnfile *file, bool create, struct xntx *tx) {
    xnmm_init();

    hp->meta.file_handle = file;
    hp->meta.idx = 1; //hard-coding metadata page
	hp->tx = tx;

    if (create) {
        xn_ensure(xnfile_set_size(hp->meta.file_handle, XNPG_SZ * 32));
        struct xnpg heap_meta_page;

        xn_ensure(xnfile_init(hp->meta.file_handle, tx));
        xn_ensure(xnfile_allocate_page(hp->meta.file_handle, tx, &heap_meta_page));

        //metadata container
        struct xnctn meta_ctn;
		xn_ensure(xnctn_open(&meta_ctn, heap_meta_page, tx));
        xn_ensure(xnctn_init(&meta_ctn));

        //first data container
        struct xnpg data_page;
        xn_ensure(xnfile_allocate_page(hp->meta.file_handle, tx, &data_page)); 
        struct xnctn data_ctn;
		xn_ensure(xnctn_open(&data_ctn, data_page, tx));
        xn_ensure(xnctn_init(&data_ctn));

        //set heap file metadata (only 2 for now)
        uint64_t start_ctn_idx = data_page.idx;
        struct xnitemid start_ctn_id;
        xn_ensure(xnctn_insert(&meta_ctn, (uint8_t*)&start_ctn_idx, sizeof(uint64_t), &start_ctn_id));

        uint64_t cur_ctn_idx = start_ctn_idx;
        struct xnitemid cur_ctn_id;
        xn_ensure(xnctn_insert(&meta_ctn, (uint8_t*)&cur_ctn_idx, sizeof(uint64_t), &cur_ctn_id));
    }

    return xn_ok();
}

static xnresult_t xnhp_get_first_ctn(struct xnhp hp, struct xnctn *ctn) {
    xnmm_init();

    struct xnctn meta_ctn;
	xn_ensure(xnctn_open(&meta_ctn, hp.meta, hp.tx));

    uint64_t first_page_idx;
    struct xnitemid id = { .pg_idx = 1 /*heap metadata page*/, .arr_idx = 0 /*current container idx*/ }; 
    xn_ensure(xnctn_get(&meta_ctn, id, (uint8_t*)&first_page_idx, sizeof(uint64_t)));

	struct xnpg first_pg = { .file_handle = hp.meta.file_handle, .idx = first_page_idx };
	xn_ensure(xnctn_open(ctn, first_pg, hp.tx));
    return xn_ok();
}

static xnresult_t xnhp_get_current_ctn(struct xnhp *hp, struct xnctn* out_ctn) {
    xnmm_init();

    struct xnctn meta_ctn;
	xn_ensure(xnctn_open(&meta_ctn, hp->meta, hp->tx));

    uint64_t cur_page_idx;
    struct xnitemid id = { .pg_idx = 1 /*heap metadata page*/, .arr_idx = 1 /*current container idx*/ }; 
    xn_ensure(xnctn_get(&meta_ctn, id, (uint8_t*)&cur_page_idx, sizeof(uint64_t)));

	struct xnpg cur_pg = { .file_handle = hp->meta.file_handle, .idx = cur_page_idx };
	xn_ensure(xnctn_open(out_ctn, cur_pg, hp->tx));
    return xn_ok();
}

static xnresult_t xnhp_set_current_ctn(struct xnhp *hp, uint64_t cur_page_idx) {
	xnmm_init();

    struct xnctn meta_ctn;
	xn_ensure(xnctn_open(&meta_ctn, hp->meta, hp->tx));
	struct xnitemid new_id;
    struct xnitemid id = { .pg_idx = 1 /*heap metadata page*/, .arr_idx = 1 /*current container idx*/ }; 
	xn_ensure(xnctn_update(&meta_ctn, id, (uint8_t*)&cur_page_idx, sizeof(uint64_t), &new_id));
	xn_ensure(id.pg_idx == new_id.pg_idx && id.arr_idx == new_id.arr_idx);

	return xn_ok();
}

static xnresult_t xnhp_append_container(struct xnhp *hp, struct xnctn *out_ctn) {
    xnmm_init();

    struct xnpg new_page;
    xn_ensure(xnfile_allocate_page(hp->meta.file_handle, hp->tx, &new_page));
	xn_ensure(xnctn_open(out_ctn, new_page, hp->tx));
    xn_ensure(xnctn_init(out_ctn));
	xn_ensure(xnhp_set_current_ctn(hp, new_page.idx));

    return xn_ok();
}

static xnresult_t xnhp_get_ctn(struct xnhp *hp, struct xnctn *ctn, uint64_t pg_idx) {
    xnmm_init();

	struct xnpg pg = { .file_handle = hp->meta.file_handle, .idx = pg_idx };

	xn_ensure(xnctn_open(ctn, pg, hp->tx));

    return xn_ok();
}

xnresult_t xnhp_put(struct xnhp *hp, uint8_t *buf, size_t size, struct xnitemid *id) {
    xnmm_init();

    struct xnctn ctn;
    xn_ensure(xnhp_get_current_ctn(hp, &ctn));

    bool can_fit;
    xn_ensure(xnctn_can_fit(&ctn, size, &can_fit));
    if (!can_fit) {
        xn_ensure(xnhp_append_container(hp, &ctn));
    }

    xn_ensure(xnctn_insert(&ctn, buf, size, id));

    return xn_ok();
}

xnresult_t xnhp_get_size(struct xnhp *hp, struct xnitemid id, size_t *out_size) {
    xnmm_init();

    struct xnctn ctn;
    xn_ensure(xnhp_get_ctn(hp, &ctn, id.pg_idx));
    xn_ensure(xnctn_get_size(&ctn, id, out_size));

    return xn_ok();
}

xnresult_t xnhp_get(struct xnhp *hp, struct xnitemid id, uint8_t *val, size_t size) {
    xnmm_init();

    struct xnctn ctn;
    xn_ensure(xnhp_get_ctn(hp, &ctn, id.pg_idx));
    xn_ensure(xnctn_get(&ctn, id, val, size));

    return xn_ok();
}

xnresult_t xnhp_del(struct xnhp *hp, struct xnitemid id) {
    xnmm_init();

    struct xnctn ctn;
    xn_ensure(xnhp_get_ctn(hp, &ctn, id.pg_idx));
    xn_ensure(xnctn_delete(&ctn, id));

    return xn_ok();
}

xnresult_t xnhpscan_open(struct xnhpscan *scan, struct xnhp hp) {
	xnmm_init();
	scan->hp = hp;

	struct xnctn ctn;
	xn_ensure(xnhp_get_first_ctn(hp, &ctn));

	xn_ensure(xnctnitr_init(&scan->ctnitr, ctn));

	return xn_ok();
}

xnresult_t xnhpscan_next(struct xnhpscan *scan, bool *result) {
	xnmm_init();

	bool ctn_result;
	xn_ensure(xnctnitr_next(&scan->ctnitr, &ctn_result));
	if (ctn_result) {
		*result = true;
		return xn_ok();
	}

	uint64_t next_pg_idx = scan->ctnitr.ctn.pg.idx + 1;
    struct xnctn last_ctn;
    xn_ensure(xnhp_get_current_ctn(&scan->hp, &last_ctn));

	if (last_ctn.pg.idx < next_pg_idx) {
		*result = false;
		return xn_ok();
	}

	struct xnctn next_ctn;
	struct xnpg next_pg = { .file_handle = scan->hp.meta.file_handle, .idx = next_pg_idx };
	xn_ensure(xnctn_open(&next_ctn, next_pg, scan->hp.tx));

	xn_ensure(xnctnitr_init(&scan->ctnitr, next_ctn));
	xn_ensure(xnctnitr_next(&scan->ctnitr, &ctn_result));
	if (ctn_result) {
		*result = true;
		return xn_ok();
	}

	*result = false;
	return xn_ok();
}

xnresult_t xnhpscan_itemid(struct xnhpscan *scan, struct xnitemid *id) {
	xnmm_init();
	xn_ensure(xnctnitr_itemid(&scan->ctnitr, id));
	return xn_ok();
}

