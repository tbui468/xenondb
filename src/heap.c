#include "heap.h"
#include <string.h>

xnresult_t xnhp_open(struct xnhp **out_hp, const char* path, bool create, struct xntx *tx) {
    xnmm_init();

    struct xnhp *hp;
    xnmm_alloc(xn_free, xn_malloc, (void**)&hp, sizeof(struct xnhp));
    xnmm_alloc(xnfile_close, xnfile_create, &hp->meta.file_handle, path, create, false); //TODO any reason to set 'direct' flag to true?
    hp->meta.idx = 1;

    if (create) {
        xn_ensure(xnfile_set_size(hp->meta.file_handle, XNPG_SZ * 32));
        struct xnpg heap_meta_page;

        xn_ensure(xnfile_init(hp->meta.file_handle, tx));
        xn_ensure(xnfile_allocate_page(hp->meta.file_handle, tx, &heap_meta_page));

        //metadata container
        xnmm_scoped_alloc(scoped_ptr, xnctn_free, xnctn_open, (struct xnctn**)&scoped_ptr, heap_meta_page, true, tx);
        struct xnctn *ctn = (struct xnctn*)scoped_ptr;

        //first data container
        struct xnpg data_page;
        {
            xn_ensure(xnfile_allocate_page(hp->meta.file_handle, tx, &data_page)); 
            xnmm_scoped_alloc(scoped_ptr, xnctn_free, xnctn_open, (struct xnctn**)&scoped_ptr, data_page, true, tx);
        }

        //set heap file metadata (only 2 for now)
        uint64_t start_ctn_idx = data_page.idx;
        struct xnitemid start_ctn_id;
        xn_ensure(xnctn_insert(ctn, tx, (uint8_t*)&start_ctn_idx, sizeof(uint64_t), &start_ctn_id));

        uint64_t cur_ctn_idx = start_ctn_idx;
        struct xnitemid cur_ctn_id;
        xn_ensure(xnctn_insert(ctn, tx, (uint8_t*)&cur_ctn_idx, sizeof(uint64_t), &cur_ctn_id));
    }

    *out_hp = hp;
    return xn_ok();
}

bool xnhp_free(void **h) {
    xnmm_init();
    struct xnhp *hp = (struct xnhp*)(*h);
    xn_ensure(xnfile_close((void**)&hp->meta.file_handle));
    xn_free(h);

    return xn_ok();
}

xnresult_t xnhp_insert(struct xnhp *hp, struct xntx *tx, uint8_t *buf, size_t size, struct xnitemid *id) {
//    xn_ensure(false); 
    //TODO implement this
    //attempt to insert into cur_ctn_idx
    //if there isn't enough space, allocate a new container and insert it there
    return false;
}
