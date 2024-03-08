#include "page.h"
#include "file.h"
#include "util.h"

xnresult_t xnpg_write(struct xnpg *page, const uint8_t *buf) {
    xnmm_init();
    xn_ensure(xnfile_write(page->file_handle, buf, page->idx * XNPG_SZ, XNPG_SZ)); 
    return xn_ok();
}

xnresult_t xnpg_read(struct xnpg *page, uint8_t *buf) {
    xnmm_init();
    xn_ensure(xnfile_read(page->file_handle, buf, page->idx * XNPG_SZ, XNPG_SZ));
    return xn_ok();
}

xnresult_t xnpg_mmap(struct xnpg *page, uint8_t **ptr) {
    xnmm_init();
    xn_ensure(xnfile_mmap(page->file_handle, page->idx * XNPG_SZ, XNPG_SZ, (void**)ptr));
    return xn_ok();
}

xnresult_t xnpg_munmap(uint8_t *ptr) {
    xnmm_init();
    xn_ensure(xnfile_munmap((void*)ptr, XNPG_SZ));
    return xn_ok();
}
