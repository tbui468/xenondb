#define _GNU_SOURCE

#include "file.h"
#include "util.h"
#include "log.h"
#include "page.h"
#include "table.h"
#include "tx.h"
#include "db.h"

#include <stdlib.h> //free
#include <pthread.h>
#include <stdint.h>


struct data {
    struct xndb *db;
    int i;
};

void *fcn(void *arg) {
    struct data* d = (struct data*)arg;
    int i = d->i;
    struct xndb *db = d->db;
    free(arg);

    if (i % 2 == 0) {
        struct xntx *tx;
        if (!xntx_create(db, XNTXMODE_WR, &tx))
            printf("failed\n");
        struct xnpg page;

        struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
        if (!xnpgr_allocate_page(&meta_page, tx, &page))
            printf("failed\n");
        if (!xntx_commit(tx))
            printf("failed\n");
    } else {
        struct xntx *tx;
        if (!xntx_create(db, XNTXMODE_RD, &tx))
            printf("failed\n");
        struct xnpg page = { .file_handle = tx->db->file_handle, .idx = 0 };

        uint8_t *buf = malloc(XNPG_SZ);
        if (!xnpg_read(&page, tx, buf, 0, XNPG_SZ))
            printf("failed\n");
        printf("pages: %d\n", *buf);
        free(buf);
        if (!xntx_free(tx))
            printf("failed\n");
    }
}

int main(int argc, char** argv) {
    struct xndb *db;
    if (!xndb_create("students", &db)) {
        printf("xndb_create failed\n");
        exit(1);
    }
    /*
    //multi-threaded test
    const int THREAD_COUNT = 24;
    pthread_t threads[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; i++) {
        struct data *p = malloc(sizeof(struct data));
        p->db = db;
        p->i = i;
        pthread_create(&threads[i], NULL, fcn, p);
    }

    for (int i = 0; i < THREAD_COUNT; i++) {
        pthread_join(threads[i], NULL);
    }*/

    //single threaded test
    {
        struct xntx *tx;
        if (!xntx_create(db, XNTXMODE_WR, &tx)) {
            printf("xntx_create failed\n");
            exit(1);
        }
        struct xnpg page;
        struct xnpg meta_page = {.file_handle = tx->db->file_handle, .idx = 0 };
        if (!xnpgr_allocate_page(&meta_page, tx, &page)) {
            printf("xntx_allocate_page failed\n");
            exit(1);
        }
        if (!xnpgr_free_page(&meta_page, tx, page)) {
            printf("xntx_free_page failed\n");
            exit(1);
        }
        if (!xnpgr_allocate_page(&meta_page, tx, &page)) {
            printf("xntx_allocate_page failed\n");
            exit(1);
        }
        if (!xntx_commit(tx)) {
            printf("xntx_commit failed\n");
            exit(1);
        }
    }

    /*
    { 
        struct xntx *tx;
        if (!xntx_create(db, XNTXMODE_WR, &tx))
            printf("failed\n");
        struct xnpg page;
        if (!xntx_allocate_page(tx, &page))
            printf("failed\n");
        if (!xntx_commit(tx))
            printf("failed\n");
    }*/

    if (!xndb_free(db)) {
        printf("xndb_free failed\n");
        exit(1);
    }
    return 0;
}
