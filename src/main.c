//for O_DIRECT
#define _GNU_SOURCE

#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <assert.h>

#include "util.h"


#define XN_BLK_SZ 4096
#define XN_BLKHDR_SZ 32

#define XN_PGR_MAXPGCOUNT 2

//#define XN_BUCKETS 101
#define XN_BUCKETS 3


struct xnstatus {
    bool ok;
    char* msg;
};

enum xnlogt {
    XNLOGT_START,
    XNLOGT_COMMIT,
    XNLOGT_ROLLBACK,
    XNLOGT_WRITE,
    XNLOGT_APPENDBLK
};

enum xnrecf {
    XNRECF_NEXT,
    XNRECF_KEYSZ,
    XNRECF_VALUESZ,
    XNRECF_KEY,
};

enum xnlogf {
    XNLOGF_TYPE,
    XNLOGF_TXID,
    XNLOGF_LSN,
    XNLOGF_WRLOG //exists only if log type is XNLOGT_WRITE
};

//shared header fields for both data and log files
enum xnhdrf {
    XNHDRF_PTRCOUNT,
    XNHDRF_SLTSTOP,
    XNHDRF_FREELISTHEAD,
    XNHDRF_OVERFLOWBLOCK
};

//shared ptr fields for both data and log files
enum xnptrf {
    XNPTRF_SLOTOFF
};


//xenon db
struct xndb {
    char dir_path[256];
    char data_path[256];
    char log_path[256];
    struct xnpgr *pager;
    struct xnlgr *logger;
    int tx_id;
};

struct xnpg {
    char buf[XN_BLK_SZ];
    int pins;
    const char *filename;
    int block_idx;

    //used in freelist
    int cur;
    int prev;
};

struct xnpgr {
    struct xnpg *pages;
};

struct xnlgr {
    int lsn;
    char log_path[256];
    struct xnpgr *pager;
};

struct xnwrlg {
    int offset;
    void *old_data;
    void *new_data;
    int data_size;
    int block_idx;
};

struct xntx {
    int id;
    struct xnpgr *pager;
    struct xnlgr *logger;
    const char *data_path;
};

struct xnsltitr {
    int slt_idx;
    int slt_count;
    struct xnpg *page;
};

struct xnblkitr {
    int block_idx;
    int block_count;
};

struct xnitr {
    struct xnblkitr blkitr;
    struct xnsltitr sltitr;
    struct xnpgr *pager;
    const char *filename;
};


void xnpgr_unpin(struct xnpg *page);
struct xnpg *xnpgr_pin(struct xnpgr *pager, const char* filename, int block_idx);

void xnlog_read(void *result, enum xnlogf fld, char *buf);

void xnlgr_init(struct xnlgr *logger, const char* log_path, struct xnpgr *pager);
struct xnstatus xnlgr_write(struct xnlgr *logger, enum xnlogt log, struct xnwrlg *wrlog_data, int tx_id);
void xndb_init_tx(struct xndb *db, struct xntx *tx);
struct xnstatus xntx_commit(struct xntx *tx);
struct xnstatus xntx_rollback(struct xntx *tx);
struct xnstatus xntx_put(struct xntx *tx, const char *key, const char *value);
struct xnstatus xntx_get(struct xntx *tx, const char *key, char *value);
struct xnstatus xntx_delete(struct xntx *tx, const char *key);

struct xnstatus xntx_do_put(struct xntx *tx, struct xnpg *page, const char *key, const char *value);
struct xnstatus xntx_do_get(struct xntx *tx, struct xnpg *page, const char *key, char *value);
struct xnstatus xntx_do_delete(struct xntx *tx, struct xnpg *page, const char *key);
struct xnstatus xnpg_delete(struct xnpg *page, const char *key);
struct xnstatus xndb_delete(struct xndb *db, const char *key);
void xnpg_write(struct xnpg *page, int off, const void *data, size_t size);
void xnpg_read(struct xnpg *page, int off, void *data, size_t size);

void xntx_redo_write(struct xntx *tx, char *log_buf);
void xntx_undo_write(struct xntx *tx, char *log_buf);

void xnsltitr_after_end(struct xnsltitr *itr, struct xnpg *page);
bool xnsltitr_prev(struct xnsltitr *itr);
void xnsltitr_before_begin(struct xnsltitr *itr, struct xnpg *page);
bool xnsltitr_next(struct xnsltitr *itr);
int xnsltitr_offset(struct xnsltitr *itr);

int xnrecf_offset(enum xnrecf fld, int rec_off);

void xndb_recover(struct xndb *db);

int xnfile_block_count(const char *filename) {
    int fd = xn_open(filename, O_RDWR, 0666);
    return xn_seek(fd, 0, SEEK_END) / XN_BLK_SZ;
}

void xnblkitr_after_end(struct xnblkitr *itr, const char *filename) {
    itr->block_idx = xnfile_block_count(filename);
    itr->block_count = -1; //not used
}

bool xnblkitr_prev(struct xnblkitr *itr) {
    itr->block_idx--;
    return itr->block_idx >= 0;
}

void xnblkitr_before_begin(struct xnblkitr *itr, const char *filename) {
    itr->block_idx = -1;
    itr->block_count = xnfile_block_count(filename);
}

bool xnblkitr_next(struct xnblkitr *itr) {
    itr->block_idx++;
    return itr->block_idx < itr->block_count;
}

int xnblkitr_idx(struct xnblkitr *itr) {
    return itr->block_idx;
}

void xnitr_before_begin(struct xnitr *itr, struct xnpgr *pager, const char *filename) {
    itr->pager = pager;
    itr->filename = filename;

    xnblkitr_before_begin(&itr->blkitr, filename);

    if (xnblkitr_next(&itr->blkitr)) {
        int block_idx = xnblkitr_idx(&itr->blkitr);
        struct xnpg *page = xnpgr_pin(pager, filename, block_idx);
        xnsltitr_before_begin(&itr->sltitr, page);
        xnpgr_unpin(page);
    }
}

bool xnitr_next(struct xnitr *itr) {
    while (!xnsltitr_next(&itr->sltitr)) {
        if (!xnblkitr_next(&itr->blkitr)) {
            return false;
        }

        int block_idx = xnblkitr_idx(&itr->blkitr);
        struct xnpg *page = xnpgr_pin(itr->pager, itr->filename, block_idx);
        xnsltitr_before_begin(&itr->sltitr, page);
        xnpgr_unpin(page);
    }

    return true;
}

void xnitr_after_end(struct xnitr *itr, struct xnpgr *pager, const char *filename) {
    itr->pager = pager;
    itr->filename = filename;

    xnblkitr_after_end(&itr->blkitr, filename);

    if (xnblkitr_prev(&itr->blkitr)) {
        int block_idx = xnblkitr_idx(&itr->blkitr);
        struct xnpg *page = xnpgr_pin(pager, filename, block_idx);
        xnsltitr_after_end(&itr->sltitr, page);
        xnpgr_unpin(page);
    }
}

bool xnitr_prev(struct xnitr *itr) {
    while (!xnsltitr_prev(&itr->sltitr)) {
        if (!xnblkitr_prev(&itr->blkitr)) {
            return false;
        }

        int block_idx = xnblkitr_idx(&itr->blkitr);
        struct xnpg *page = xnpgr_pin(itr->pager, itr->filename, block_idx);
        xnsltitr_after_end(&itr->sltitr, page);
        xnpgr_unpin(page);
    }

    return true;
}

int xnitr_slotoff(struct xnitr *itr) {
    return xnsltitr_offset(&itr->sltitr);
}

int xnitr_blockidx(struct xnitr *itr) {
    return xnblkitr_idx(&itr->blkitr);
}

void xndb_itr(struct xndb *db, struct xnitr *itr) {
    xnitr_before_begin(itr, db->pager, db->data_path);
}

bool xndb_next(struct xnitr *itr) {
    return xnitr_next(itr);
}

void xndb_key(struct xnitr *itr, char *key) {
    int block_idx = xnblkitr_idx(&itr->blkitr);
    struct xnpg *page = xnpgr_pin(itr->pager, itr->filename, block_idx);

    int slot_off = xnsltitr_offset(&itr->sltitr);
    int keysz;
    xnpg_read(page, xnrecf_offset(XNRECF_KEYSZ, slot_off), &keysz, sizeof(int));
    xnpg_read(page, xnrecf_offset(XNRECF_KEY, slot_off), key, keysz);

    xnpgr_unpin(page);
}

void xndb_value(struct xnitr *itr, char *value) {
    int block_idx = xnblkitr_idx(&itr->blkitr);
    struct xnpg *page = xnpgr_pin(itr->pager, itr->filename, block_idx);

    int slot_off = xnsltitr_offset(&itr->sltitr);
    int keysz;
    xnpg_read(page, xnrecf_offset(XNRECF_KEYSZ, slot_off), &keysz, sizeof(int));
    int valsz;
    xnpg_read(page, xnrecf_offset(XNRECF_VALUESZ, slot_off), &valsz, sizeof(int));
    xnpg_read(page, xnrecf_offset(XNRECF_KEY, slot_off) + keysz, value, valsz);

    xnpgr_unpin(page);
}


int xnrecf_offset(enum xnrecf fld, int rec_off) {
    switch (fld) {
    case XNRECF_NEXT:
        return rec_off;
    case XNRECF_KEYSZ:
        return rec_off + sizeof(int);
    case XNRECF_VALUESZ:
        return rec_off + sizeof(int) * 2;
    case XNRECF_KEY:
        return rec_off + sizeof(int) * 3;
    default:
        assert(false);
    }
}

int xnlogf_offset(enum xnlogf fld) {
    switch (fld) {
    case XNLOGF_TYPE:
        return 0;
    case XNLOGF_TXID:
        return sizeof(enum xnlogt);
    case XNLOGF_LSN:
        return sizeof(enum xnlogt) + sizeof(int);
    case XNLOGF_WRLOG: //exists only if log type is XNLOGT_WRITE
        return sizeof(enum xnlogt) + sizeof(int) * 2;
    default:
        assert(false);
    }
}

int xnhdrf_offset(enum xnhdrf fld) {
    switch (fld) {
    case XNHDRF_PTRCOUNT:
        return 0;
    case XNHDRF_SLTSTOP:
        return sizeof(int);
    case XNHDRF_FREELISTHEAD:
        return sizeof(int) * 2;
    case XNHDRF_OVERFLOWBLOCK:
        return sizeof(int) * 3;
    default:
        assert(false);
    }
}

int xnptrf_offset(enum xnptrf fld, int idx) {
    return XN_BLKHDR_SZ + sizeof(int) * idx;
}

void xnpgr_flush(struct xnpg *page) {
    int fd = xn_open(page->filename, O_RDWR, 0666);
    xn_seek(fd, page->block_idx * XN_BLK_SZ, SEEK_SET);
    xn_write(fd, page->buf, XN_BLK_SZ);
    close(fd);
}

void xnpgr_flush_all(struct xnpgr *pager) {
    for (int i = 0; i < XN_PGR_MAXPGCOUNT; i++) {
        struct xnpg *page = &pager->pages[i];
        xnpgr_flush(page);
    }
}


void xnpgr_read_page_from_disk(struct xnpg *page, const char *filename, int block_idx) {
    page->block_idx = block_idx;
    page->filename = filename;
    page->pins = 0;
    int fd = xn_open(filename, O_RDWR, 0666);
    xn_seek(fd, page->block_idx * XN_BLK_SZ, SEEK_SET);
    xn_read(fd, page->buf, XN_BLK_SZ);
    close(fd);
}

void xnpgr_unpin(struct xnpg *page) {
    page->pins--;
}

struct xnpg *xnpgr_pin(struct xnpgr *pager, const char* filename, int block_idx) {
    //page is in buffer
    for (int i = 0; i < XN_PGR_MAXPGCOUNT; i++) {
        struct xnpg *page = &pager->pages[i];
        if (page->block_idx == block_idx && strcmp(filename, page->filename) == 0) {
            page->pins++;
            return page;
        }
    }

    //Naive eviction policy: evict first page with 0 pins
    for (int i = 0; i < XN_PGR_MAXPGCOUNT; i++) {
        struct xnpg *page = &pager->pages[i];
        if (page->pins == 0) {
            xnpgr_flush(page);
            xnpgr_read_page_from_disk(page, filename, block_idx);
            page->pins++;
            return page;
        }
    }

    assert(false && "xenondb: no pages available to evict");
    return NULL;
}


struct xnpgr *xnpgr_create(const char* data_path) {
    assert(XN_BUCKETS >= XN_PGR_MAXPGCOUNT && "buckets + 1 not greater or equal to max page count");

    struct xnpgr *pager = xn_malloc(sizeof(struct xnpgr));
    pager->pages = xn_memalign(XN_PGR_MAXPGCOUNT * sizeof(struct xnpg));

    int fd = xn_open(data_path, O_RDWR, 0666);
    for (int i = 0; i < XN_PGR_MAXPGCOUNT; i++) {
        pager->pages[i].pins = 0;
        pager->pages[i].block_idx = i;
        pager->pages[i].filename = data_path;
        
        xn_seek(fd, i * XN_BLK_SZ, SEEK_SET);
        xn_read(fd, pager->pages[i].buf, XN_BLK_SZ);
    }
    close(fd);

    return pager;
}

void xndb_free(struct xndb* db) {
    xnpgr_flush_all(db->pager);
    free((void*)db);
}

void xndb_make_dir(const char *name) {
    DIR *dir;
    if ((dir = opendir(name))) {
        closedir(dir);
    } else if (errno == ENOENT) {
        if (mkdir(name, 0777) < 0) {
            fprintf(stderr, "xenondb: mkdir failed\n");
            exit(1);
        }
    } else {
        fprintf(stderr, "xenondb: opendir failed\n");
        exit(1);
    }
}

bool xndb_dir_exists(const char* name) {
    DIR *dir;
    if ((dir = opendir(name))) {
        closedir(dir);
        return true;
    } else {
        return false;
    }
}


void xnpg_init_data(struct xnpg *page) {
    memset(page->buf, 0, XN_BLK_SZ);
    page->pins = 0;
    page->block_idx = -1;
    int block_size = XN_BLK_SZ;
    xnpg_write(page, xnhdrf_offset(XNHDRF_SLTSTOP), &block_size, sizeof(int));
}

void xndb_init_file(const char *path, int blocks) {
    int fd = xn_open(path, O_CREAT | O_RDWR, 0666);

    struct xnpg page;
    xnpg_init_data(&page);

    for (int i = 0; i < blocks; i++) {
        xn_write(fd, page.buf, XN_BLK_SZ);
    }

    close(fd);
}

//TODO: this function is a mess and doesn't work if an existing db is opened - clean it up
struct xndb* xndb_init(const char* path, bool make_dir) {
    struct xndb* db = xn_malloc(sizeof(struct xndb));
    //make directory path
    xn_strcpy(db->dir_path, path);

    //make data and log paths
    xn_strcpy(db->data_path, path);
    xn_strcat(db->data_path, "/data");

    xn_strcpy(db->log_path, db->dir_path);
    xn_strcat(db->log_path, "/log");

    if (!xndb_dir_exists(path)) {
        if (make_dir) {
            xndb_make_dir(path);
            xndb_init_file(db->data_path, XN_BUCKETS);
            xndb_init_file(db->log_path, 1);
        } else {
            fprintf(stderr, "xenondb: database directory doesn't exist.");
            exit(1);
        }
    }


    db->tx_id = 0;

    //making the pager
    db->pager = xnpgr_create(db->data_path);

    //making logger
    db->logger = xn_memalign(sizeof(struct xnlgr));
    //xnpg_init_data(&db->logger->page);
    xnlgr_init(db->logger, db->log_path, db->pager);

    xndb_recover(db);

    return db;
}

struct xnstatus xnstatus_create(bool ok, char* msg) {
    struct xnstatus s;
    s.ok = ok;
    s.msg = msg;
    return s;
}

int xndb_get_hash_bucket(const char* key) {
    //TODO replace with a better hash algorithm
    size_t total = 0;
    for (int i = 0; i < strlen(key); i++) {
        total += (i + 1) * key[i];
    }

    return total % XN_BUCKETS;
}

bool xnslt_append(struct xnpg *page, int data_size, int *data_offset) {
    int slot_count;
    xnpg_read(page, xnhdrf_offset(XNHDRF_PTRCOUNT), &slot_count, sizeof(int));
    int data_top;
    xnpg_read(page, xnhdrf_offset(XNHDRF_SLTSTOP), &data_top, sizeof(int));

    data_top -= data_size;
    int new_slt_idx = slot_count;
    slot_count++;

    if (data_top <= XN_BLKHDR_SZ + sizeof(int) * slot_count) return false;

    xnpg_write(page, xnhdrf_offset(XNHDRF_PTRCOUNT), &slot_count, sizeof(int));
    xnpg_write(page, xnhdrf_offset(XNHDRF_SLTSTOP), &data_top, sizeof(int));
    xnpg_write(page, xnptrf_offset(XNPTRF_SLOTOFF, new_slt_idx), &data_top, sizeof(int));

    *data_offset = data_top;
    return true;
}

int xnpg_find_rec_idx(struct xnpg *page, const char *key) {
    struct xnsltitr itr;
    xnsltitr_before_begin(&itr, page);
    while (xnsltitr_next(&itr)) {
        int slot_off = xnsltitr_offset(&itr);
        int key_size;
        xnpg_read(page, xnrecf_offset(XNRECF_KEYSZ, slot_off), &key_size, sizeof(int));
        int key_off = xnrecf_offset(XNRECF_KEY, slot_off);
        if (strlen(key) == key_size && strncmp(page->buf + key_off, key, key_size) == 0) {
            return itr.slt_idx;
        }
    }

    return -1;
}

void xnpg_write(struct xnpg *page, int off, const void *data, size_t size) {
    memcpy(page->buf + off, data, size);
}

void xnpg_read(struct xnpg *page, int off, void *data, size_t size) {
    memcpy(data, page->buf + off, size);
}

void xnpg_init_freeslot_search(struct xnpg *page) {
    page->cur = xnhdrf_offset(XNHDRF_FREELISTHEAD);
    page->prev = 0;
}

bool xnpg_next_freeslot(struct xnpg *page) {
    page->prev = page->cur;
    xnpg_read(page, page->cur, &page->cur, sizeof(int));
    return page->cur != 0;
}

//TODO this is problematic
//if we ever change the order of slot headers, this will break
int xnpg_freeslot_size(struct xnpg *page) {
    int keysz;
    int valsz;
    xnpg_read(page, page->cur + sizeof(int), &keysz, sizeof(int));
    xnpg_read(page, page->cur + sizeof(int) * 2, &valsz, sizeof(int));
    return keysz + valsz;
}

int xnpg_remove_freeslot(struct xnpg *page) {
    int next;
    xnpg_read(page, page->cur, &next, sizeof(int));
    xnpg_write(page, page->prev, &next, sizeof(int));
    return page->cur;
}

//searches for a free slot of exact size in freelist
//if found, removes slot from freelist and returns offset
//otherwise returns -1
int xnpg_get_freeslot_offset(struct xnpg *page, const char *key, const char *value) {
    int target_size = strlen(key) + strlen(value);

    xnpg_init_freeslot_search(page);
    while (xnpg_next_freeslot(page)) {
        if (xnpg_freeslot_size(page) == target_size) {
            int offset = xnpg_remove_freeslot(page);
            return offset;
        }
    }

    return -1;
}

struct xnstatus xntx_write(struct xntx *tx, struct xnpg *page, int offset, const void *data, size_t size) {
    struct xnwrlg log_data;
    log_data.offset = offset;
    log_data.old_data = page->buf + offset;
    log_data.new_data = (void*)data;
    log_data.data_size = size;
    log_data.block_idx = page->block_idx;

    xnlgr_write(tx->logger, XNLOGT_WRITE, &log_data, tx->id);
    xnpg_write(page, offset, data, size);
}

struct xnstatus xntx_read(struct xntx *tx, struct xnpg *page, int offset, void *data, size_t size) {
    //do we need to read this tx's own writes?
    xnpg_read(page, offset, data, size);
}

struct xnstatus xntx_do_put(struct xntx *tx, struct xnpg *page, const char *key, const char *value) {
    int rec_idx = xnpg_find_rec_idx(page, key);

    if (rec_idx != -1) {
        int slot_off;
        xntx_read(tx, page, xnptrf_offset(XNPTRF_SLOTOFF, rec_idx), &slot_off, sizeof(int));
        int old_valsz;
        xntx_read(tx, page, xnrecf_offset(XNRECF_VALUESZ, slot_off), &old_valsz, sizeof(int));
        if (strlen(value) == old_valsz) {
            int keysz;
            xntx_read(tx, page, xnrecf_offset(XNRECF_KEYSZ, slot_off), &keysz, sizeof(int));
            xntx_write(tx, page, xnrecf_offset(XNRECF_KEY, slot_off) + keysz, value, old_valsz);
            return xnstatus_create(true, NULL);
        }

        //if size is not the same, delete record
        //the remaining code in function will add a new record with the proper size
        struct xnstatus s = xntx_do_delete(tx, page, key);
        if (!s.ok)
            return s;
    }

    int free_offset = xnpg_get_freeslot_offset(page, key, value);
    int rec_count;
    xntx_read(tx, page, xnhdrf_offset(XNHDRF_PTRCOUNT), &rec_count, sizeof(int));

    if (free_offset != -1) {
        //reuse old slot that matches size
        xntx_write(tx, page, xnptrf_offset(XNPTRF_SLOTOFF, rec_count), &free_offset, sizeof(int));
    } else {
        //using a new slot
        int datatop;
        xntx_read(tx, page, xnhdrf_offset(XNHDRF_SLTSTOP), &datatop, sizeof(int));
        int dataoff = datatop - (sizeof(int) * 3 + strlen(key) + strlen(value));
        xntx_write(tx, page, xnhdrf_offset(XNHDRF_SLTSTOP), &dataoff, sizeof(int));
        xntx_write(tx, page, xnptrf_offset(XNPTRF_SLOTOFF, rec_count), &dataoff, sizeof(int));
    }

    int next = 0;
    int keysz = strlen(key);
    int valsz = strlen(value);
    int slot_off;
    xntx_read(tx, page, xnptrf_offset(XNPTRF_SLOTOFF, rec_count), &slot_off, sizeof(int));
    xntx_write(tx, page, xnrecf_offset(XNRECF_NEXT, slot_off), &next, sizeof(int));
    xntx_write(tx, page, xnrecf_offset(XNRECF_KEYSZ, slot_off), &keysz, sizeof(int));
    xntx_write(tx, page, xnrecf_offset(XNRECF_VALUESZ, slot_off), &valsz, sizeof(int));
    xntx_write(tx, page, xnrecf_offset(XNRECF_KEY, slot_off), key, keysz);
    xntx_write(tx, page, xnrecf_offset(XNRECF_KEY, slot_off) + keysz, value, valsz);

    rec_count++;
    xntx_write(tx, page, xnhdrf_offset(XNHDRF_PTRCOUNT), &rec_count, sizeof(int));
    return xnstatus_create(true, NULL);
}

struct xnstatus xndb_put(struct xndb *db, const char *key, const char *value) {
    //TODO assert that key/value will fit into a single block

    struct xntx tx;
    xndb_init_tx(db, &tx); 
    struct xnstatus s = xntx_put(&tx, key, value);
    if (s.ok) {
        xntx_commit(&tx);
    } else {
        xntx_rollback(&tx);
    }

    return s;
}


struct xnstatus xntx_do_get(struct xntx *tx, struct xnpg *page, const char *key, char* result) {
    int rec_idx = xnpg_find_rec_idx(page, key);

    if (rec_idx == -1) {
        return xnstatus_create(false, "xenondb: key not found");
    }

    int valsz;
    int rec_off;
    xntx_read(tx, page, xnptrf_offset(XNPTRF_SLOTOFF, rec_idx), &rec_off, sizeof(int));
    xntx_read(tx, page, xnrecf_offset(XNRECF_VALUESZ, rec_off), &valsz, sizeof(int));
    int keysz;
    xntx_read(tx, page, xnrecf_offset(XNRECF_KEYSZ, rec_off), &keysz, sizeof(int));
    xntx_read(tx, page, xnrecf_offset(XNRECF_KEY, rec_off) + keysz, result, valsz);
    result[valsz] = '\0';
    return xnstatus_create(true, NULL);
}

struct xnstatus xndb_get(struct xndb *db, const char *key, char* result) {
    //TODO assert that key fits into a single block
    struct xntx tx;
    xndb_init_tx(db, &tx); 
    struct xnstatus s = xntx_get(&tx, key, result);
    if (s.ok) {
        xntx_commit(&tx);
    } else {
        xntx_rollback(&tx);
    }

    return s;
}

struct xnstatus xntx_do_delete(struct xntx *tx, struct xnpg *page, const char *key) {
    int rec_idx = xnpg_find_rec_idx(page, key);
    if (rec_idx == -1) {
        return xnstatus_create(false, "xenondb: key not found");
    }

    //insert new free record at head of freelist
    //the 'next' field in this record now points to the previous freelist head
    int prev_freelisthead;
    xntx_read(tx, page, xnhdrf_offset(XNHDRF_FREELISTHEAD), &prev_freelisthead, sizeof(int));
    int rec_off;
    xntx_read(tx, page, xnptrf_offset(XNPTRF_SLOTOFF, rec_idx), &rec_off, sizeof(int));
    xntx_write(tx, page, xnrecf_offset(XNRECF_NEXT, rec_off), &prev_freelisthead, sizeof(int));

    int data_off;
    xntx_read(tx, page, xnptrf_offset(XNPTRF_SLOTOFF, rec_idx), &data_off, sizeof(int));
    xntx_write(tx, page, xnhdrf_offset(XNHDRF_FREELISTHEAD), &data_off, sizeof(int));

    int ptr_count;
    xntx_read(tx, page, xnhdrf_offset(XNHDRF_PTRCOUNT), &ptr_count, sizeof(int));

    //copy pointers to the left to remove deleted ptr
    for (int i = rec_idx; i < ptr_count - 1; i++) {
        int right_ptr;
        xntx_read(tx, page, xnptrf_offset(XNPTRF_SLOTOFF, i + 1), &right_ptr, sizeof(int));
        xntx_write(tx, page, xnptrf_offset(XNPTRF_SLOTOFF, i), &right_ptr, sizeof(int));
    }
   
    ptr_count--;
    xntx_write(tx, page, xnhdrf_offset(XNHDRF_PTRCOUNT), &ptr_count, sizeof(int));
    return xnstatus_create(true, NULL);
}

struct xnstatus xndb_delete(struct xndb *db, const char *key) {
    //TODO assert key will fit into a single block
   
    struct xntx tx;
    xndb_init_tx(db, &tx); 
    struct xnstatus s = xntx_delete(&tx, key);
    if (s.ok) {
        xntx_commit(&tx);
    } else {
        xntx_rollback(&tx);
    }

    return s;
}

void xndb_init_tx(struct xndb *db, struct xntx *tx) {
    tx->id = ++(db->tx_id);
    tx->pager = db->pager;
    tx->logger = db->logger;
    tx->data_path = db->data_path;
    xnlgr_write(db->logger, XNLOGT_START, NULL, tx->id);
}

struct xnstatus xntx_commit(struct xntx *tx) {
    xnlgr_write(tx->logger, XNLOGT_COMMIT, NULL, tx->id);
    return xnstatus_create(true, NULL);
}

void xnsltitr_after_end(struct xnsltitr *itr, struct xnpg *page) {
    itr->page = page;
    int slt_idx;
    xnpg_read(page, xnhdrf_offset(XNHDRF_PTRCOUNT), &slt_idx, sizeof(int));
    itr->slt_idx = slt_idx;
    itr->slt_count = -1; //not used
}
bool xnsltitr_prev(struct xnsltitr *itr) {
    itr->slt_idx--;
    return itr->slt_idx >= 0;
}
void xnsltitr_before_begin(struct xnsltitr *itr, struct xnpg *page) {
    itr->page = page;
    itr->slt_idx = -1;
    int slt_count;
    xnpg_read(page, xnhdrf_offset(XNHDRF_PTRCOUNT), &slt_count, sizeof(int));
    itr->slt_count = slt_count;
}
bool xnsltitr_next(struct xnsltitr *itr) {
    itr->slt_idx++;
    return itr->slt_idx < itr->slt_count;
}
int xnsltitr_offset(struct xnsltitr *itr) {
    int off;
    xnpg_read(itr->page, xnptrf_offset(XNPTRF_SLOTOFF, itr->slt_idx), &off, sizeof(int));
    return off;
}

struct xnstatus xntx_rollback(struct xntx *tx) {
    struct xnitr itr;
    xnitr_after_end(&itr, tx->pager, tx->logger->log_path);
    while (xnitr_prev(&itr)) {
        int slot_off = xnitr_slotoff(&itr);
        int block_idx = xnitr_blockidx(&itr);
        struct xnpg *page = xnpgr_pin(tx->pager, tx->logger->log_path, block_idx);

        int tx_id;
        xnlog_read(&tx_id, XNLOGF_TXID, page->buf + slot_off);
        if (tx_id != tx->id) {
            xnpgr_unpin(page);
            continue;
        }

        enum xnlogt type;
        xnlog_read(&type, XNLOGF_TYPE, page->buf + slot_off);
        if (type == XNLOGT_START) {
            xnpgr_unpin(page);
            break;
        }

        if (type != XNLOGT_WRITE) {
            xnpgr_unpin(page);
            continue;
        }

        xntx_undo_write(tx, page->buf + slot_off);
        xnpgr_unpin(page);
    }

    xnlgr_write(tx->logger, XNLOGT_ROLLBACK, NULL, tx->id);
    return xnstatus_create(true, NULL);
}

struct xnstatus xntx_put(struct xntx *tx, const char *key, const char *value) {
    int block_idx = xndb_get_hash_bucket(key);
    //TODO get exclusive lock here using block_idx as lock identifier
    //eg, xncnr_acquire_xlock(tx->concur, block_idx);
    struct xnpg *page = xnpgr_pin(tx->pager, tx->data_path, block_idx);
    struct xnstatus s = xntx_do_put(tx, page, key, value);
    xnpgr_unpin(page);
    //TODO release x-lock
    //eg, xncnr_release_xlock(tx->concur, block_idx);
    return s;
}
struct xnstatus xntx_get(struct xntx *tx, const char *key, char *value) {
    int block_idx = xndb_get_hash_bucket(key);
    //TODO get shared lock here using block_idx as lock identifier
    //eg, xncnr_slock(tx->concur, block_idx);
    struct xnpg *page = xnpgr_pin(tx->pager, tx->data_path, block_idx);
    //TODO write to log here??
    struct xnstatus s = xntx_do_get(tx, page, key, value);
    xnpgr_unpin(page);
    //TODO release s-lock
    return s;
}
struct xnstatus xntx_delete(struct xntx *tx, const char *key) {
    int block_idx = xndb_get_hash_bucket(key);
    //TODO get exclusive lock here using block_idx as lock identifier
    struct xnpg *page = xnpgr_pin(tx->pager, tx->data_path, block_idx);
    struct xnstatus s = xntx_do_delete(tx, page, key);
    xnpgr_unpin(page);
    //TODO release x-lock
    return s;
}

void xnlgr_init(struct xnlgr *logger, const char* log_path, struct xnpgr *pager) {
    logger->lsn = 0;
    xn_strcpy(logger->log_path, log_path);
    logger->pager = pager;
}

int xnlgr_next_lsn(struct xnlgr *logger) {
    //TODO lock
    int lsn = ++logger->lsn;
    //TODO unlock
    return lsn;
}

void xntx_redo_write(struct xntx *tx, char *log_buf) {
    int tx_id;
    xnlog_read(&tx_id, XNLOGF_TXID, log_buf);
    assert(tx->id == tx_id);

    enum xnlogt type;
    xnlog_read(&type, XNLOGF_TYPE, log_buf);
    assert(type == XNLOGF_WRLOG);

    //skip log type, tx id, and lsn
    int offset = sizeof(enum xnlogt) + sizeof(int) * 2;

    int block_idx;
    memcpy(&block_idx, log_buf + offset, sizeof(int));
    offset += sizeof(int);
    int data_off;
    memcpy(&data_off, log_buf + offset, sizeof(int));
    offset += sizeof(int);
    int data_size;
    memcpy(&data_size, log_buf + offset, sizeof(int));
    offset += sizeof(int); //offset is at old data
    offset += data_size; //offset is at new data

    struct xnpg *page = xnpgr_pin(tx->pager, tx->data_path, block_idx);
    xnpg_write(page, data_off, log_buf + offset, data_size);
    xnpgr_unpin(page);
}

void xntx_undo_write(struct xntx *tx, char *log_buf) {
    int tx_id;
    xnlog_read(&tx_id, XNLOGF_TXID, log_buf);
    assert(tx->id == tx_id);

    enum xnlogt type;
    xnlog_read(&type, XNLOGF_TYPE, log_buf);
    assert(type == XNLOGF_WRLOG);

    //skip log type, tx id, and lsn
    int offset = sizeof(enum xnlogt) + sizeof(int) * 2;

    int block_idx;
    memcpy(&block_idx, log_buf + offset, sizeof(int));
    offset += sizeof(int);
    int data_off;
    memcpy(&data_off, log_buf + offset, sizeof(int));
    offset += sizeof(int);
    int data_size;
    memcpy(&data_size, log_buf + offset, sizeof(int));
    offset += sizeof(int); //offset is at old data now

    struct xnpg *page = xnpgr_pin(tx->pager, tx->data_path, block_idx);
    xnpg_write(page, data_off, log_buf + offset, data_size);
    xnpgr_unpin(page);
}

void xnlog_read(void *result, enum xnlogf fld, char *buf) {
    switch (fld) {
    case XNLOGF_TYPE:
        memcpy(result, buf, sizeof(enum xnlogt));
        break;
    case XNLOGF_TXID:
        memcpy(result, buf + sizeof(enum xnlogt), sizeof(int));
        break;
    case XNLOGF_LSN:
        memcpy(result, buf + sizeof(enum xnlogt) + sizeof(int), sizeof(int));
        break;
    case XNLOGF_WRLOG:
        //use xntx_undo_write and xntx_redo_write rather
        //than reading write log data directly
    default:
        assert(false);
        break;
    }
}

void xnlog_write(char *buf, enum xnlogf fld, const void *data) {
    switch (fld) {
    case XNLOGF_TYPE:
        memcpy(buf, data, sizeof(enum xnlogt));
        break;
    case XNLOGF_TXID:
        memcpy(buf + sizeof(enum xnlogt), data, sizeof(int));
        break;
    case XNLOGF_LSN:
        memcpy(buf + sizeof(enum xnlogt) + sizeof(int), data, sizeof(int));
        break;
    case XNLOGF_WRLOG: {//exists only if log type is XNLOGT_WRITE
        struct xnwrlg *wrlg = (struct xnwrlg*)data;
        int offset = sizeof(enum xnlogt) + sizeof(int) * 2;
        memcpy(buf + offset, &wrlg->block_idx, sizeof(wrlg->block_idx));
        offset += sizeof(wrlg->block_idx);
        memcpy(buf + offset, &wrlg->offset, sizeof(wrlg->offset));
        offset += sizeof(wrlg->offset);
        memcpy(buf + offset, &wrlg->data_size, sizeof(wrlg->data_size));
        offset += sizeof(wrlg->data_size);
        memcpy(buf + offset, wrlg->old_data, wrlg->data_size);
        offset += wrlg->data_size;
        memcpy(buf + offset, wrlg->new_data, wrlg->data_size);
        break;
    }
    default:
        assert(false);
        break;
    }
}

struct xnstatus xnlgr_write(struct xnlgr *logger, enum xnlogt log_type, struct xnwrlg *wrlog_data, int tx_id) {
    int lsn = xnlgr_next_lsn(logger);
    int log_size = sizeof(log_type) + sizeof(tx_id) + sizeof(lsn);

    if (log_type == XNLOGT_WRITE) {
        assert(wrlog_data != NULL);
        log_size += sizeof(wrlog_data->block_idx) + 
                    sizeof(wrlog_data->offset) + 
                    sizeof(wrlog_data->data_size) + 
                    wrlog_data->data_size * 2; //old and new data length
    }

    struct xnpg *page = xnpgr_pin(logger->pager, logger->log_path, 0);

    int log_off;
    if (!xnslt_append(page, log_size, &log_off)) {
        printf("debug error: log record does not fit on one page\n");
        //TODO: add an overflow block to page, and append the record there 
    }

    char *buf = page->buf + log_off;
    xnlog_write(buf, XNLOGF_TYPE, &log_type);
    xnlog_write(buf, XNLOGF_TXID, &tx_id);
    xnlog_write(buf, XNLOGF_LSN, &lsn);

    if (log_type == XNLOGT_WRITE) {
        xnlog_write(buf, XNLOGF_WRLOG, wrlog_data);
    }

    xnpgr_unpin(page);
}

void xndb_recover(struct xndb *db) {
    printf("recovering...\n");
    struct xntx tx;
    xndb_init_tx(db, &tx);

    struct xnilist *rb;
    struct xnilist *cm;
    xnilist_init(&rb);
    xnilist_init(&cm);

    struct xnitr itr;
    xnitr_after_end(&itr, tx.pager, tx.logger->log_path);
    while (xnitr_prev(&itr)) {
        printf("looking at a log\n");
        int slot_off = xnitr_slotoff(&itr);
        int block_idx = xnitr_blockidx(&itr);
        struct xnpg *page = xnpgr_pin(tx.pager, tx.logger->log_path, block_idx);

        int tx_id;
        xnlog_read(&tx_id, XNLOGF_TXID, page->buf + slot_off);

        enum xnlogt type;
        xnlog_read(&type, XNLOGF_TYPE, page->buf + slot_off);

        if (type == XNLOGT_ROLLBACK) {
            xnilist_append(rb, tx_id); 
        } else if (type == XNLOGT_COMMIT) {
            xnilist_append(cm, tx_id); 
        } else if (type == XNLOGT_WRITE) {
            if (xnilist_contains(rb, tx_id)) {
                xntx_undo_write(&tx, page->buf + slot_off);
            }
        }

        xnpgr_unpin(page);
    }

    //printf("rb length: %d, cm length: %d\n", rb->count, cm->count);

    struct xnitr fitr;
    xnitr_before_begin(&fitr, tx.pager, tx.logger->log_path);
    while (xnitr_next(&fitr)) {
        int slot_off = xnitr_slotoff(&fitr);
        int block_idx = xnitr_blockidx(&fitr);
        struct xnpg *page = xnpgr_pin(tx.pager, tx.logger->log_path, block_idx);

        int tx_id;
        xnlog_read(&tx_id, XNLOGF_TXID, page->buf + slot_off);

        enum xnlogt type;
        xnlog_read(&type, XNLOGF_TYPE, page->buf + slot_off);

        if (type == XNLOGT_COMMIT) {
            xntx_redo_write(&tx, page->buf + slot_off);
        }

        xnpgr_unpin(page);
    }

    free(rb);
    free(cm);
}

void basic_put_test() {
    struct xndb* db = xndb_init("students", true);

    struct xnstatus s;

    s = xndb_put(db, "cat", "a");
    assert(s.ok && "put 1 failed");

    s = xndb_put(db, "dog", "b");
    assert(s.ok && "put 2 failed");

    s = xndb_put(db, "hamster", "c");
    assert(s.ok && "put 3 failed");

    s = xndb_put(db, "turtle", "d");
    assert(s.ok && "put 2 failed");

    xndb_free(db);

    printf("basic_put_test passed\n");
}

void basic_get_test() {
    struct xndb* db = xndb_init("students", true);

    struct xnstatus s;
    char buf[2];

    s = xndb_put(db, "cat", "a");
    assert(s.ok && "put 1 failed");
    s = xndb_get(db, "cat", buf);
    assert(s.ok && strcmp("a", buf) == 0 && "basic get 0 failed");

    s = xndb_put(db, "dog", "b");
    assert(s.ok && "put 2 failed");
    s = xndb_get(db, "dog", buf);
    assert(s.ok && strcmp("b", buf) == 0 && "basic get 1 failed");

    s = xndb_put(db, "hamster", "c");
    assert(s.ok && "put 3 failed");
    s = xndb_get(db, "hamster", buf);
    assert(s.ok && strcmp("c", buf) == 0 && "basic get 2 failed");

    s = xndb_put(db, "turtle", "d");
    assert(s.ok && "put 2 failed");
    s = xndb_get(db, "turtle", buf);
    assert(s.ok && strcmp("d", buf) == 0 && "basic get 3 failed");

    xndb_free(db);

    printf("basic_get_test passed\n");
}

void basic_delete_test() {
    struct xndb* db = xndb_init("students", true);

    struct xnstatus s;
    char buf[2];

    //delete entry in last position in block
    s = xndb_put(db, "cat", "a");
    assert(s.ok && "put 1 failed");
    s = xndb_delete(db, "cat");
    assert(s.ok && "delete failed");
    s = xndb_get(db, "cat", buf); //TODO this line is breaking
    assert(!s.ok && "delete failed");

    s = xndb_put(db, "dog", "b");
    assert(s.ok && "put 2 failed");

    //delete entry that is not last entry in block
    s = xndb_put(db, "turtle", "d");
    assert(s.ok && "put 2 failed");
    s = xndb_delete(db, "turtle");
    assert(s.ok && "delete failed");
    s = xndb_get(db, "turtle", buf);
    assert(!s.ok && "delete failed");

    s = xndb_get(db, "dog", buf);
    assert(s.ok && strcmp(buf, "b") == 0 && "delete failed");

    xndb_free(db);

    printf("basic_delete_test passed\n");
}

void put_test() {
    struct xndb* db = xndb_init("students", true);

    struct xnstatus s;
    char buf[2];

    s = xndb_put(db, "dog", "c");
    assert(s.ok && "put 3 failed");

    s = xndb_get(db, "dog", buf);
    assert(s.ok && strcmp(buf, "c") == 0 && "put failed");

    s = xndb_put(db, "dog", "d");
    assert(s.ok && "put on existing key failed");

    s = xndb_get(db, "dog", buf);
    assert(s.ok && strcmp(buf, "d") == 0 && "put failed");

    s = xndb_delete(db, "dog");
    s = xndb_get(db, "dog", buf);
    assert(!s.ok && "put failed");

    xndb_free(db);

    printf("put_test passed\n");
}

void iterate_test() {
    struct xndb* db = xndb_init("students", true);

    struct xnstatus status;
    status = xndb_put(db, "cat", "a");
    status = xndb_put(db, "dog", "b");
    status = xndb_put(db, "dog", "d");
    status = xndb_put(db, "bird", "c");
    status = xndb_delete(db, "bird");
    status = xndb_put(db, "ant", "e");

    struct xnitr itr;
    xndb_itr(db, &itr);

    char key[5];
    char value[2];
    int count = 0;
    while (xndb_next(&itr)) {
        xndb_key(&itr, key);
        xndb_value(&itr, value);
        count++;
    }

    assert(count == 3 && "iterate test failed");

    xndb_free(db);

    printf("iterate_test passed\n");
}

void tx_commit_test() {
    struct xndb* db = xndb_init("students", true);
    struct xnstatus s;

    struct xntx tx;
    xndb_init_tx(db, &tx);
    xntx_put(&tx, "cat", "a");
    xntx_commit(&tx);

    char buf[2];
    s = xndb_get(db, "cat", buf);
    assert(s.ok && *buf == 'a' && "tx commit test failed");

    xndb_free(db);

    printf("tx_commit_test passed\n");
}

void tx_rollback_test() {
    struct xndb* db = xndb_init("students", true);
    struct xnstatus s;

    struct xntx tx;
    xndb_init_tx(db, &tx);
    xntx_put(&tx, "cat", "a");
    xntx_rollback(&tx);

    char buf[2];
    s = xndb_get(db, "cat", buf);
    assert(!s.ok && "tx rollback test failed");

    xndb_free(db);

    printf("tx_rollback_test passed\n");
}

void recovery_test() {
    //simulating db crashing
    {
        struct xndb* db = xndb_init("students", true);
        struct xnstatus s;

        {
            struct xntx tx;
            xndb_init_tx(db, &tx);
            xntx_put(&tx, "cat", "a");
            xntx_commit(&tx);
        }

        /*
        {
            struct xntx tx;
            xndb_init_tx(db, &tx);
            xntx_put(&tx, "dog", "b");
            xntx_rollback(&tx);
        }

        {
            struct xntx tx;
            xndb_init_tx(db, &tx);
            xntx_put(&tx, "turtle", "c");
            //not commiting nor rolling back
            //simulating database crashing
        }*/
    }

    /*
    {
        struct xndb* db = xndb_init("students", true);
        struct xnstatus s;

        char buf[2];
        s = xndb_get(db, "cat", buf);
        assert(s.ok && "recovery test failed");

        s = xndb_get(db, "dog", buf);
        assert(!s.ok && "recovery test failed");

        s = xndb_get(db, "turtle", buf);
        assert(!s.ok && "recovery test failed");
        xndb_free(db);
    }*/

    printf("recovery_test passed\n");
}

int main(int argc, char** argv) {
    basic_put_test();
    system("exec rm -rf students");
    basic_get_test();
    system("exec rm -rf students");
    basic_delete_test();
    system("exec rm -rf students");
    put_test();
    system("exec rm -rf students");
    iterate_test();
    system("exec rm -rf students");
    tx_commit_test();
    system("exec rm -rf students");
    tx_rollback_test();
    system("exec rm -rf students");
//    recovery_test();
//    system("exec rm -rf students");
    return 0;
}
