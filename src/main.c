//for O_DIRECT
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/stat.h>
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
    XNRECF_VALUE
};

enum xnlogf {
    XNLOGF_TYPE,
    XNLOGF_TXID,
    XNLOGF_LSN,
    XNLOGF_WRLOG //exists only if log type is XNLOGT_WRITE
};

enum xnfld {
    //data page
    XNFLD_HDR_PTRCOUNT,
    XNFLD_HDR_SLTSTOP,
    XNFLD_HDR_FREELISTHEAD,
    XNFLD_HDR_OVERFLOWBLOCK,
    XNFLD_PTR_SLTOFF,
    XNFLD_SLT_NEXT,
    XNFLD_SLT_KEYSZ,
    XNFLD_SLT_VALUESZ,
    XNFLD_SLT_KEY,
    XNFLD_SLT_VALUE,

    //log page
    XNFLD_LOG_LOGTOP,
    XNFLD_LOG_TYPE,
    XNFLD_LOG_TXID,
    XNFLD_LOG_LSN
};

//xenon db
struct xndb {
    char dir_path[256];
    char data_path[256];
    struct xnpgr *pager;
    struct xnlgr *logger;
    int tx_id;
};

struct xnpg {
    char buf[XN_BLK_SZ];
    int pins;
    int block_idx;

    //fields used to search freelist for a slot
    //should really be moved elsewhere
    int16_t cur;
    int16_t prev;
};

struct xnpgr {
    struct xnpg pages[XN_PGR_MAXPGCOUNT];
    char data_path[256];
};

struct xnlgr {
    struct xnpg page;
    int lsn;
    char log_path[256];
};

struct xnwrlg {
    int16_t offset;
    const void *old_data;
    const void *new_data;
    int data_size;
    int block_idx;
};

struct xnlgrdr {
    int block_idx;
    int offset;
};

struct xnitr {
    int block_idx;
    int rec_idx;
    struct xnpgr *pager;
};

struct xntx {
    int id;
    struct xnpgr *pager;
    struct xnlgr *logger;
};

void xnlgr_init_lgrdr(struct xnlgr *logger, struct xnlgrdr *lgrdr);
bool xnlgrdr_next(struct xnlgrdr *lgrdr);
void xnlgrdr_read(struct xnlgrdr *lgrdr, enum xnfld fld, void *data, size_t size);
struct xnwrlg *xnlgrdr_read_write_log(struct xnlgrdr *lgrdr);
void xnlgrdr_free_write_log(struct xnwrlg *wrlg);

void xnlgr_init(struct xnlgr *logger, const char* log_path);
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
void xnpg_write(struct xnpg *page, int16_t off, const void *data, size_t size);
void xnpg_read(struct xnpg *page, int16_t off, void *data, size_t size);
int16_t xnpg_fld_offset(struct xnpg *page, enum xnfld fld, int slot);

struct xnstatus xnlgr_flush(struct xnlgr *logger);

void xnpgr_flush(struct xnpgr *pager, struct xnpg *page) {
    int fd = xn_open(pager->data_path, O_RDWR, 0666);
    xn_seek(fd, page->block_idx * XN_BLK_SZ, SEEK_SET);
    xn_write(fd, page->buf, XN_BLK_SZ);
    close(fd);
}

void xnpgr_flush_all(struct xnpgr *pager) {
    for (int i = 0; i < XN_PGR_MAXPGCOUNT; i++) {
        struct xnpg *page = &pager->pages[i];
        xnpgr_flush(pager, page);
    }
}


void xnpgr_read_page_from_disk(struct xnpgr *pager, struct xnpg *page, int block_idx) {
    page->block_idx = block_idx;
    page->pins = 0;
    int fd = xn_open(pager->data_path, O_RDWR, 0666);
    xn_seek(fd, page->block_idx * XN_BLK_SZ, SEEK_SET);
    xn_read(fd, page->buf, XN_BLK_SZ);
    close(fd);
}

void xnpgr_unpin(struct xnpgr *pager, struct xnpg *page) {
    page->pins--;
}

struct xnpg *xnpgr_pin(struct xnpgr *pager, int block_idx) {
    //page is in buffer
    for (int i = 0; i < XN_PGR_MAXPGCOUNT; i++) {
        struct xnpg *page = &pager->pages[i];
        if (page->block_idx == block_idx) {
            page->pins++;
            return page;
        }
    }

    //Naive eviction policy: evict first page with 0 pins
    for (int i = 0; i < XN_PGR_MAXPGCOUNT; i++) {
        struct xnpg *page = &pager->pages[i];
        if (page->pins == 0) {
            xnpgr_flush(pager, page);
            xnpgr_read_page_from_disk(pager, page, block_idx);
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

    memcpy(pager->data_path, data_path, strlen(data_path));
    pager->data_path[strlen(data_path)] = '\0';

    int fd = xn_open(pager->data_path, O_RDWR, 0666);
    for (int i = 0; i < XN_PGR_MAXPGCOUNT; i++) {
        pager->pages[i].pins = 0;
        pager->pages[i].block_idx = i;
        
        xn_seek(fd, i * XN_BLK_SZ, SEEK_SET);
        xn_read(fd, pager->pages[i].buf, XN_BLK_SZ);
    }
    close(fd);

    return pager;
}

void xndb_free(struct xndb* db) {
    xnlgr_flush(db->logger);
    xnpgr_flush_all(db->pager);
    xn_free((void*)db);
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
    int16_t block_size = XN_BLK_SZ;
    xnpg_write(page, xnpg_fld_offset(page, XNFLD_HDR_SLTSTOP, -1), &block_size, sizeof(int16_t));
}

//TODO: this function is a mess and doesn't work if an existing db is opened - clean it up
struct xndb* xndb_init(char* path, bool make_dir) {
    if (make_dir) {
        xndb_make_dir(path);
    }

    if (!xndb_dir_exists(path)) {
        fprintf(stderr, "xenondb: database directory doesn't exist.");
        exit(1);
    }

    struct xndb* db = xn_malloc(sizeof(struct xndb));

    db->tx_id = 0;

    //make directory path
    memcpy(db->dir_path, path, strlen(path));
    db->dir_path[strlen(path)] = '\0';

    //make data path
    int len = strlen(db->dir_path);
    memcpy(db->data_path, db->dir_path, len);
    db->data_path[len] = '/';
    len++;
    memcpy(db->data_path + len, "data", 4);
    len += 4;
    db->data_path[len] = '\0';

    int fd = xn_open(db->data_path, O_CREAT | O_RDWR, 0666);

    struct xnpg page;
    xnpg_init_data(&page);

    for (int i = 0; i < XN_BUCKETS; i++) {
        xn_write(fd, page.buf, XN_BLK_SZ);
    }

    close(fd);

    //logger page buffer needs to be memory aligned for O_DIRECT to be used
    struct stat fstat;
    stat(db->data_path, &fstat);
    size_t block_size = fstat.st_blksize;

    posix_memalign((void**)&db->logger, block_size, sizeof(struct xnlgr));
    xnpg_init_data(&db->logger->page);

    char log_path[256];
    len = strlen(db->dir_path);
    memcpy(log_path, db->dir_path, len);
    log_path[len] = '/';
    len++;
    memcpy(log_path + len, "log", 3);
    len += 3;
    log_path[len] = '\0';
    xnlgr_init(db->logger, log_path);

    //making the pager
    db->pager = xnpgr_create(db->data_path);

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

bool xnslt_append(struct xnpg *page, int16_t data_size, int16_t *data_offset) {
    int16_t slot_count;
    xnpg_read(page, xnpg_fld_offset(page, XNFLD_HDR_PTRCOUNT, -1), &slot_count, sizeof(int16_t));
    int16_t data_top;
    xnpg_read(page, xnpg_fld_offset(page, XNFLD_HDR_SLTSTOP, -1), &data_top, sizeof(int16_t));

    data_top -= data_size;
    slot_count++;

    if (data_top <= XN_BLKHDR_SZ + sizeof(int16_t) * slot_count) return false;

    xnpg_write(page, xnpg_fld_offset(page, XNFLD_HDR_PTRCOUNT, -1), &slot_count, sizeof(int16_t));
    xnpg_write(page, xnpg_fld_offset(page, XNFLD_HDR_SLTSTOP, -1), &data_top, sizeof(int16_t));

    *data_offset = data_top;
    return true;
}

int xnpg_find_rec_idx(struct xnpg *page, const char *key) {
    int16_t rec_count;
    xnpg_read(page, xnpg_fld_offset(page, XNFLD_HDR_PTRCOUNT, -1), &rec_count, sizeof(int16_t));
    for (int i = 0; i < rec_count; i++) {
        int16_t key_size;
        xnpg_read(page, xnpg_fld_offset(page, XNFLD_SLT_KEYSZ, i), &key_size, sizeof(int16_t));
        char buf[key_size + 1];
        xnpg_read(page, xnpg_fld_offset(page, XNFLD_SLT_KEY, i), buf, key_size);
        buf[key_size] = '\0';
        if (strcmp(key, buf) == 0 )
            return i;
    }

    return -1;
}

void xnpg_write(struct xnpg *page, int16_t off, const void *data, size_t size) {
    memcpy(page->buf + off, data, size);
}

void xnpg_read(struct xnpg *page, int16_t off, void *data, size_t size) {
    memcpy(data, page->buf + off, size);
}


int16_t xnpg_fld_offset(struct xnpg *page, enum xnfld fld, int slot) {
    switch (fld) {
    case XNFLD_HDR_FREELISTHEAD:
        assert(slot == -1 && "xenondb: slot idx not used for header fields");
        return 0;
    case XNFLD_HDR_PTRCOUNT:
        assert(slot == -1 && "xenondb: slot idx not used for header fields");
        return sizeof(int16_t);
    case XNFLD_HDR_SLTSTOP:
        assert(slot == -1 && "xenondb: slot idx not used for header fields");
        return sizeof(int16_t) * 2;
    case XNFLD_HDR_OVERFLOWBLOCK:
        assert(slot == -1 && "xenondb: slot idx not used for header fields");
        return sizeof(int16_t) * 3;
    case XNFLD_PTR_SLTOFF:
        assert(slot > -1 && "xenondb: slot idx must be 0 or large");
        return XN_BLKHDR_SZ + sizeof(int16_t) * slot;
    case XNFLD_SLT_NEXT: {
        assert(slot > -1 && "xenondb: slot idx must be 0 or large");
        int16_t slot_off;
        xnpg_read(page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, slot), &slot_off, sizeof(int16_t));
        return slot_off;
    }
    case XNFLD_SLT_KEYSZ: {
        assert(slot > -1 && "xenondb: slot idx must be 0 or large");
        int16_t slot_off;
        xnpg_read(page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, slot), &slot_off, sizeof(int16_t));
        return slot_off + sizeof(int16_t);
    }
    case XNFLD_SLT_VALUESZ: {
        assert(slot > -1 && "xenondb: slot idx must be 0 or large");
        int16_t slot_off;
        xnpg_read(page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, slot), &slot_off, sizeof(int16_t));
        return slot_off + sizeof(int16_t) * 2;
    }
    case XNFLD_SLT_KEY: {
        assert(slot > -1 && "xenondb: slot idx must be 0 or large");
        int16_t slot_off;
        xnpg_read(page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, slot), &slot_off, sizeof(int16_t));
        return slot_off + sizeof(int16_t) * 3;
    }
    case XNFLD_SLT_VALUE: {
        assert(slot > -1 && "xenondb: slot idx must be 0 or large");
        int16_t slot_off;
        xnpg_read(page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, slot), &slot_off, sizeof(int16_t));
        int16_t keysz;
        xnpg_read(page, xnpg_fld_offset(page, XNFLD_SLT_KEYSZ, slot), &keysz, sizeof(int16_t));
        return slot_off + sizeof(int16_t) * 3 + keysz;
    }
    case XNFLD_LOG_LOGTOP: {
        assert(slot == -1 && "xenondb: slot idx not used for log header fields");
        return 0;
    }
    default:
        assert(false && "xenondb: invalid field");
        break;
    }
}


void xnpg_init_freeslot_search(struct xnpg *page) {
    page->cur = xnpg_fld_offset(page, XNFLD_HDR_FREELISTHEAD, -1);
    page->prev = 0;
}

bool xnpg_next_freeslot(struct xnpg *page) {
    page->prev = page->cur;
    xnpg_read(page, page->cur, &page->cur, sizeof(int16_t));
    return page->cur != 0;
}

//TODO this is problematic
//if we ever change the order of slot headers, this will break
int16_t xnpg_freeslot_size(struct xnpg *page) {
    int16_t keysz;
    int16_t valsz;
    xnpg_read(page, page->cur + sizeof(int16_t), &keysz, sizeof(int16_t));
    xnpg_read(page, page->cur + sizeof(int16_t) * 2, &valsz, sizeof(int16_t));
    return keysz + valsz;
}

int16_t xnpg_remove_freeslot(struct xnpg *page) {
    int16_t next;
    xnpg_read(page, page->cur, &next, sizeof(int16_t));
    xnpg_write(page, page->prev, &next, sizeof(int16_t));
    return page->cur;
}

//searches for a free slot of exact size in freelist
//if found, removes slot from freelist and returns offset
//otherwise returns -1
int16_t xnpg_get_freeslot_offset(struct xnpg *page, const char *key, const char *value) {
    int16_t target_size = strlen(key) + strlen(value);

    xnpg_init_freeslot_search(page);
    while (xnpg_next_freeslot(page)) {
        if (xnpg_freeslot_size(page) == target_size) {
            int16_t offset = xnpg_remove_freeslot(page);
            return offset;
        }
    }

    return -1;
}

struct xnstatus xntx_write(struct xntx *tx, struct xnpg *page, int16_t offset, const void *data, size_t size) {
    struct xnwrlg log_data;
    log_data.offset = offset;
    log_data.old_data = page->buf + offset;
    log_data.new_data = data;
    log_data.data_size = size;
    log_data.block_idx = page->block_idx;

    xnlgr_write(tx->logger, XNLOGT_WRITE, &log_data, tx->id);
    xnpg_write(page, offset, data, size);
}

struct xnstatus xntx_read(struct xntx *tx, struct xnpg *page, int16_t offset, void *data, size_t size) {
    //do we need to read this tx's own writes?
    xnpg_read(page, offset, data, size);
}

struct xnstatus xntx_do_put(struct xntx *tx, struct xnpg *page, const char *key, const char *value) {
    int rec_idx = xnpg_find_rec_idx(page, key);

    if (rec_idx != -1) {
        int16_t old_valsz;
        xntx_read(tx, page, xnpg_fld_offset(page, XNFLD_SLT_VALUESZ, rec_idx), &old_valsz, sizeof(int16_t));
        if (strlen(value) == old_valsz) {
            xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_SLT_VALUE, rec_idx), value, old_valsz);
            return xnstatus_create(true, NULL);
        }

        //if size is not the same, delete record
        //the remaining code in function will add a new record with the proper size
        struct xnstatus s = xntx_do_delete(tx, page, key);
        if (!s.ok)
            return s;
    }

    int16_t free_offset = xnpg_get_freeslot_offset(page, key, value);
    int16_t rec_count;
    xntx_read(tx, page, xnpg_fld_offset(page, XNFLD_HDR_PTRCOUNT, -1), &rec_count, sizeof(int16_t));

    if (free_offset != -1) {
        //reuse old slot that matches size
        xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, rec_count), &free_offset, sizeof(int16_t));
    } else {
        //using a new slot
        int16_t datatop;
        xntx_read(tx, page, xnpg_fld_offset(page, XNFLD_HDR_SLTSTOP, -1), &datatop, sizeof(int16_t));
        int16_t dataoff = datatop - (sizeof(int16_t) * 3 + strlen(key) + strlen(value));
        xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_HDR_SLTSTOP, -1), &dataoff, sizeof(int16_t));
        xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, rec_count), &dataoff, sizeof(int16_t));
    }

    int16_t next = 0;
    int16_t keysz = strlen(key);
    int16_t valsz = strlen(value);
    xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_SLT_NEXT, rec_count), &next, sizeof(int16_t));
    xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_SLT_KEYSZ, rec_count), &keysz, sizeof(int16_t));
    xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_SLT_VALUESZ, rec_count), &valsz, sizeof(int16_t));
    xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_SLT_KEY, rec_count), key, keysz);
    xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_SLT_VALUE, rec_count), value, valsz);

    rec_count++;
    xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_HDR_PTRCOUNT, -1), &rec_count, sizeof(int16_t));
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

    int16_t valsz;
    xntx_read(tx, page, xnpg_fld_offset(page, XNFLD_SLT_VALUESZ, rec_idx), &valsz, sizeof(int16_t));
    xntx_read(tx, page, xnpg_fld_offset(page, XNFLD_SLT_VALUE, rec_idx), result, valsz);
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

    int16_t prev_freelisthead;
    xntx_read(tx, page, xnpg_fld_offset(page, XNFLD_HDR_FREELISTHEAD, -1), &prev_freelisthead, sizeof(int16_t));
    xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_SLT_NEXT, rec_idx), &prev_freelisthead, sizeof(int16_t));
    int16_t data_off;
    xntx_read(tx, page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, rec_idx), &data_off, sizeof(int16_t));
    xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_HDR_FREELISTHEAD, -1), &data_off, sizeof(int16_t));

    int16_t ptr_count;
    xntx_read(tx, page, xnpg_fld_offset(page, XNFLD_HDR_PTRCOUNT, -1), &ptr_count, sizeof(int16_t));

    //copy pointers to the left to remove deleted ptr
    for (int i = rec_idx; i < ptr_count - 1; i++) {
        int16_t right_ptr;
        xntx_read(tx, page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, i + 1), &right_ptr, sizeof(int16_t));
        xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, i), &right_ptr, sizeof(int16_t));
    }
   
    //TODO: can get rid of this - not strictly necessary.  We can just leave junk data there
    //zero out right most ptr
    int16_t zero = 0;
    xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, ptr_count - 1), &zero, sizeof(int16_t));

    ptr_count--;
    xntx_write(tx, page, xnpg_fld_offset(page, XNFLD_HDR_PTRCOUNT, -1), &ptr_count, sizeof(int16_t));
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

void xndb_init_itr(struct xndb *db, struct xnitr *itr) {
    itr->block_idx = 0;
    itr->rec_idx = -1;
    itr->pager = db->pager;
}

bool xnitr_next(struct xnitr *itr) {
    struct xnpg *page = xnpgr_pin(itr->pager, itr->block_idx);
    int16_t rec_count;
    xnpg_read(page, xnpg_fld_offset(page, XNFLD_HDR_PTRCOUNT, -1), &rec_count, sizeof(int16_t));

    while (true) {
        itr->rec_idx++;
        if (itr->rec_idx >= rec_count) {
            xnpgr_unpin(itr->pager, page);
            itr->block_idx++;
            if (itr->block_idx >= XN_BUCKETS) {
                return false;
            }
            page = xnpgr_pin(itr->pager, itr->block_idx);
            xnpg_read(page, xnpg_fld_offset(page, XNFLD_HDR_PTRCOUNT, -1), &rec_count, sizeof(int16_t));
            itr->rec_idx = -1;
            continue;
        }
        return true;
    }

    xnpgr_unpin(itr->pager, page);
    return false;
}

void xnitr_read_key(struct xnitr *itr, char *key) {
    struct xnpg *page = xnpgr_pin(itr->pager, itr->block_idx);
    int16_t keysz;
    xnpg_read(page, xnpg_fld_offset(page, XNFLD_SLT_KEYSZ, itr->rec_idx), &keysz, sizeof(int16_t));
    xnpg_read(page, xnpg_fld_offset(page, XNFLD_SLT_KEY, itr->rec_idx), key, keysz);
    xnpgr_unpin(itr->pager, page);
}

void xnitr_read_value(struct xnitr *itr, char *value) {
    struct xnpg *page = xnpgr_pin(itr->pager, itr->block_idx);
    int16_t valsz;
    xnpg_read(page, xnpg_fld_offset(page, XNFLD_SLT_VALUESZ, itr->rec_idx), &valsz, sizeof(int16_t));
    xnpg_read(page, xnpg_fld_offset(page, XNFLD_SLT_VALUE, itr->rec_idx), value, valsz);
    xnpgr_unpin(itr->pager, page);
}


void xndb_init_tx(struct xndb *db, struct xntx *tx) {
    tx->id = ++(db->tx_id);
    tx->pager = db->pager;
    tx->logger = db->logger;
    xnlgr_write(db->logger, XNLOGT_START, NULL, tx->id);
}

struct xnstatus xntx_commit(struct xntx *tx) {
    xnlgr_write(tx->logger, XNLOGT_COMMIT, NULL, tx->id);
    return xnstatus_create(true, NULL);
}


void xnlgr_init_lgrdr(struct xnlgr *logger, struct xnlgrdr *lgrdr) {
    //read logger buffer first
    /*
    lgrdr->block_idx = logger->page.block_idx;
    lgrdr->offset = -1;*/
}
bool xnlgrdr_next(struct xnlgrdr *lgrdr) {
    /*
    if (lgrdr->offset == -1) {
        xnpg_read(&logger->page, xnpg_fld_offset(&logger->page, XNFLD_LOG_LOGTOP, -1), &lgrdr->offset, sizeof(int16_t));
    }*/

}
void xnlgrdr_read(struct xnlgrdr *lgrdr, enum xnfld fld, void *data, size_t size) {
}
struct xnwrlg *xnlgrdr_read_write_log(struct xnlgrdr *lgrdr) {
    //allocate heap memory here to store xnrlg (require caller to free it)
}
void xnlgrdr_free_write_log(struct xnwrlg *wrlg) {
    //free xnrlg returned by xnlgrdr_read_write_log
}

struct xnstatus xntx_rollback(struct xntx *tx) {
    /*
    struct xnlgrdr log_reader;
    xnlgr_init_lgrdr(tx->logger, &log_reader);

    while (xnlgrdr_next(&log_reader)) {
        int txid;
        xnlgrdr_read(&log_reader, XNFLD_LOG_TXID, &txid, sizeof(int));
        if (txid != tx->id)
            continue;

        enum xnlogt type;
        xnlgrdr_read(&log_reader, XNFLD_LOG_TYPE, &type, sizeof(enum xnlogt));
        
        if (type == XNLOGT_START)
            break;
        if (type != XNLOGT_WRITE)
            continue;

        struct xnwrlg *wrlg = xnlgrdr_read_write_log(&log_reader);

        struct xnpg *page = xnpgr_pin(tx->pager, wrlg->block_idx);
        //writing directly to page so that log of undo operation is not written
        xnpg_write(page, wrlg->offset, wrlg->old_data, wrlg->data_size);
        xnpgr_unpin(tx->pager, page);

        xnlgrdr_free_write_log(wrlg);

    }

    xnlgr_write(tx->logger, XNLOGT_ROLLBACK, NULL, tx->id);*/
    return xnstatus_create(true, NULL);
}

struct xnstatus xntx_put(struct xntx *tx, const char *key, const char *value) {
    int block_idx = xndb_get_hash_bucket(key);
    //TODO get exclusive lock here using block_idx as lock identifier
    //eg, xncnr_acquire_xlock(tx->concur, block_idx);
    struct xnpg *page = xnpgr_pin(tx->pager, block_idx);
    struct xnstatus s = xntx_do_put(tx, page, key, value);
    xnpgr_unpin(tx->pager, page);
    //TODO release x-lock
    //eg, xncnr_release_xlock(tx->concur, block_idx);
    return s;
}
struct xnstatus xntx_get(struct xntx *tx, const char *key, char *value) {
    int block_idx = xndb_get_hash_bucket(key);
    //TODO get shared lock here using block_idx as lock identifier
    //eg, xncnr_slock(tx->concur, block_idx);
    struct xnpg *page = xnpgr_pin(tx->pager, block_idx);
    //TODO write to log here??
    struct xnstatus s = xntx_do_get(tx, page, key, value);
    xnpgr_unpin(tx->pager, page);
    //TODO release s-lock
    return s;
}
struct xnstatus xntx_delete(struct xntx *tx, const char *key) {
    int block_idx = xndb_get_hash_bucket(key);
    //TODO get exclusive lock here using block_idx as lock identifier
    struct xnpg *page = xnpgr_pin(tx->pager, block_idx);
    struct xnstatus s = xntx_do_delete(tx, page, key);
    xnpgr_unpin(tx->pager, page);
    //TODO release x-lock
    return s;
}

void xnlgr_init(struct xnlgr *logger, const char* log_path) {
    logger->lsn = 0;
    memcpy(logger->log_path, log_path, strlen(log_path) + 1); //include null terminator

    logger->page.pins = 0;
    logger->page.block_idx = 0;
    int16_t logtop = XN_BLK_SZ;
    xnpg_write(&logger->page, xnpg_fld_offset(&logger->page, XNFLD_LOG_LOGTOP, -1), &logtop, sizeof(int16_t));
    xnlgr_flush(logger);
    
    //TODO 
    //if file exists
    //  load in last block and set
    //      logger->page.pins = 0;
    //      logger->page.block_idx = file_size / 4096 ??? (is the file offset going to be 4095?)
    //else
    //  create new file and zero it all out
    //  set header (only logtop field needs to be set)
    //  logger->page.pins = 0;
    //  logger->page.block_idx = 0;   
}

int xnlgr_next_lsn(struct xnlgr *logger) {
    //TODO lock
    int lsn = ++logger->lsn;
    //TODO unlock
    return lsn;
}

void xnlog_read(char *buf, enum xnlogf fld, void *data) {

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
        int offset = 0;
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
    int16_t log_size = sizeof(log_type) + sizeof(tx_id) + sizeof(lsn);

    switch (log_type) {
    case XNLOGT_START:
    case XNLOGT_COMMIT:
    case XNLOGT_ROLLBACK:
    case XNLOGT_APPENDBLK: {
        int16_t log_off;
        if (!xnslt_append(&logger->page, log_size, &log_off)) {
            //TODO: add an overflow block to page, and append the record there 
        }
        char *buf = logger->page.buf + log_off;
        xnlog_write(buf, XNLOGF_TYPE, &log_type);
        xnlog_write(buf, XNLOGF_TXID, &tx_id);
        xnlog_write(buf, XNLOGF_LSN, &lsn);
        break;
    }
    case XNLOGT_WRITE: {
        log_size += sizeof(wrlog_data->block_idx) + 
                    sizeof(wrlog_data->offset) + 
                    sizeof(wrlog_data->data_size) + 
                    wrlog_data->data_size * 2; //old and new data length

        int16_t log_off;
        if (!xnslt_append(&logger->page, log_size, &log_off)) {
            //TODO: add an overflow block to page, and append the record there 
        }
        char *buf = logger->page.buf + log_off;
        xnlog_write(buf, XNLOGF_TYPE, &log_type);
        xnlog_write(buf, XNLOGF_TXID, &tx_id);
        xnlog_write(buf, XNLOGF_LSN, &lsn);
        xnlog_write(buf, XNLOGF_WRLOG, wrlog_data);
        break;
    }
    default:
        assert(false && "xenondb: invalid log type");
        break;
    }
}

struct xnstatus xnlgr_flush(struct xnlgr *logger) {
    int fd = xn_open(logger->log_path, O_CREAT | O_RDWR | O_DIRECT | O_SYNC, 0666);
    xn_seek(fd, logger->page.block_idx * XN_BLK_SZ, SEEK_SET);
    xn_write(fd, logger->page.buf, XN_BLK_SZ);
    close(fd);
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
    s = xndb_get(db, "cat", buf);
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
    xndb_init_itr(db, &itr);

    char key[5];
    char value[2];
    int count = 0;
    while (xnitr_next(&itr)) {
        xnitr_read_key(&itr, key);
        xnitr_read_value(&itr, value);
        count++;
    }

    assert(count == 3 && "iterate test failed");

    xndb_free(db);

    printf("iterate_test passed\n");
}

//when using a bucket size of 3 and the basic hash algorithm
//described in Advanced Unix Programming, these three keys
//end up in three different blocks.  Setting the max page
//count in the pager < 3 is necessary to verify that this test 
//works
void pager_test() {
    struct xndb* db = xndb_init("students", true);

    struct xnstatus status;
    status = xndb_put(db, "cat", "a");
    assert(status.ok && "put 1 failed");

    status = xndb_put(db, "dog", "b");
    assert(status.ok && "put 2 failed");

    status = xndb_put(db, "hamster", "c");
    assert(status.ok && "put 3 failed");
    char value[2];
    for (int i = 0; i < 3; i++) {
        status = xndb_get(db, "cat", value);
        assert(status.ok && *value == 'a' && "pager test failed");
        status = xndb_get(db, "dog", value);
        assert(status.ok && *value == 'b' && "pager test failed");
        status = xndb_get(db, "hamster", value);
        assert(status.ok && *value == 'c' && "pager test failed");
    }

    for (int i = 0; i < 10; i++) {
        //increment all values by 1 each iteration
        status = xndb_get(db, "cat", value);
        char c = *value;
        *value = ++c;
        status = xndb_put(db, "cat", value);

        status = xndb_get(db, "dog", value);
        c = *value;
        *value = ++c;
        status = xndb_put(db, "dog", value);

        status = xndb_get(db, "hamster", value);
        c = *value;
        *value = ++c;
        status = xndb_put(db, "hamster", value);
    }

    for (int i = 0; i < 3; i++) {
        //read all three and make sure the values are correct
        status = xndb_get(db, "cat", value);
        assert(status.ok && *value == 'k' && "pager test failed");
        status = xndb_get(db, "dog", value);
        assert(status.ok && *value == 'l' && "pager test failed");
        status = xndb_get(db, "hamster", value);
        assert(status.ok && *value == 'm' && "pager test failed");
    }

    xndb_free(db);

    printf("pager_test passed\n");
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

    printf("tx_commit_test passed\n");
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
    pager_test();
    system("exec rm -rf students");
    tx_commit_test();
    //system("exec rm -rf students");
    //tx_rollback_test();
    //system("exec rm -rf students");
    return 0;
}
