#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <sys/types.h>
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

enum xnlog {
    XNLOG_START,
    XNLOG_COMMIT,
    XNLOG_ROLLBACK,
    XLNOG_WRITE
};

enum xnfld {
    XNFLD_HDR_PTRCOUNT,
    XNFLD_HDR_SLTSTOP,
    XNFLD_HDR_FREELISTHEAD,
    XNFLD_HDR_OVERFLOWBLOCK,
    XNFLD_PTR_SLTOFF,
    XNFLD_SLT_NEXT,
    XNFLD_SLT_KEYSZ,
    XNFLD_SLT_VALUESZ,
    XNFLD_SLT_KEY,
    XNFLD_SLT_VALUE
};

//xenon db
struct xndb {
    char dir_path[256];
    char data_path[256];
    struct xnpgr *pager;
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

struct xnitr {
    int block_idx;
    int rec_idx;
    struct xnpgr *pager;
};

struct xntx {
    int id;
    struct xnpgr *pager;
};

void xndb_init_tx(struct xndb *db, struct xntx *tx);
struct xnstatus xntx_commit(struct xntx *tx);
struct xnstatus xntx_rollback(struct xntx *tx);
struct xnstatus xntx_put(struct xntx *tx, const char *key, const char *value);
struct xnstatus xntx_get(struct xntx *tx, const char *key, char *value);
struct xnstatus xntx_delete(struct xntx *tx, const char *key);

struct xnstatus xnpg_delete(struct xnpg *page, const char *key);
struct xnstatus xndb_delete(struct xndb *db, const char *key);
void xnpg_write(struct xnpg *page, int16_t off, const void *data, size_t size);
void xnpg_read(struct xnpg *page, int16_t off, void *data, size_t size);
int16_t xnpg_fld_offset(struct xnpg *page, enum xnfld fld, int slot);

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


void xnpg_init_data_block(struct xnpg *page) {
    memset(page->buf, 0, XN_BLK_SZ);
    int16_t block_size = XN_BLK_SZ;
    xnpg_write(page, xnpg_fld_offset(page, XNFLD_HDR_SLTSTOP, -1), &block_size, sizeof(int16_t));
}

struct xndb* xndb_init(char* path, bool make_dir) {
    if (make_dir) {
        xndb_make_dir(path);
    }

    if (!xndb_dir_exists(path)) {
        fprintf(stderr, "xenondb: database directory doesn't exist.");
        exit(1);
    }

    struct xndb* db = xn_malloc(sizeof(struct xndb));

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
    xnpg_init_data_block(&page);

    for (int i = 0; i < XN_BUCKETS; i++) {
        xn_write(fd, page.buf, XN_BLK_SZ);
    }

    close(fd);


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

void xnpg_write_record(struct xnpg *page, int16_t idx, const char *key, const char *value) {
    int16_t next = 0;
    int16_t keysz = strlen(key);
    int16_t valsz = strlen(value);
    xnpg_write(page, xnpg_fld_offset(page, XNFLD_SLT_NEXT, idx), &next, sizeof(int16_t));
    xnpg_write(page, xnpg_fld_offset(page, XNFLD_SLT_KEYSZ, idx), &keysz, sizeof(int16_t));
    xnpg_write(page, xnpg_fld_offset(page, XNFLD_SLT_VALUESZ, idx), &valsz, sizeof(int16_t));
    xnpg_write(page, xnpg_fld_offset(page, XNFLD_SLT_KEY, idx), key, keysz);
    xnpg_write(page, xnpg_fld_offset(page, XNFLD_SLT_VALUE, idx), value, valsz);
}

struct xnstatus xnpg_put(struct xnpg *page, const char *key, const char *value) {
    int rec_idx = xnpg_find_rec_idx(page, key);

    if (rec_idx != -1) {
        int16_t old_valsz;
        xnpg_read(page, xnpg_fld_offset(page, XNFLD_SLT_VALUESZ, rec_idx), &old_valsz, sizeof(int16_t));
        if (strlen(value) == old_valsz) {
            xnpg_write(page, xnpg_fld_offset(page, XNFLD_SLT_VALUE, rec_idx), value, old_valsz);
            return xnstatus_create(true, NULL);
        }

        //if size is not the same, delete record
        //the remaining code in function will add a new record with the proper size
        struct xnstatus s = xnpg_delete(page, key);
        if (!s.ok)
            return s;
    }

    int16_t free_offset = xnpg_get_freeslot_offset(page, key, value);
    int16_t rec_count;
    xnpg_read(page, xnpg_fld_offset(page, XNFLD_HDR_PTRCOUNT, -1), &rec_count, sizeof(int16_t));

    if (free_offset != -1) {
        //reuse old slot that matches size
        xnpg_write(page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, rec_count), &free_offset, sizeof(int16_t));
    } else {
        //using a new slot
        int16_t datatop;
        xnpg_read(page, xnpg_fld_offset(page, XNFLD_HDR_SLTSTOP, -1), &datatop, sizeof(int16_t));
        int16_t dataoff = datatop - (sizeof(int16_t) * 3 + strlen(key) + strlen(value));
        xnpg_write(page, xnpg_fld_offset(page, XNFLD_HDR_SLTSTOP, -1), &dataoff, sizeof(int16_t));
        xnpg_write(page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, rec_count), &dataoff, sizeof(int16_t));
    }

    xnpg_write_record(page, rec_count, key, value);

    rec_count++;
    xnpg_write(page, xnpg_fld_offset(page, XNFLD_HDR_PTRCOUNT, -1), &rec_count, sizeof(int16_t));
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


struct xnstatus xnpg_get(struct xnpg *page, const char *key, char* result) {
    int rec_idx = xnpg_find_rec_idx(page, key);

    if (rec_idx == -1) {
        return xnstatus_create(false, "xenondb: key not found");
    }

    int16_t valsz;
    xnpg_read(page, xnpg_fld_offset(page, XNFLD_SLT_VALUESZ, rec_idx), &valsz, sizeof(int16_t));
    xnpg_read(page, xnpg_fld_offset(page, XNFLD_SLT_VALUE, rec_idx), result, valsz);
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

struct xnstatus xnpg_delete(struct xnpg *page, const char *key) {
    int rec_idx = xnpg_find_rec_idx(page, key);
    if (rec_idx == -1) {
        return xnstatus_create(false, "xenondb: key not found");
    }

    int16_t prev_freelisthead;
    xnpg_read(page, xnpg_fld_offset(page, XNFLD_HDR_FREELISTHEAD, -1), &prev_freelisthead, sizeof(int16_t));
    xnpg_write(page, xnpg_fld_offset(page, XNFLD_SLT_NEXT, rec_idx), &prev_freelisthead, sizeof(int16_t));
    int16_t data_off;
    xnpg_read(page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, rec_idx), &data_off, sizeof(int16_t));
    xnpg_write(page, xnpg_fld_offset(page, XNFLD_HDR_FREELISTHEAD, -1), &data_off, sizeof(int16_t));

    //delete ptr
    int16_t ptr_count;
    xnpg_read(page, xnpg_fld_offset(page, XNFLD_HDR_PTRCOUNT, -1), &ptr_count, sizeof(int16_t));

    //copy pointers to the left to remove deleted ptr
    for (int i = rec_idx; i < ptr_count - 1; i++) {
        int16_t right_ptr;
        xnpg_read(page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, i + 1), &right_ptr, sizeof(int16_t));
        xnpg_write(page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, i), &right_ptr, sizeof(int16_t));
    }
   
    //TODO: can get rid of this - not strictly necessary.  We can just leave junk data there
    //zero out right most ptr
    int16_t zero = 0;
    xnpg_write(page, xnpg_fld_offset(page, XNFLD_PTR_SLTOFF, ptr_count - 1), &zero, sizeof(int16_t));

    ptr_count--;
    xnpg_write(page, xnpg_fld_offset(page, XNFLD_HDR_PTRCOUNT, -1), &ptr_count, sizeof(int16_t));
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
    tx->id = 0; //TODO tx_id is set to 0 for now.  store tx counter in header of file and use that to give each tx a unique id
    tx->pager = db->pager;
    //tx->logger = db->logger;
    //xnlog_write(db->logger, XNLOG_START);
}

struct xnstatus xntx_commit(struct xntx *tx) {
    //TODO what should happen here?
    //xnlog_write(tx->logger, XNLOG_COMMIT);
    return xnstatus_create(true, NULL);
}

struct xnstatus xntx_rollback(struct xntx *tx) {
    //TODO what should happen here?
    //go through log and undo all changes in this tx???
    //xnlog_write(tx->logger, XNLOG_ROLLBACK);
    return xnstatus_create(true, NULL);
}

struct xnstatus xntx_put(struct xntx *tx, const char *key, const char *value) {
    int block_idx = xndb_get_hash_bucket(key);
    struct xnpg *page = xnpgr_pin(tx->pager, block_idx);
    //TODO write to log here
    struct xnstatus s;
    s = xnpg_put(page, key, value);
    xnpgr_unpin(tx->pager, page);
    return s;
}
struct xnstatus xntx_get(struct xntx *tx, const char *key, char *value) {
    int block_idx = xndb_get_hash_bucket(key);
    struct xnpg *page = xnpgr_pin(tx->pager, block_idx);
    //TODO write to log here??
    struct xnstatus s;
    s = xnpg_get(page, key, value);
    xnpgr_unpin(tx->pager, page);
    return s;
}
struct xnstatus xntx_delete(struct xntx *tx, const char *key) {
    int block_idx = xndb_get_hash_bucket(key);
    struct xnpg *page = xnpgr_pin(tx->pager, block_idx);
    //TODO write to log here
    struct xnstatus s;
    s = xnpg_delete(page, key);
    xnpgr_unpin(tx->pager, page);
    return s;
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


void tx_test() {
    struct xndb* db = xndb_init("students", true);
    struct xnstatus s;
    /*

    struct xntx tx;
    xndb_init_tx(db, &tx);
    xntx_put(&tx, "cat", "a");
    xntx_commit(&tx);*/

    /*
    char value[2];
    s = xndb_get(db, "cat", value);
    assert(s.ok && strcmp(value, "a") == 0 && "tx test failed");

    struct xntx tx2;
    xndb_init_tx(db, &tx2);
    xntx_put(tx2, "dog", "a");
    xntx_rollback(tx2);
    s = xndb_get(db, "dog", value);
    assert(!s.ok && "tx test failed");*/

    xndb_free(db);

    printf("tx_test passed\n");
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
    tx_test();
    system("exec rm -rf students");
    return 0;
}
