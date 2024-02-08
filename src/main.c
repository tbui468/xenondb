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

enum xnfld {
    XNFLD_HDR_PTRCOUNT,
    XNFLD_HDR_DATATOP,
    XNFLD_HDR_FREELISTHEAD,
    XNFLD_HDR_OVERFLOWBLOCK,
    XNFLD_PTR_DATAOFF,
    XNFLD_REC_NEXT,
    XNFLD_REC_KEYSZ,
    XNFLD_REC_VALUESZ,
    XNFLD_REC_KEY,
    XNFLD_REC_VALUE
};



//xenon db
struct xndb {
    char dir_path[256];
    char data_path[256];
    int iter_block_idx;
    int iter_rec_idx;
    struct xnpgr *pager;
};

struct xnpg {
    char buf[XN_BLK_SZ];
    int pins;
    int block_idx;
    int16_t cur;
    int16_t prev;
};


struct xnpgr {
    struct xnpg pages[XN_PGR_MAXPGCOUNT];
    char data_path[256];
};



struct xnstatus xndb_delete(struct xndb *db, const char *key);

void xnpg_write_hdr(struct xnpg *page, enum xnfld fld, const void *data, size_t size);
void xnpg_write_ptr(struct xnpg *page, enum xnfld fld, int idx, const void *data, size_t size);
void xnpg_write_rec(struct xnpg *page, enum xnfld fld, int idx, const void *data, size_t size);
void xnpg_read_hdr(struct xnpg *page, enum xnfld fld, void *result, size_t size);
void xnpg_read_ptr(struct xnpg *page, enum xnfld fld, int idx, void *result, size_t size);
void xnpg_read_rec(struct xnpg *page, enum xnfld fld, int idx, void *result, size_t size);


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
    xnpg_write_hdr(page, XNFLD_HDR_DATATOP, &block_size, sizeof(int16_t));
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
    xnpg_read_hdr(page, XNFLD_HDR_PTRCOUNT, &rec_count, sizeof(int16_t));
    for (int i = 0; i < rec_count; i++) {
        int16_t key_size;
        xnpg_read_rec(page, XNFLD_REC_KEYSZ, i, &key_size, sizeof(int16_t));
        char buf[key_size + 1];
        xnpg_read_rec(page, XNFLD_REC_KEY, i, buf, key_size);
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

void xnpg_write_hdr(struct xnpg *page, enum xnfld fld, const void *data, size_t size) {
    switch (fld) {
    case XNFLD_HDR_PTRCOUNT:
        assert(size == sizeof(int16_t) && "xenondb: pointer count must be size of int16_t");
        memcpy(page->buf, data, size);
        break;
    case XNFLD_HDR_DATATOP:
        assert(size == sizeof(int16_t) && "xenondb: data top pointer must be size of int16_t");
        memcpy(page->buf + sizeof(int16_t), data, size);
        break;
    case XNFLD_HDR_FREELISTHEAD:
        assert(size == sizeof(int16_t) && "xenondb: freelist pointer must be size of int16_t");
        memcpy(page->buf + sizeof(int16_t) * 2, data, size);
        break;
    case XNFLD_HDR_OVERFLOWBLOCK:
        assert(size == sizeof(int32_t) && "xenondb: overflow block index must be size of int32_t");
        memcpy(page->buf + sizeof(int16_t) * 3, data, size);
        break;
    default:
        assert(false && "xenondb: invalid block header field");
    };
}

void xnpg_write_ptr(struct xnpg *page, enum xnfld fld, int idx, const void *data, size_t size) {
    switch (fld) {
    case XNFLD_PTR_DATAOFF:
        assert(size == sizeof(int16_t) && "xenondb: pointer data offset field must be size of int16_t");
        int16_t ptr_count;
        xnpg_read_hdr(page, XNFLD_HDR_PTRCOUNT, &ptr_count, sizeof(int16_t));
        memcpy(page->buf + XN_BLKHDR_SZ + ptr_count * sizeof(int16_t), data, size);
        break;
    default:
        assert(false && "xenondb: invalid ptr field");
    }
}
void xnpg_write_rec(struct xnpg *page, enum xnfld fld, int idx, const void *data, size_t size) {
    int16_t rec_off;
    xnpg_read_ptr(page, XNFLD_PTR_DATAOFF, idx, &rec_off, sizeof(int16_t));

    switch (fld) {
    case XNFLD_REC_NEXT:
        assert(size == sizeof(int16_t) && "xenondb: record next pointer must be size of int16_t");
        memcpy(page->buf + rec_off, data, size);
        break;
    case XNFLD_REC_KEYSZ:
        assert(size == sizeof(int16_t) && "xenondb: record key size must be size of int16_t");
        memcpy(page->buf + rec_off + sizeof(int16_t), data, size);
        break;
    case XNFLD_REC_VALUESZ:
        assert(size == sizeof(int16_t) && "record value size must be size of int16_t");
        memcpy(page->buf + rec_off + sizeof(int16_t) * 2, data, size);
        break;
    case XNFLD_REC_KEY: {
        int16_t key_size;
        xnpg_read_rec(page, XNFLD_REC_KEYSZ, idx, &key_size, sizeof(int16_t));
        assert(key_size != 0 && "record key size must be written before key");
        assert(key_size == size && "record key size written different from size");
        memcpy(page->buf + rec_off + sizeof(int16_t) * 3, data, size);
        break;
    }
    case XNFLD_REC_VALUE: {
        int16_t key_size;
        xnpg_read_rec(page, XNFLD_REC_KEYSZ, idx, &key_size, sizeof(int16_t));
        assert(key_size != 0 && "record key size must be written before value offset can be determined");
        int16_t value_size;
        xnpg_read_rec(page, XNFLD_REC_VALUESZ, idx, &value_size, sizeof(int16_t));
        assert(value_size != 0 && "record value size must be written before value");
        assert(size == value_size && "record value size written different from size");
        memcpy(page->buf + rec_off + sizeof(int16_t) * 3 + key_size, data, size);
        break;
    }
    default:
        assert(false && "xenondb: invalid record field");
    };
}

void xnpg_read_hdr(struct xnpg *page, enum xnfld fld, void *result, size_t size) {
    switch (fld) {
    case XNFLD_HDR_PTRCOUNT:
        assert(size == sizeof(int16_t) && "xenondb: pointer count must be size of int16_t");
        memcpy(result, page->buf, size);
        break;
    case XNFLD_HDR_DATATOP:
        assert(size == sizeof(int16_t) && "xenondb: data top pointer must be size of int16_t");
        memcpy(result, page->buf + sizeof(int16_t), size);
        break;
    case XNFLD_HDR_FREELISTHEAD:
        assert(size == sizeof(int16_t) && "xenondb: freelist pointer must be size of int16_t");
        memcpy(result, page->buf + sizeof(int16_t) * 2, size);
        break;
    case XNFLD_HDR_OVERFLOWBLOCK:
        assert(size == sizeof(int32_t) && "xenondb: overflow block index must be size of int32_t");
        memcpy(result, page->buf + sizeof(int16_t) * 3, size);
        break;
    default:
        assert(false && "xenondb: invalid block header field");
    };
}

void xnpg_read_ptr(struct xnpg *page, enum xnfld fld, int idx, void *result, size_t size) {
    switch (fld) {
    case XNFLD_PTR_DATAOFF:
        assert(size == sizeof(int16_t) && "xenondb: pointer data offset field must be size of int16_t");
        memcpy(result, page->buf + XN_BLKHDR_SZ + idx * sizeof(int16_t), size);
        break;
    default:
        assert(false && "xenondb: invalid ptr field");
    }
}

void xnpg_read_rec(struct xnpg *page, enum xnfld fld, int idx, void *result, size_t size) {
    int16_t rec_off;
    xnpg_read_ptr(page, XNFLD_PTR_DATAOFF, idx, &rec_off, sizeof(int16_t));

    switch (fld) {
    case XNFLD_REC_NEXT:
        assert(size == sizeof(int16_t) && "xenondb: record next pointer must be size of int16_t");
        memcpy(result, page->buf + rec_off, size);
        break;
    case XNFLD_REC_KEYSZ:
        assert(size == sizeof(int16_t) && "xenondb: record key size must be size of int16_t");
        memcpy(result, page->buf + rec_off + sizeof(int16_t), size);
        break;
    case XNFLD_REC_VALUESZ:
        assert(size == sizeof(int16_t) && "record value size must be size of int16_t");
        memcpy(result, page->buf + rec_off + sizeof(int16_t) * 2, size);
        break;
    case XNFLD_REC_KEY: {
        int16_t key_size;
        xnpg_read_rec(page, XNFLD_REC_KEYSZ, idx, &key_size, sizeof(int16_t));
        assert(key_size != 0 && "record key size must be written before key");
        assert(key_size == size && "record key size written different from size");
        memcpy(result, page->buf + rec_off + sizeof(int16_t) * 3, size);
        break;
    }
    case XNFLD_REC_VALUE: {
        int16_t key_size;
        xnpg_read_rec(page, XNFLD_REC_KEYSZ, idx, &key_size, sizeof(int16_t));
        assert(key_size != 0 && "record key size must be written before value offset can be determined");
        int16_t value_size;
        xnpg_read_rec(page, XNFLD_REC_VALUESZ, idx, &value_size, sizeof(int16_t));
        assert(value_size != 0 && "record value size must be written before value");
        assert(size == value_size && "record value size written different from size");
        memcpy(result, page->buf + rec_off + sizeof(int16_t) * 3 + key_size, size);
        break;
    }
    default:
        assert(false && "xenondb: invalid record field");
    };
}

void xnpg_init_freeslot_search(struct xnpg *page) {
    page->cur = sizeof(int16_t) * 2; //location of freelist TODO arbitrary offset here is ugly and error prone
    page->prev = 0;
}

bool xnpg_next_freeslot(struct xnpg *page) {
    page->prev = page->cur;
    xnpg_read(page, page->cur, &page->cur, sizeof(int16_t));
    return page->cur != 0;
}

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
int16_t xndb_get_freeslot_offset(struct xndb *db, const char *key, const char *value) {
    int block_idx = xndb_get_hash_bucket(key);
    struct xnpg *page = xnpgr_pin(db->pager, block_idx);
    int16_t target_size = strlen(key) + strlen(value);

    xnpg_init_freeslot_search(page);
    while (xnpg_next_freeslot(page)) {
        if (xnpg_freeslot_size(page) == target_size) {
            int16_t offset = xnpg_remove_freeslot(page);
            xnpgr_unpin(db->pager, page);
            return offset;
        }
    }

    xnpgr_unpin(db->pager, page);
    return -1;
}

void xnpg_write_record(struct xnpg *page, int16_t idx, const char *key, const char *value) {
    int16_t next = 0;
    int16_t keysz = strlen(key);
    int16_t valsz = strlen(value);
    xnpg_write_rec(page, XNFLD_REC_NEXT, idx, &next, sizeof(int16_t));
    xnpg_write_rec(page, XNFLD_REC_KEYSZ, idx, &keysz, sizeof(int16_t));
    xnpg_write_rec(page, XNFLD_REC_VALUESZ, idx, &valsz, sizeof(int16_t));
    xnpg_write_rec(page, XNFLD_REC_KEY, idx, key, keysz);
    xnpg_write_rec(page, XNFLD_REC_VALUE, idx, value, valsz);
}

//this should really be a tx function
struct xnstatus xndb_put(struct xndb *db, const char *key, const char *value) {
    //TODO assert that key/value will fit into a single block

    int block_idx = xndb_get_hash_bucket(key);
    struct xnpg *page = xnpgr_pin(db->pager, block_idx);
    int rec_idx = xnpg_find_rec_idx(page, key);

    if (rec_idx != -1) {
        int16_t old_valsz;
        xnpg_read_rec(page, XNFLD_REC_VALUESZ, rec_idx, &old_valsz, sizeof(int16_t));
        if (strlen(value) == old_valsz) {
            xnpg_write_rec(page, XNFLD_REC_VALUE, rec_idx, value, old_valsz);
            xnpgr_unpin(db->pager, page);
            return xnstatus_create(true, NULL);
        }

        //if size is not the same, delete record
        //the remaining code in function will add a new record with the proper size
        xndb_delete(db, key);
    }

    int16_t free_offset = xndb_get_freeslot_offset(db, key, value);
    int16_t rec_count;
    xnpg_read_hdr(page, XNFLD_HDR_PTRCOUNT, &rec_count, sizeof(int16_t));

    if (free_offset != -1) {
        //reuse old slot that matches size
        xnpg_write_ptr(page, XNFLD_PTR_DATAOFF, rec_count, &free_offset, sizeof(int16_t));
    } else {
        //using a new slot
        int16_t datatop;
        xnpg_read_hdr(page, XNFLD_HDR_DATATOP, &datatop, sizeof(int16_t));
        int16_t dataoff = datatop - (sizeof(int16_t) * 3 + strlen(key) + strlen(value));
        xnpg_write_hdr(page, XNFLD_HDR_DATATOP, &dataoff, sizeof(int16_t));
        xnpg_write_ptr(page, XNFLD_PTR_DATAOFF, rec_count, &dataoff, sizeof(int16_t));
    }

    xnpg_write_record(page, rec_count, key, value);

    rec_count++;
    xnpg_write_hdr(page, XNFLD_HDR_PTRCOUNT, &rec_count, sizeof(int16_t));

    xnpgr_unpin(db->pager, page);
    return xnstatus_create(true, NULL);
}

struct xnstatus xndb_get(struct xndb *db, const char *key, char* result) {
    //TODO assert that key fits into a single block

    int block_idx = xndb_get_hash_bucket(key);
    struct xnpg *page = xnpgr_pin(db->pager, block_idx);
    int rec_idx = xnpg_find_rec_idx(page, key);

    if (rec_idx == -1) {
        xnpgr_unpin(db->pager, page);
        return xnstatus_create(false, "xenondb: key not found");
    }

    int16_t valsz;
    xnpg_read_rec(page, XNFLD_REC_VALUESZ, rec_idx, &valsz, sizeof(int16_t));
    xnpg_read_rec(page, XNFLD_REC_VALUE, rec_idx, result, valsz);
    result[valsz] = '\0';

    xnpgr_unpin(db->pager, page);
    return xnstatus_create(true, NULL);
}

struct xnstatus xndb_delete(struct xndb *db, const char *key) {
    //TODO assert key will fit into a single block

    int block_idx = xndb_get_hash_bucket(key);
    struct xnpg *page = xnpgr_pin(db->pager, block_idx);
    int rec_idx = xnpg_find_rec_idx(page, key);
    if (rec_idx == -1) {
        xnpgr_unpin(db->pager, page);
        return xnstatus_create(false, "xenondb: key not found");
    }

    int16_t prev_freelisthead;
    xnpg_read_hdr(page, XNFLD_HDR_FREELISTHEAD, &prev_freelisthead, sizeof(int16_t));
    xnpg_write_rec(page, XNFLD_REC_NEXT, rec_idx, &prev_freelisthead, sizeof(int16_t));
    int16_t data_off;
    xnpg_read_ptr(page, XNFLD_PTR_DATAOFF, rec_idx, &data_off, sizeof(int16_t));
    xnpg_write_hdr(page, XNFLD_HDR_FREELISTHEAD, &data_off, sizeof(int16_t));

    //delete ptr
    int16_t ptr_count;
    xnpg_read_hdr(page, XNFLD_HDR_PTRCOUNT, &ptr_count, sizeof(int16_t));

    //copy pointers to the left to remove deleted ptr
    for (int i = rec_idx; i < ptr_count - 1; i++) {
        int16_t right_ptr;
        xnpg_read_ptr(page, XNFLD_PTR_DATAOFF, i + 1, &right_ptr, sizeof(int16_t));
        xnpg_write_ptr(page, XNFLD_PTR_DATAOFF, i, &right_ptr, sizeof(int16_t));
    }
   
    //TODO: can get rid of this - not strictly necessary.  We can just leave junk data there
    //zero out right most ptr
    int16_t zero = 0;
    xnpg_write_ptr(page, XNFLD_PTR_DATAOFF, ptr_count - 1, &zero, sizeof(int16_t));

    ptr_count--;
    xnpg_write_hdr(page, XNFLD_HDR_PTRCOUNT, &ptr_count, sizeof(int16_t));

    xnpgr_unpin(db->pager, page);
    return xnstatus_create(true, NULL);
}

/*
void xniter_reset(struct xndb *db) {
    db->iter_block_idx = 0; //first index block
    db->iter_rec_idx = -1; //one before first record
}

bool xniter_next(struct xndb *db) {
    struct xnpg *page = xnpgr_pin(db->pager, db->iter_block_idx);
    int rec_count;
    xnpg_read_header_field(page, XNFLD_HDR_RECCOUNT, &rec_count);

    while (true) {
        db->iter_rec_idx++;
        if (db->iter_rec_idx >= rec_count) {
            xnpgr_unpin(db->pager, page);
            db->iter_block_idx++;
            if (db->iter_block_idx >= XN_BUCKETS) {
                return false;
            }
            page = xnpgr_pin(db->pager, db->iter_block_idx);
            xnpg_read_header_field(page, XNFLD_HDR_RECCOUNT, &rec_count);
            db->iter_rec_idx = -1;
            continue;
        }

        bool valid;
        xnpg_read_rec_field(page, db->iter_rec_idx, XNFLD_REC_VALID, &valid);
        if (valid) {
            xnpgr_unpin(db->pager, page);
            return true;
        }
    }

    xnpgr_unpin(db->pager, page);
    return false;
}

void xniter_read_key_value(struct xndb *db, struct xnkey *key, struct xnvalue *value) {
    struct xnpg *page = xnpgr_pin(db->pager, db->iter_block_idx);
    xnpg_read_rec_field(page, db->iter_rec_idx, XNFLD_REC_KEY, key);
    xnpg_read_rec_field(page, db->iter_rec_idx, XNFLD_REC_VALUE, value);
    xnpgr_unpin(db->pager, page);
}*/

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

/*
void get_test() {
    struct xndb* db = xndb_init("students", true);

    struct xnstatus status;
    status = xndb_put(db, "cat", "a");
    assert(status.ok && "put 1 failed");
    status = xndb_put(db, "dog", "b");
    assert(status.ok && "put 2 failed");
    status = xndb_put(db, "whale", "c");
    assert(status.ok && "put 3 failed");
    status = xndb_put(db, "whale", "d");
    assert(status.ok && "put on existing key failed");

    struct xnvalue value;
    status = xndb_get(db, "cat", &value);
    assert(status.ok && strcmp((char*)&value, "a") == 0 && "get 1 failed");

    status = xndb_get(db, "dog", &value);
    assert(status.ok && strcmp((char*)&value, "b") == 0 && "get 2 failed");

    status = xndb_get(db, "whale", &value);
    assert(status.ok && strcmp((char*)&value, "d") == 0 && "get on replaced value failed");

    xndb_free(db);

    printf("get_test passed\n");
}

void delete_test() {
    struct xndb* db = xndb_init("students", true);

    struct xnstatus status;
    status = xndb_put(db, "cat", "a");
    status = xndb_put(db, "dog", "b");
    status = xndb_delete(db, "cat");
    assert(status.ok && "delete 1 failed");

    struct xnvalue value;
    status = xndb_get(db, "cat", &value);
    assert(!status.ok && "delete 2 failed");

    status = xndb_get(db, "dog", &value);
    assert(status.ok && strcmp((char*)&value, "b") == 0 && "delete 3 failed");

    xndb_free(db);

    printf("delete_test passed\n");
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

    xniter_reset(db);

    struct xnkey key;
    struct xnvalue value;
    int count = 0;
    while (xniter_next(db)) {
        xniter_read_key_value(db, &key, &value);
        count++;
    }

    assert(count == 3 && "iterate test failed");

    xndb_free(db);

    printf("iterate_test passed\n");
}

//Note: this test passes if only a single key/value pair is in database (old record is replaced)
void freelist_test() {
    struct xndb* db = xndb_init("students", true);

    struct xnstatus status;
    char buf[3];
    buf[0] = 0x01;
    buf[1] = 0x20;
    buf[2] = '\0';

    status = xndb_put(db, buf, "first"); //ascii 65 (+1, so block 66)
    status = xndb_delete(db, buf);

    status = xndb_put(db, "A", "second"); //ascii 65 (+1, so block 66)
    status = xndb_delete(db, "A");

    xndb_free(db);

    printf("freelist_test passed\n");
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

    struct xnvalue value;
    for (int i = 0; i < 3; i++) {
        status = xndb_get(db, "cat", &value);
        assert(status.ok && *((char*)&value) == 'a' && "pager test failed");
        status = xndb_get(db, "dog", &value);
        assert(status.ok && *((char*)&value) == 'b' && "pager test failed");
        status = xndb_get(db, "hamster", &value);
        assert(status.ok && *((char*)&value) == 'c' && "pager test failed");
    }

    for (int i = 0; i < 10; i++) {
        //increment all values by 1 each iteration
        status = xndb_get(db, "cat", &value);
        char c = *((char*)&value);
        *((char*)&value) = ++c;
        status = xndb_put(db, "cat", (char*)&value);

        status = xndb_get(db, "dog", &value);
        c = *((char*)&value);
        *((char*)&value) = ++c;
        status = xndb_put(db, "dog", (char*)&value);

        status = xndb_get(db, "hamster", &value);
        c = *((char*)&value);
        *((char*)&value) = ++c;
        status = xndb_put(db, "hamster", (char*)&value);
    }

    for (int i = 0; i < 3; i++) {
        //read all three and make sure the values are correct
        status = xndb_get(db, "cat", &value);
        assert(status.ok && *((char*)&value) == 'k' && "pager test failed");
        status = xndb_get(db, "dog", &value);
        assert(status.ok && *((char*)&value) == 'l' && "pager test failed");
        status = xndb_get(db, "hamster", &value);
        assert(status.ok && *((char*)&value) == 'm' && "pager test failed");
    }

    xndb_free(db);

    printf("pager_test passed\n");
}*/

int main(int argc, char** argv) {
    basic_put_test();
    system("exec rm -rf students");
    basic_get_test();
    system("exec rm -rf students");
    basic_delete_test();
    system("exec rm -rf students");
    put_test();
//    system("exec rm -rf students");
    //get_test();
    //system("exec rm -rf students");
    /*
    delete_test();
    system("exec rm -rf students");
    iterate_test();
    system("exec rm -rf students");
    freelist_test(); //how can we automate this test
    system("exec rm -rf students");
    pager_test();
    system("exec rm -rf students");*/
    return 0;
}
