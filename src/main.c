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
#define XN_BLKHDR_SZ 256

#define XN_REC_SZ 256
#define XN_RECHDR_SZ 8 //next (4 bytes), valid (1 byte), extra (3 bytes)
#define XN_RECKEY_SZ 120
#define XN_RECVAL_SZ 128

#define XN_PGR_MAXPGCOUNT 2

//#define XN_BUCKETS 101
#define XN_BUCKETS 3


enum xnfld {
    XNFLD_HDR_RECCOUNT,
    XNFLD_HDR_FREELIST,
    XNFLD_REC_NEXT, 
    XNFLD_REC_VALID, 
    XNFLD_REC_KEY, 
    XNFLD_REC_VALUE, 
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
};

struct xnpgr {
    struct xnpg pages[XN_PGR_MAXPGCOUNT];
    char data_path[256];
};

void xnpgr_flush(struct xnpgr *pager, struct xnpg *page) {
    int fd = xn_open(pager->data_path, O_RDWR, 0666);
    xn_seek(fd, page->block_idx * XN_BLK_SZ, SEEK_SET);
    xn_write(fd, page->buf, XN_BLK_SZ);
    close(fd);
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
    assert(XN_BUCKETS + 1 >= XN_PGR_MAXPGCOUNT && "buckets + 1 not greater or equal to max page count");

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

    //write meta block
    char buf[XN_BLK_SZ];
    memset(buf, 0, XN_BLK_SZ);

    xn_write(fd, buf, XN_BLK_SZ);

    //write remaining buckets (with next set to -1)
    int freelist = -1;
    memcpy(buf + sizeof(int), &freelist, sizeof(int));
    for (int i = 0; i < XN_BUCKETS; i++) {
        xn_write(fd, buf, XN_BLK_SZ);
    }

    close(fd);


    db->pager = xnpgr_create(db->data_path);

    return db;
}

struct xnstatus {
    bool ok;
    char* msg;
};

struct xnstatus xnstatus_create(bool ok, char* msg) {
    struct xnstatus s;
    s.ok = ok;
    s.msg = msg;
    return s;
}

struct xnvalue {
    char data[XN_RECVAL_SZ];
};

struct xnkey {
    char data[XN_RECKEY_SZ];
};


int xndb_get_block_idx(const char* key) {
    size_t total = 0;
    for (int i = 0; i < strlen(key); i++) {
        total += (i + 1) * key[i];
    }

    return total % XN_BUCKETS + 1;
}

void xnpg_write_header_field(struct xnpg *page, enum xnfld fld, void* value) {
    switch (fld) {
    case XNFLD_HDR_RECCOUNT:
        memcpy(page->buf, value, sizeof(int));
        break;
    case XNFLD_HDR_FREELIST:
        memcpy(page->buf + sizeof(int), value, sizeof(int));
        break;
    default:
        assert(false && "xenondb: invalid header field type");
        break;
    }
}

void xnpg_read_header_field(struct xnpg *page, enum xnfld fld, void* result) {
    switch (fld) {
    case XNFLD_HDR_RECCOUNT:
        memcpy(result, page->buf, sizeof(int));
        break;
    case XNFLD_HDR_FREELIST:
        memcpy(result, page->buf + sizeof(int), sizeof(int));
        break;
    default:
        assert(false && "xenondb: invalid header field type");
        break;
    }
}

void xnpg_write_rec_field(struct xnpg *page, int rec_idx, enum xnfld fld, void* value) {
    switch (fld) {
    case XNFLD_REC_NEXT:
        memcpy(page->buf + XN_BLKHDR_SZ + rec_idx * XN_REC_SZ, value, sizeof(int));
        break;
    case XNFLD_REC_VALID:
        memcpy(page->buf + XN_BLKHDR_SZ + rec_idx * XN_REC_SZ + sizeof(int), value, sizeof(bool));
        break;
    case XNFLD_REC_KEY:
        memcpy(page->buf + XN_BLKHDR_SZ + rec_idx * XN_REC_SZ + XN_RECHDR_SZ, value, XN_RECKEY_SZ);
        break;
    case XNFLD_REC_VALUE:
        memcpy(page->buf + XN_BLKHDR_SZ + rec_idx * XN_REC_SZ + XN_RECHDR_SZ + XN_RECKEY_SZ, value, XN_RECVAL_SZ);
        break;
    default:
        assert(false && "xenondb: invalid record field type");
        break;
    }
}

void xnpg_read_rec_field(struct xnpg *page, int rec_idx, enum xnfld fld, void* result) {
    switch (fld) {
    case XNFLD_REC_NEXT:
        memcpy(result, page->buf + XN_BLKHDR_SZ + rec_idx * XN_REC_SZ, sizeof(int));
        break;
    case XNFLD_REC_VALID:
        memcpy(result, page->buf + XN_BLKHDR_SZ + rec_idx * XN_REC_SZ + sizeof(int), sizeof(bool));
        break;
    case XNFLD_REC_KEY:
        memcpy(result, page->buf + XN_BLKHDR_SZ + rec_idx * XN_REC_SZ + XN_RECHDR_SZ, XN_RECKEY_SZ);
        break;
    case XNFLD_REC_VALUE:
        memcpy(result, page->buf + XN_BLKHDR_SZ + rec_idx * XN_REC_SZ + XN_RECHDR_SZ + XN_RECKEY_SZ, XN_RECVAL_SZ);
        break;
    default:
        assert(false && "xenondb: invalid record field type");
        break;
    }
}

int xnpg_find_rec_idx(struct xnpg *page, const char *key) {
    int rec_count;
    xnpg_read_header_field(page, XNFLD_HDR_RECCOUNT, &rec_count);
    struct xnkey cur_key;
    for (int i = 0; i < rec_count; i++) {
        xnpg_read_rec_field(page, i, XNFLD_REC_KEY, &cur_key);
        if (strcmp(key, (char*)&cur_key) == 0 )
            return i;
    }

    return -1;
}

void xndb_add_to_freelist(struct xndb *db, int block_idx, int rec_idx) {
    struct xnpg *page = xnpgr_pin(db->pager, block_idx);

    int head_idx;
    xnpg_read_header_field(page, XNFLD_HDR_FREELIST, &head_idx);
    xnpg_write_rec_field(page, rec_idx, XNFLD_REC_NEXT, &head_idx);
    xnpg_write_header_field(page, XNFLD_HDR_FREELIST, &rec_idx);

    xnpgr_unpin(db->pager, page);
}

int xndb_remove_freelist_rec(struct xndb *db, int block_idx) {
    struct xnpg *page = xnpgr_pin(db->pager, block_idx);

    int head_idx;
    xnpg_read_header_field(page, XNFLD_HDR_FREELIST, &head_idx);

    if (head_idx == -1) {
        xnpgr_unpin(db->pager, page);
        return head_idx;
    }

    int new_head_idx;
    xnpg_read_rec_field(page, head_idx, XNFLD_REC_NEXT, &new_head_idx);
    xnpg_write_header_field(page, XNFLD_HDR_FREELIST, &new_head_idx);

    xnpgr_unpin(db->pager, page);
    return head_idx;
}


//this should really be a tx function
struct xnstatus xndb_put(struct xndb *db, const char *raw_key, const char *raw_value) {
    assert(strlen(raw_key) < XN_RECKEY_SZ && "key length is larger than XN_RECKEY_SZ");
    assert(strlen(raw_value) < XN_RECVAL_SZ && "value length is larger than XN_RECVAL_SZ");

    //prepare inputs
    struct xnkey key;
    memset(&key, 0, XN_RECKEY_SZ);
    struct xnvalue value;
    memset(&value, 0, XN_RECVAL_SZ);

    memcpy(&key, raw_key, strlen(raw_key) + 1);
    memcpy(&value, raw_value, strlen(raw_value) + 1);

    //check if key already exists, and replace value if so
    int block_idx = xndb_get_block_idx((char*)&key);
    struct xnpg *page = xnpgr_pin(db->pager, block_idx);
    int rec_idx = xnpg_find_rec_idx(page, (char*)&key);
    xnpgr_unpin(db->pager, page);

    if (rec_idx != -1) {
        struct xnpg *page = xnpgr_pin(db->pager, block_idx);
        xnpg_write_rec_field(page, rec_idx, XNFLD_REC_VALUE, (void*)&value);
        xnpgr_unpin(db->pager, page);
        return xnstatus_create(true, NULL);
    }

    //key doesn't exist yet
    int free_idx = xndb_remove_freelist_rec(db, block_idx);
    if (free_idx != -1) {
        //reusing vacant slot
        struct xnpg *page = xnpgr_pin(db->pager, block_idx);
        bool valid = true;
        xnpg_write_rec_field(page, free_idx, XNFLD_REC_VALID, &valid);
        xnpg_write_rec_field(page, free_idx, XNFLD_REC_KEY, (void*)&key);
        xnpg_write_rec_field(page, free_idx, XNFLD_REC_VALUE, (void*)&value);
        xnpgr_unpin(db->pager, page);
        return xnstatus_create(true, NULL);
    } else {
        //using a new slot
        struct xnpg *page = xnpgr_pin(db->pager, block_idx);

        int rec_count;
        xnpg_read_header_field(page, XNFLD_HDR_RECCOUNT, &rec_count);

        assert((rec_count + 1) * XN_REC_SZ < XN_BLK_SZ - XN_BLKHDR_SZ && "block cannot fit anymore records");

        bool valid = true;
        xnpg_write_rec_field(page, rec_count, XNFLD_REC_VALID, &valid);
        xnpg_write_rec_field(page, rec_count, XNFLD_REC_KEY, (void*)&key);
        xnpg_write_rec_field(page, rec_count, XNFLD_REC_VALUE, (void*)&value);

        rec_count++;
        xnpg_write_header_field(page, XNFLD_HDR_RECCOUNT, &rec_count);

        xnpgr_unpin(db->pager, page);
        return xnstatus_create(true, NULL);
    }
}

struct xnstatus xndb_get(struct xndb *db, const char *raw_key, struct xnvalue* result) {
    assert(strlen(raw_key) < XN_RECKEY_SZ && "key length is larger than XN_RECKEY_SZ");

    //prepare inputs
    struct xnkey key;
    memset(&key, 0, XN_RECKEY_SZ);
    memcpy(&key, raw_key, strlen(raw_key) + 1);

    int block_idx = xndb_get_block_idx((char*)&key);
    struct xnpg *page = xnpgr_pin(db->pager, block_idx);
    int rec_idx = xnpg_find_rec_idx(page, (char*)&key);

    if (rec_idx == -1) {
        xnpgr_unpin(db->pager, page);
        return xnstatus_create(false, "xenondb: key not found");
    }

    bool valid;
    xnpg_read_rec_field(page, rec_idx, XNFLD_REC_VALID, &valid);
    if (!valid) {
        xnpgr_unpin(db->pager, page);
        return xnstatus_create(false, "xenondb: key not found");
    }

    xnpg_read_rec_field(page, rec_idx, XNFLD_REC_VALUE, result);

    xnpgr_unpin(db->pager, page);
    return xnstatus_create(true, NULL);
}

struct xnstatus xndb_delete(struct xndb *db, const char *raw_key) {
    assert(strlen(raw_key) < XN_RECKEY_SZ && "key length is larger than XN_RECKEY_SZ");

    //prepare inputs
    struct xnkey key;
    memset(&key, 0, XN_RECKEY_SZ);
    memcpy(&key, raw_key, strlen(raw_key) + 1);

    int block_idx = xndb_get_block_idx((char*)&key);
    struct xnpg *page = xnpgr_pin(db->pager, block_idx);
    int rec_idx = xnpg_find_rec_idx(page, (char*)&key);
    if (rec_idx == -1) {
        xnpgr_unpin(db->pager, page);
        return xnstatus_create(false, "xenondb: key not found");
    }

    bool valid = false;
    xnpg_write_rec_field(page, rec_idx, XNFLD_REC_VALID, &valid);
    xndb_add_to_freelist(db, block_idx, rec_idx);

    xnpgr_unpin(db->pager, page);
    return xnstatus_create(true, NULL);
}

void xniter_reset(struct xndb *db) {
    db->iter_block_idx = 1; //first index block
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
            if (db->iter_block_idx > XN_BUCKETS) {
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
}

void put_test() {
    struct xndb* db = xndb_init("students", true);

    struct xnstatus status;
    status = xndb_put(db, "cat", "a");
    assert(status.ok && "put 1 failed");

    status = xndb_put(db, "dog", "b");
    assert(status.ok && "put 2 failed");

    status = xndb_put(db, "hamster", "c");
    assert(status.ok && "put 3 failed");

    /*
    status = xndb_put(db, "hamster", "d");
    assert(status.ok && "put on existing key failed");*/

    xndb_free(db);

    printf("put_test passed\n");
}

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
}

int main(int argc, char** argv) {
    put_test();
    system("exec rm -rf students");
    get_test();
    system("exec rm -rf students");
    delete_test();
    system("exec rm -rf students");
    iterate_test();
    system("exec rm -rf students");
    freelist_test(); //how can we automate this test
    system("exec rm -rf students");
    pager_test();
    system("exec rm -rf students");
    return 0;
}
