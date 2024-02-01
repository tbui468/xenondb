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

#define XN_BLK_SZ 4096
#define XN_BLKHDR_SZ 256

#define XN_REC_SZ 256
#define XN_RECHDR_SZ 8
#define XN_RECKEY_SZ 120
#define XN_RECVAL_SZ 128

//#define XN_BUCKETS 101
#define XN_BUCKETS 3

//system call wrappers
void *xn_malloc(size_t size) {
    void *p;
    if (!(p = malloc(size))) {
        fprintf(stderr, "xenondb: malloc failed\n");
        exit(1);
    }
    return p;
}

void xn_free(void *ptr) {
    free(ptr);
}

void xn_write(int fd, const void* buf, size_t count) {
    int total = 0;
    while (total < count) {
        int result = write(fd, buf + total, count - total);
        if (result < 0) {
            if (errno != EINTR) {
                fprintf(stderr, "xenondb: write failed\n");
                exit(1);
            }
        } 

        total += result;
    }
}

int xn_open(const char* path, int flags, mode_t mode) {
    int fd;
    if ((fd = open(path, flags, mode)) == -1) {
        fprintf(stderr, "xenondb: open failed.");
        exit(1);
    }
    return fd;
}

off_t xn_seek(int fd, off_t offset, int whence) {
    int result;
    if ((result = lseek(fd, offset, whence)) == -1) {
        fprintf(stderr, "xenondb: lseek failed.");
        exit(1);
    }

    return result;
}

void xn_read(int fd, void *buf, size_t count) {
    int result;
    int total = 0;
    while (total < count) {
        int result = read(fd, buf + total, count - total);
        if (result < 0) {
            fprintf(stderr, "xenondb: write failed\n");
            exit(1);
        } 

        total += result;
    }
}

//xenon db
struct xndb {
    char dir_path[256];
    char data_path[256];
    int iter_block_idx;
    int iter_rec_idx;
};

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
    int next = -1;
    memcpy(buf + sizeof(int), &next, sizeof(int));
    for (int i = 0; i < XN_BUCKETS; i++) {
        xn_write(fd, buf, XN_BLK_SZ);
    }

    /*
    //write header of data file
    char hdr[XN_BLKHDR_SZ];
    memset(hdr, 0, XN_BLKHDR_SZ);
    xn_seek(fd, 0, SEEK_SET);
    xn_write(fd, hdr, XN_BLKHDR_SZ);*/

    close(fd);

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

int xndb_read_rec_count(int fd, int block_idx) {
    char buf[XN_BLKHDR_SZ];
    xn_seek(fd, block_idx * XN_BLK_SZ, SEEK_SET);
    xn_read(fd, buf, XN_BLKHDR_SZ);
    return *((int*)(buf));
}

void xndb_write_rec_count(int fd, int block_idx, int rec_count) {
    xn_seek(fd, block_idx * XN_BLK_SZ, SEEK_SET);
    xn_write(fd, &rec_count, sizeof(int));
}

void xndb_read_key(int fd, int block_idx, int idx, struct xnkey* key) {
    xn_seek(fd, block_idx * XN_BLK_SZ + XN_BLKHDR_SZ + XN_REC_SZ * idx + XN_RECHDR_SZ, SEEK_SET);
    xn_read(fd, key, XN_RECKEY_SZ);
}

void xndb_read_value(int fd, int block_idx, int idx, struct xnvalue* value) {
    xn_seek(fd, block_idx * XN_BLK_SZ + XN_BLKHDR_SZ + XN_REC_SZ * idx + XN_RECHDR_SZ + XN_RECKEY_SZ, SEEK_SET);
    xn_read(fd, value, XN_RECVAL_SZ);
}

int xndb_find_idx(struct xndb *db, int block_idx, const char* key) {
    int fd = xn_open(db->data_path, O_RDWR, 0666);

    int rec_count = xndb_read_rec_count(fd, block_idx);
    struct xnkey cur_key;
    for (int i = 0; i < rec_count; i++) {
        xndb_read_key(fd, block_idx, i, &cur_key);
        if (strcmp(key, (char*)&cur_key) == 0) {
            close(fd);
            return i;
        }
    }

    close(fd);
    return -1;
}

void xndb_write_valid(int fd, int block_idx, int idx, bool valid) {
    xn_seek(fd, block_idx * XN_BLK_SZ + XN_BLKHDR_SZ + idx * XN_REC_SZ + sizeof(int), SEEK_SET);
    xn_write(fd, &valid, sizeof(bool));
}

bool xndb_read_valid(int fd, int block_idx, int idx) {
    bool valid;
    xn_seek(fd, block_idx * XN_BLK_SZ + XN_BLKHDR_SZ + idx * XN_REC_SZ + sizeof(int), SEEK_SET);
    xn_read(fd, &valid, sizeof(bool));
    return valid;
}

void xndb_write_next(int fd, int block_idx, int rec_idx, int next) {
    xn_seek(fd, block_idx * XN_BLK_SZ + XN_BLKHDR_SZ + rec_idx * XN_REC_SZ, SEEK_SET);
    xn_write(fd, &next, sizeof(int));
}

int xndb_read_next(int fd, int block_idx, int rec_idx) {
    int next;
    xn_seek(fd, block_idx * XN_BLK_SZ + XN_BLKHDR_SZ + rec_idx * XN_REC_SZ, SEEK_SET);
    xn_read(fd, &next, sizeof(int));
    return next;
}

void xndb_write_key(int fd, int block_idx, int idx, const char* key, size_t size) {
    xn_seek(fd, block_idx * XN_BLK_SZ + XN_BLKHDR_SZ + idx * XN_REC_SZ + XN_RECHDR_SZ, SEEK_SET);
    xn_write(fd, key, strlen(key) + 1); //include null terminator
}

void xndb_write_value(int fd, int block_idx, int idx, const char* value, size_t size) {
    xn_seek(fd, block_idx * XN_BLK_SZ + XN_BLKHDR_SZ + idx * XN_REC_SZ + XN_RECHDR_SZ + XN_RECKEY_SZ, SEEK_SET);
    xn_write(fd, value, strlen(value) + 1); //include null terminator*/
}

int xndb_get_block_idx(const char* key) {
    size_t total = 0;
    for (int i = 0; i < strlen(key); i++) {
        total += (i + 1) * key[i];
    }

    return total % XN_BUCKETS + 1;
}

void xndb_add_to_freelist(int fd, int block_idx, int rec_idx) {
    int cur;
    xn_seek(fd, block_idx * XN_BLK_SZ + sizeof(int), SEEK_SET);
    xn_read(fd, &cur, sizeof(int));

    xndb_write_next(fd, block_idx, rec_idx, cur);
    xn_seek(fd, block_idx * XN_BLK_SZ + sizeof(int), SEEK_SET);
    xn_write(fd, &rec_idx, sizeof(int));
}

int xndb_remove_freelist_rec(int fd, int block_idx) {
    int cur;
    xn_seek(fd, block_idx * XN_BLK_SZ + sizeof(int), SEEK_SET);
    xn_read(fd, &cur, sizeof(int));

    if (cur == -1)
        return cur;

    int next = xndb_read_next(fd, block_idx, cur);
    xn_seek(fd, block_idx * XN_BLK_SZ + sizeof(int), SEEK_SET);
    xn_write(fd, &next, sizeof(int));

    return cur;
}

struct xnstatus xndb_put(struct xndb *db, const char *key, const char *value) {
    assert(strlen(key) < XN_RECKEY_SZ && "key length is larger than XN_RECKEY_SZ");
    assert(strlen(value) < XN_RECVAL_SZ && "value length is larger than XN_RECVAL_SZ");

    int block_idx = xndb_get_block_idx(key);
    int idx = xndb_find_idx(db, block_idx, key);
    if (idx != -1) {
        int fd = xn_open(db->data_path, O_RDWR, 0666);

        xndb_write_value(fd, block_idx, idx, value, strlen(value) + 1);

        return xnstatus_create(true, NULL);
    } else {
        int fd = xn_open(db->data_path, O_RDWR, 0666);

        int free_idx = xndb_remove_freelist_rec(fd, block_idx);
        if (free_idx != -1) {
            xndb_write_valid(fd, block_idx, free_idx, true);
            xndb_write_key(fd, block_idx, free_idx, key, strlen(key) + 1);
            xndb_write_value(fd, block_idx, free_idx, value, strlen(value) + 1);
            close(fd);
            return xnstatus_create(true, NULL);
        }

        int rec_count = xndb_read_rec_count(fd, block_idx);

        assert((rec_count + 1) * XN_REC_SZ < XN_BLK_SZ - XN_BLKHDR_SZ && "block cannot fit anymore records");

        xndb_write_valid(fd, block_idx, rec_count, true);
        xndb_write_key(fd, block_idx, rec_count, key, strlen(key) + 1);
        xndb_write_value(fd, block_idx, rec_count, value, strlen(value) + 1);

        rec_count++;
        xndb_write_rec_count(fd, block_idx, rec_count);

        close(fd);

        return xnstatus_create(true, NULL);
    }
}

struct xnstatus xndb_get(struct xndb *db, const char *key, struct xnvalue* result) {
    assert(strlen(key) < XN_RECKEY_SZ && "key length is larger than XN_RECKEY_SZ");

    int block_idx = xndb_get_block_idx(key);
    int idx = xndb_find_idx(db, block_idx, key);
    int fd = xn_open(db->data_path, O_RDWR, 0666);

    if (idx == -1 || !xndb_read_valid(fd, block_idx, idx)) {
        close(fd);
        return xnstatus_create(false, "xenondb: key not found");
    }

    xndb_read_value(fd, block_idx, idx, result);
    close(fd);

    return xnstatus_create(true, NULL);
}

struct xnstatus xndb_delete(struct xndb *db, const char *key) {
    assert(strlen(key) < XN_RECKEY_SZ && "key length is larger than XN_RECKEY_SZ");

    int block_idx = xndb_get_block_idx(key);
    int idx = xndb_find_idx(db, block_idx, key);
    if (idx == -1)
        return xnstatus_create(false, "xenondb: key not found");

    int fd = xn_open(db->data_path, O_RDWR, 0666);
    xndb_write_valid(fd, block_idx, idx, false);
    xndb_add_to_freelist(fd, block_idx, idx);
    close(fd);

    return xnstatus_create(true, NULL);
}

void xniter_reset(struct xndb *db) {
    db->iter_block_idx = 1; //first index block
    db->iter_rec_idx = -1; //one before first record
}

bool xniter_next(struct xndb *db) {
    int fd = xn_open(db->data_path, O_RDWR, 0666);

    int rec_count = xndb_read_rec_count(fd, db->iter_block_idx);

    while (true) {
        db->iter_rec_idx++;
        if (db->iter_rec_idx >= rec_count) {
            db->iter_block_idx++;
            if (db->iter_block_idx > XN_BUCKETS) {
                close(fd);
                return false;
            }
            rec_count = xndb_read_rec_count(fd, db->iter_block_idx);
            db->iter_rec_idx = -1;
            continue;
        }

        if (xndb_read_valid(fd, db->iter_block_idx, db->iter_rec_idx)) {
            close(fd);
            return true;
        }
    }

    close(fd);

    return false;
}

void xniter_read_key_value(struct xndb *db, struct xnkey *key, struct xnvalue *value) {
    int fd = xn_open(db->data_path, O_RDWR, 0666);
    xndb_read_key(fd, db->iter_block_idx, db->iter_rec_idx, key);
    xndb_read_value(fd, db->iter_block_idx, db->iter_rec_idx, value);
    close(fd);
}

void put_test() {
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
    return 0;
}
