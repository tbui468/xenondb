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

    //write block 0
    char buf[XN_BLK_SZ];
    memset(buf, 0, XN_BLK_SZ);
    xn_write(fd, buf, XN_BLK_SZ);

    //write header of data file
    char hdr[XN_BLKHDR_SZ];
    memset(hdr, 0, XN_BLKHDR_SZ);
    xn_seek(fd, 0, SEEK_SET);
    xn_write(fd, hdr, XN_BLKHDR_SZ);

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

struct xnblk_hdr {
    int rec_count;
};

void xndb_read_header(int fd, struct xnblk_hdr *hdr) {
    char buf[XN_BLKHDR_SZ];
    xn_seek(fd, 0, SEEK_SET);
    xn_read(fd, buf, XN_BLKHDR_SZ);
    hdr->rec_count = *((int*)(buf));
}

void xndb_write_header(int fd, struct xnblk_hdr *hdr) {
    xn_seek(fd, 0, SEEK_SET);
    xn_write(fd, &hdr->rec_count, sizeof(int));
}

void xndb_read_key(int fd, int idx, struct xnkey* key) {
    xn_seek(fd, XN_BLKHDR_SZ + XN_REC_SZ * idx + XN_RECHDR_SZ, SEEK_SET);
    xn_read(fd, key, XN_RECKEY_SZ);
}

void xndb_read_value(int fd, int idx, struct xnvalue* value) {
    xn_seek(fd, XN_BLKHDR_SZ + XN_REC_SZ * idx + XN_RECHDR_SZ + XN_RECKEY_SZ, SEEK_SET);
    xn_read(fd, value, XN_RECVAL_SZ);
}

int xndb_find_idx(struct xndb *db, const char* key) {
    int fd = xn_open(db->data_path, O_RDWR, 0666);

    struct xnblk_hdr hdr;
    xndb_read_header(fd, &hdr);

    struct xnkey cur_key;
    for (int i = 0; i < hdr.rec_count; i++) {
        xndb_read_key(fd, i, &cur_key);

        if (strcmp(key, (char*)&cur_key) == 0) {
            close(fd);
            return i;
        }
    }

    close(fd);
    return -1;
}

void xndb_write_key(int fd, int idx, const char* key, size_t size) {
    xn_seek(fd, XN_BLKHDR_SZ + idx * XN_REC_SZ + XN_RECHDR_SZ, SEEK_SET);
    xn_write(fd, key, strlen(key) + 1); //include null terminator
}

void xndb_write_value(int fd, int idx, const char* value, size_t size) {
    xn_seek(fd, XN_BLKHDR_SZ + idx * XN_REC_SZ + XN_RECHDR_SZ + XN_RECKEY_SZ, SEEK_SET);
    xn_write(fd, value, strlen(value) + 1); //include null terminator*/
}

struct xnstatus xndb_put(struct xndb *db, const char *key, const char *value) {
    assert(strlen(key) < XN_RECKEY_SZ && "key length is larger than XN_RECKEY_SZ");
    assert(strlen(value) < XN_RECVAL_SZ && "value length is larger than XN_RECVAL_SZ");

    int idx = xndb_find_idx(db, key);
    if (idx != -1) {
        int fd = xn_open(db->data_path, O_RDWR, 0666);

        xndb_write_value(fd, idx, value, strlen(value) + 1);

        return xnstatus_create(true, NULL);
    } else {
        int fd = xn_open(db->data_path, O_RDWR, 0666);

        struct xnblk_hdr hdr;
        xndb_read_header(fd, &hdr);

        xndb_write_key(fd, hdr.rec_count, key, strlen(key) + 1);
        xndb_write_value(fd, hdr.rec_count, value, strlen(value) + 1);

        hdr.rec_count++;
        xndb_write_header(fd, &hdr);
        close(fd);

        return xnstatus_create(true, NULL);
    }
}

struct xnstatus xndb_get(struct xndb *db, const char *key, struct xnvalue* result) {
    assert(strlen(key) < XN_RECKEY_SZ && "key length is larger than XN_RECKEY_SZ");

    int idx = xndb_find_idx(db, key);
    if (idx == -1)
        return xnstatus_create(false, "xenondb: key not found");

    int fd = xn_open(db->data_path, O_RDWR, 0666);
    xndb_read_value(fd, idx, result);
    close(fd);

    return xnstatus_create(true, NULL);
}

void put_get_test() {
    struct xndb* db = xndb_init("students", true);

    struct xnstatus status;
    status = xndb_put(db, "cat", "a");
    status = xndb_put(db, "dog", "b");
    status = xndb_put(db, "whale", "c");
    status = xndb_put(db, "whale", "d");

    struct xnvalue value;
    status = xndb_get(db, "cat", &value);
    assert(strcmp((char*)&value, "a") == 0 && "put/get 1 failed");

    status = xndb_get(db, "dog", &value);
    assert(strcmp((char*)&value, "b") == 0 && "put/get 2 failed");

    status = xndb_get(db, "whale", &value);
    assert(strcmp((char*)&value, "d") == 0 && "put on existing key failed");

    xndb_free(db);

    printf("put_get_test passed\n");

    //TODO run cleanup code to remove 'students' directory (and all correponding files)
}

int main(int argc, char** argv) {
    put_get_test();
    return 0;
}
