#ifndef XN_UTIL
#define XN_UTIL

#include <sys/types.h>
#include <stdbool.h>
#include <stddef.h>

void *xn_malloc(size_t size);
void xn_write(int fd, const void* buf, size_t count);
int xn_open(const char* path, int flags, mode_t mode);
off_t xn_seek(int fd, off_t offset, int whence);
void xn_read(int fd, void *buf, size_t count);

struct xnilist {
    int count;
    int capacity;
    int* ints;
};

void xnilist_init(struct xnilist **il);
void xnilist_append(struct xnilist *il, int n); 
bool xnilist_contains(struct xnilist *il, int n);
void *xn_aligned_alloc(size_t size);

char *xn_strcpy(char *dst, const char *src);
char *xn_strcat(char* dst, const char *src);

#endif
