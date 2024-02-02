#ifndef XN_UTIL
#define XN_UTIL

#include <sys/types.h>
#include <stddef.h>

void *xn_malloc(size_t size);
void xn_free(void *ptr);
void xn_write(int fd, const void* buf, size_t count);
int xn_open(const char* path, int flags, mode_t mode);
off_t xn_seek(int fd, off_t offset, int whence);
void xn_read(int fd, void *buf, size_t count);

#endif
