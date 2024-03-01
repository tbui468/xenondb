#pragma once

#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

extern char _xnerr_buf[1024];

#define xn_ok() true

#define xn_failed(msg) ({ strcat(_xnerr_buf, "'"); \
    strcat(_xnerr_buf, __func__); \
    strcat(_xnerr_buf, "' in "); \
    strcat(_xnerr_buf, __FILE__); \
    strcat(_xnerr_buf, " [line "); \
    char buf[10]; \
    sprintf(buf, "%d", __LINE__); \
    strcat(_xnerr_buf, buf); \
    strcat(_xnerr_buf, "] failed: "); \
    strcat(_xnerr_buf, msg); \
    false; })

#define xnerr_tostring() _xnerr_buf

#define xn_ensure(b) if (!(b)) { xn_failed(strerror(errno)); }

bool xn_malloc(size_t size, void **ptr);
bool xn_aligned_malloc(size_t size, void **ptr);
