#pragma once

#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#define xnresult_t __attribute__((warn_unused_result)) bool

#define xnmm_init() int _all_ptr_count_ = 0; \
                     int _defer_ptr_count_ = 0; \
                     void *_all_ptrs_[16]; \
                     void *_defer_ptrs_[16];

#define xnmm_defer(p) _defer_ptrs_[_all_ptr_count_] = p; \
                      _defer_ptr_count_++;

#define xnmm_cleanup() for (int _i_ = _defer_ptr_count_ - 1; _i_ >= 0; _i_--) { \
                        _defer_ptrs_[_i_]; \
                    }

#define xmm_clean_all() //this should be called if an error occurs


extern char _xnerr_buf[1024];

#define xn_ok() ({ xnmm_cleanup(); true; })

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
