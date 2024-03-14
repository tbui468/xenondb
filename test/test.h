#pragma once

#include <stdbool.h>
#include <string.h>

void (*fcns[64])();
char fcn_name[128];
bool passed = true;
int fcn_count = 0;

#define MAX_ERR 64

#define KRED "\x1B[31m"
#define KGRN "\x1B[32m"
#define RESET "\x1B[0m"

#define assert(b) strcpy(fcn_name, __func__); \
    if (!(b)) { \
        passed = false; \
    }

void append_test(void (*fcn)());
