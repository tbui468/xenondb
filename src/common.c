#include "common.h"
#include <string.h>

char _xnerr_buf[1024];

/*
bool xnstatus_failed(const char *msg) {
    strcpy(_xnerr_buf, __FILE__);
    //strcat(_xnerr_buf, __LINE__);
    strcat(_xnerr_buf, __func__);
    strcat(_xnerr_buf, msg);
    return false;
}*/

/*
char *xnerr_tostring() {
    return _xnerr_buf;
}*/
