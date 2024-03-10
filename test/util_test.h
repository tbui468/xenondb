#pragma once

#include "util.h"
#include "test.h"
#include <string.h>

/*
 * result tests
 */

xnresult_t okay() {
    xnmm_init();
    return xn_ok();
}

xnresult_t fail() {
    xnmm_init();
    xn_ensure(false);
    return xn_ok();
}

xnresult_t nested_okay() {
    xnmm_init();
    xn_ensure(okay());
    xn_ensure(okay());
    return xn_ok();
}

xnresult_t nested_fail() {
    xnmm_init();
    xn_ensure(okay());
    xn_ensure(fail());
    return xn_ok();
}

/*
 * syscall tests
 */

xnresult_t syscall_succeed() {
    xnmm_init();
    struct stat s;
    xn_ensure(xn_stat("util_test.h", &s));
    return xn_ok();
}

xnresult_t syscall_fail() {
    xnmm_init();
    struct stat s;
    xn_ensure(xn_stat("not_a_file.c", &s));
    return xn_ok();
}

xnresult_t nested_syscall_succeed() {
    xnmm_init();
    xn_ensure(syscall_succeed());
    return xn_ok();
}

xnresult_t nested_syscall_fail() {
    xnmm_init();
    xn_ensure(syscall_fail());
    return xn_ok();
}

void util_basic_result() {
    assert(okay());
    assert(fail() == false);
}

void util_nested_result() {
    assert(nested_okay());
    assert(nested_fail() == false);
}

void util_syscall() {
    assert(syscall_succeed());    
    assert(syscall_fail() == false);    
}

void util_nested_syscall() {
    assert(nested_syscall_succeed());
    assert(nested_syscall_fail() == false);
}

void util_tests() {
    append_test(util_basic_result);
    append_test(util_nested_result);
    append_test(util_syscall);
    append_test(util_nested_syscall);
}

//TODO how to check if defer and automatic memory freeing is working???
//assert should be defined by use
//when an assert fails, put function name into list so that we can print it out later
//could implement automatic closing of functions and try to open and invalid fd
//or could try to
