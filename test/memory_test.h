#pragma once

#include "util.h"
#include "file.h"
#include "test.h"

bool alloc_ok(void **ptp) {
    xnmm_init();
    xnmm_alloc(ptp, xn_ensure(xn_malloc(1, ptp)), xn_free);
    xn_ensure(true);

    return xn_ok();
}

bool alloc_fail(void **ptp) {
    xnmm_init();
    xnmm_alloc(ptp, xn_ensure(xn_malloc(1, ptp)), xn_free);
    xn_ensure(false);

    return xn_ok();
}

void memory_basic_alloc() {
    {
        void *ptr;
        alloc_ok(&ptr);
        assert(ptr != NULL);
        free(ptr);
    }
    {
        void *ptr;
        alloc_fail(&ptr);
        assert(ptr == NULL);
    }
}

//outer allocation call first
bool outerok_nestedok_alloc(void **ptr1, void **ptr2) {
    xnmm_init();
    xnmm_alloc(ptr1, xn_ensure(xn_malloc(1, ptr1)), xn_free);
    xn_ensure(alloc_ok(ptr2));
    return xn_ok();
}
bool outerok_nestedfail_alloc(void **ptr1, void **ptr2) {
    xnmm_init();
    xnmm_alloc(ptr1, xn_ensure(xn_malloc(1, ptr1)), xn_free);
    xn_ensure(alloc_fail(ptr2));
    return xn_ok();
}
bool outerfail_nestedok_alloc(void **ptr1, void **ptr2) {
    xnmm_init();
    xnmm_alloc(ptr1, xn_ensure(xn_malloc(1, ptr1)), xn_free);
    xn_ensure(false);
    xn_ensure(alloc_ok(ptr2));
    return xn_ok();
}
bool outerfail_nestedfail_alloc(void **ptr1, void **ptr2) {
    xnmm_init();
    xnmm_alloc(ptr1, xn_ensure(xn_malloc(1, ptr1)), xn_free);
    xn_ensure(false);
    xn_ensure(alloc_fail(ptr2));
    return xn_ok();
}

//nested allocation call first
bool nestedok_outerok(void **ptr1, void **ptr2) {
    xnmm_init();
    xn_ensure(alloc_ok(ptr2));
    xnmm_alloc(ptr1, xn_ensure(xn_malloc(1, ptr1)), xn_free);
    return xn_ok();
}
bool nestedfail_outerok(void **ptr1, void **ptr2) {
    xnmm_init();
    xn_ensure(alloc_fail(ptr2));
    xnmm_alloc(ptr1, xn_ensure(xn_malloc(1, ptr1)), xn_free);
    return xn_ok();
}
bool nestedok_outerfail(void **ptr1, void **ptr2) {
    xnmm_init();
    //Note: *ptr2 was successfully allocated in nested fcn, so
    //it will NOT be freed when this function fails.  Should implement
    //a xnmm_free_on_failure(ptr2, xn_free) macro if freeing is required
    xn_ensure(alloc_ok(ptr2));
    //xnmm_free_on_failure(ptr2, xn_free); implement this macro if necessary
    xnmm_alloc(ptr1, xn_ensure(xn_malloc(1, ptr1)), xn_free);
    xn_ensure(false);
    return xn_ok();
}
bool nestedfail_outerfail(void **ptr1, void **ptr2) {
    xnmm_init();
    xn_ensure(alloc_fail(ptr2));
    xnmm_alloc(ptr1, xn_ensure(xn_malloc(1, ptr1)), xn_free);
    xn_ensure(false);
    return xn_ok();
}

void memory_nested_alloc() {
    //outer first
    {
        void *ptr1 = NULL;
        void *ptr2 = NULL;
        assert(outerok_nestedok_alloc(&ptr1, &ptr2));
        assert(ptr1);
        assert(ptr2);
        free(ptr1);
        free(ptr2);
    }
    {
        void *ptr1 = NULL;
        void *ptr2 = NULL;
        assert(outerok_nestedfail_alloc(&ptr1, &ptr2) == false);
        assert(!ptr1);
        assert(!ptr2);
    }
    {
        void *ptr1 = NULL;
        void *ptr2 = NULL;
        assert(outerfail_nestedok_alloc(&ptr1, &ptr2) == false);
        assert(!ptr1);
        assert(!ptr2);
    }
    {
        void *ptr1 = NULL;
        void *ptr2 = NULL;
        assert(outerfail_nestedfail_alloc(&ptr1, &ptr2) == false);
        assert(!ptr1);
        assert(!ptr2);
    }


    //nested first
    {
        void *ptr1 = NULL;
        void *ptr2 = NULL;
        assert(nestedok_outerok(&ptr1, &ptr2));
        assert(ptr1);
        assert(ptr2);
        free(ptr1);
        free(ptr2);
    }
    {
        void *ptr1 = NULL;
        void *ptr2 = NULL;
        assert(nestedfail_outerok(&ptr1, &ptr2) == false);
        assert(!ptr1);
        assert(!ptr2);
    }
    {
        void *ptr1 = NULL;
        void *ptr2 = NULL;
        assert(nestedok_outerfail(&ptr1, &ptr2) == false);
//        assert(!ptr1);
//        assert(!ptr2);
    }
    {
        void *ptr1 = NULL;
        void *ptr2 = NULL;
        assert(nestedfail_outerfail(&ptr1, &ptr2) == false);
        assert(!ptr1);
        assert(!ptr2);
    }
}

void memory_tests() {
    append_test(memory_basic_alloc);
    append_test(memory_nested_alloc);
}
