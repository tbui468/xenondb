#include <stdio.h>
#include <stdlib.h>
#include <curl/curl.h>

#include "test.h"
#include "util_test.h"
#include "file_test.h"
#include "page_test.h"
#include "table_test.h"
#include "log_test.h"
#include "logitr_test.h"
#include "db_test.h"
#include "paging_test.h"
#include "memory_test.h"
#include "tx_test.h"
#include "wrtx_test.h"
#include "container_test.h"
#include "containeritr_test.h"
#include "heap_test.h"
#include "rs_test.h"

struct string {
    char *ptr;
    size_t len;
};

void append_test(void (*fcn)(void)) {
    fcns[fcn_count++] = fcn;    
}

size_t write_fcn(void *ptr, size_t size, size_t nmemb, struct string *s) {
    size_t new_len = s->len + size * nmemb;
    s->ptr = realloc(s->ptr, new_len + 1);
    if (s->ptr == NULL) {
        exit(1);
    }
    memcpy(s->ptr + s->len, ptr, size * nmemb);
    s->ptr[new_len] = '\0';
    s->len = new_len;
    return size * nmemb;
}

void init_string(struct string *s) {
    s->len = 0;
    s->ptr = malloc(s->len + 1);
    if (s->ptr == NULL) {
        exit(1);
    }
    s->ptr[0] = '\0';
}

bool next_float(struct string s, off_t *off, float *f) {
    if ((*off) == 0) { //skip beginning of payload
        while (*(s.ptr + *off) != '[')
            (*off)++;
        (*off)++; //skip [
    }

    if (*(s.ptr + *off) == ',')
        (*off)++;

    char *end;
    *f = strtof(s.ptr + *off, &end);
    if (s.ptr + *off == end)
        return false;

    *off = end - s.ptr;
    return true;
}

void integrated_test() {
    CURL *curl;
    CURLcode res;
    curl = curl_easy_init();
    struct string s;
    float vector[768];
    int vec_count = 0;
    if (curl) {
        init_string(&s);
        curl_easy_setopt(curl, CURLOPT_URL, "http://localhost:11434/api/embeddings");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, "{ \"model\": \"nomic-embed-text\", \"prompt\": \"this is a cat\", \"stream\": false }");
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_fcn);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s);
        res = curl_easy_perform(curl);
        if (res != CURLE_OK)
            fprintf(stderr, "shit broke: %s\n", curl_easy_strerror(res));
        
        off_t off = 0;
        float f;
        int count = 0;
        while (next_float(s, &off, &f)) {
            vector[vec_count] = f;
            vec_count++;
        }

        free(s.ptr);
        curl_easy_cleanup(curl);
    }

    system("rm -rf dummy");

    {
        struct xndb *db;
        assert(xndb_create("dummy", true, &db));
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_WR));

        struct xnrs rs;
        assert(xnrs_open(&rs, db, "data", true, XNRST_HEAP, tx));

        {
            struct xnitemid id;
            assert(xnrs_put(rs, sizeof(vector), (uint8_t*)vector, &id));
        }

        struct xnrsscan scan;
        assert(xnrsscan_open(&scan, rs));
        bool more;
        int count = 0;
        while (true) {
            assert(xnrsscan_next(&scan, &more));
            if (!more)
                break; 
            count++;
        }
        
        assert(xntx_commit(tx));
        assert(xndb_free(db));
    }

    {
        struct xndb *db;
        assert(xndb_create("dummy", false, &db));
        struct xntx *tx;
        assert(xntx_create(&tx, db, XNTXMODE_WR));

        struct xnrs rs;
        assert(xnrs_open(&rs, db, "data", false, XNRST_HEAP, tx));

        struct xnrsscan scan;
        assert(xnrsscan_open(&scan, rs));
        bool more;
        int count = 0;
        while (true) {
            assert(xnrsscan_next(&scan, &more));
            if (!more)
                break; 

            struct xnitemid id;
            assert(xnrsscan_itemid(&scan, &id));
            size_t size;
            assert(xnrs_get_size(rs, id, &size));
            assert(size == sizeof(vector));
            char *buf = malloc(size);
            assert(xnrs_get(rs, id, buf, size));
            assert(memcmp(vector, buf, size) == 0);
            count++;
        }
        
        assert(xntx_commit(tx));
        assert(xndb_free(db));
    }
}

int main() {
    append_test(integrated_test);
    //TODO commenting this out causes some messages to show up
    //when running some tests.  Are these stack traces from errors in 
    //the program?
//    freopen("/dev/null", "w", stderr); //ignoring stderr

/*    util_tests();
    memory_tests();
    file_tests();
    page_tests();
    table_tests();
    log_tests();
    logitr_tests();
    paging_tests();
    tx_tests();
    wrtx_tests();
    container_tests();
    containeritr_tests();
    db_tests();*/
    //heap_tests();
	//rs_tests();
   
    int passed_count = 0;
    
    for (int i = 0; i < fcn_count; i++) {
        system("rm -rf dummy");

        passed = true;
        fcns[i]();
        printf("%-35s", fcn_name);
        if (passed) {
            printf("%s%s%s\n", KGRN, "passed", RESET);
            passed_count++;
        } else {
            printf("%s%s%s\n", KRED, "failed", RESET);
        }
    }

    printf("%d/%d tests passed\n", passed_count, fcn_count);
    return 0;
}
