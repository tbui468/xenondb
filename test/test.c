#include <stdio.h>
#include "test.h"
#include "util_test.h"
#include "file_test.h"
#include "page_test.h"

void append_test(void (*fcn)(void)) {
    fcns[fcn_count++] = fcn;    
}

int main() {
    freopen("/dev/null", "w", stderr); //ignoring stderr

    util_tests();
    file_tests();
    page_tests();
    
    int passed_count = 0;
    
    for (int i = 0; i < fcn_count; i++) {
        passed = true;
        fcns[i]();
        printf("%-30s", fcn_name);
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
