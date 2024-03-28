main: test
	./test

test: libxenondb.a test.c test.h file_test.h util_test.h page_test.h table_test.h log_test.h logitr_test.h db_test.h paging_test.h memory_test.h tx_test.h container_test.h wrtx_test.h containeritr_test.h heap_test.h vector_test.h
	gcc test.c -L. -lxenondb -I./../src -lm -pthread -o test

example: main.c libxenondb.a
	gcc main.c -L. -lxenondb -I./../src -o main

clean:
	rm -rf students log main dummy
