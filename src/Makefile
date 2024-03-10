main: remove_o
	mv libxenondb.a ./../test/libxenondb.a

remove_o: create_lib
	rm *.o

create_lib: compile
	ar -rcs libxenondb.a *.o

compile: util.h util.c file.h file.c log.h log.c page.h page.c table.c table.h tx.h tx.c db.h db.c
	gcc -std=c11 -c file.c util.c log.c page.c table.c tx.c db.c -pthread -g

