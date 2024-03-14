#pragma once

#include "test.h"
#include "file.h"

//TODO test opening file in O_DIRECT + O_SYNC mode
//trying to write a buffer that is NOT aligned to page byte should fail

void file_open_close() {
    struct xnfile *handle;
    assert(xnfile_create(&handle, "dummy", true, false));
    assert(xnfile_close((void**)&handle));
}

void file_set_size() {
    {
        struct xnfile *handle;
        assert(xnfile_create(&handle, "dummy", true, false));
        assert(xnfile_set_size(handle, 1024));
        assert(handle->size == 1024);
        assert(xnfile_close((void**)&handle));
    }


    {
        struct xnfile *handle;
        assert(xnfile_create(&handle, "dummy", true, false));
        assert(handle->size == 1024);
        assert(xnfile_close((void**)&handle));
    }
}

void file_read_write() {
    {
        struct xnfile *handle;
        assert(xnfile_create(&handle, "dummy", true, false));
        assert(xnfile_set_size(handle, 1024));
        assert(handle->size == 1024);

        //write/read from 0 offset
        {
            const char *msg = "hello";
            assert(xnfile_write(handle, msg, 0, strlen(msg) + 1));
            char buf[8];
            assert(xnfile_read(handle, buf, 0, strlen(msg) + 1));
            assert(strcmp(msg, buf) == 0);
        }

        //write/read from non-zero offset
        {
            const char *msg = "hello";
            assert(xnfile_write(handle, msg, 1000, strlen(msg) + 1));
            char buf[8];
            assert(xnfile_read(handle, buf, 1000, strlen(msg) + 1));
            assert(strcmp(msg, buf) == 0);
        }

        //attempt to write past end of file
        {
            const char *msg = "hello";
            assert(xnfile_write(handle, msg, 1024, strlen(msg) + 1) == false);
        }

        //attempt to read past end of file
        {
            const char *msg = "hello";
            char buf[8];
            assert(xnfile_read(handle, buf, 1024, strlen(msg) + 1) == false);
        }
        assert(xnfile_close((void**)&handle));
    }
}

void file_map_unmap() {
    {
        struct xnfile *handle;
        assert(xnfile_create(&handle, "dummy", true, false));
        assert(xnfile_set_size(handle, 5000));
        assert(handle->size == 5000);

        //map offset 0
        {
            const char *msg = "hello";
            assert(xnfile_write(handle, msg, 0, strlen(msg) + 1));
            void *buf;
            assert(xnfile_mmap(handle, 0, 8, &buf));
            assert(strcmp(msg, buf) == 0);
            assert(xnfile_munmap(buf, 8));
        }

        //map on block offset
        {
            const char *msg = "hello";
            assert(xnfile_write(handle, msg, handle->block_size, strlen(msg) + 1));
            void *ptr;
            assert(xnfile_mmap(handle, handle->block_size, 8, &ptr));
            assert(strcmp(msg, ptr) == 0);
            assert(xnfile_munmap(ptr, 8));
        }

        //attempt map on non-block offset
        {
            void *ptr;
            assert(xnfile_mmap(handle, 1, 8, &ptr) == false);
        }

        //attempt map past end of file
        {
            void *ptr;
            assert(xnfile_mmap(handle, handle->block_size, handle->block_size, &ptr) == false);
        }

        assert(xnfile_close((void**)&handle));
    }
}

void file_tests() {
    append_test(file_open_close);
    append_test(file_set_size);
    append_test(file_read_write);
    append_test(file_map_unmap);
}
