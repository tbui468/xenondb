# XenonDB - An Educational Storage Engine

## Introduction
XenonDB is a basic storage engine for educational purposes.  The goal is a basic program that can demonstrate
ACID properties.  The engine is implemented in C.

## Infrastructure
We need all-or-nothing semantics when allocating memory in functions that can fail.  If a failure occurs, ALL memory allocations in that function need to be released before the function returns an error code.  This keeps memory management simple(r).

xn_ok and xn_fail are the two status macros that all functions that could result in an error return.  xn_ensure is a macro used to exit a function with a failure status if the argument evaluates to false.  This macro is wrapped around all functions that could possibly return an error.  

The code

if ((ptr = malloc(size)) == NULL) {
    //save error message
    return xnfailed();
}

if ((fd = open(path)) == -1) {
    //save error message
    return xnfailed();
}

if (close(fd) != 0) {
    //save error message
    return xnfailed();
}

return xn_ok();

now becomes

xn_ensure((ptr = malloc(size)) != NULL);
xn_ensure((fd = open(path)) != -1);
xn_ensure(close(fd) == 0);
return xn_ok();

This is easier to read and less error prone.  

## File System Access
The file system module abstracts over the OS file system, and provides an file handle to the rest of the storage engine.  Writing to the file system is a lot more complex than it seems, and considering multiple OSs and compilers further complicates things.  To keep is simple, we will write code that works with Linux and the gcc compiler.

The struct xnfile will store information about a file in XenonDB.  Other modules and functions in XenonDB will pass around this struct.

struct xnfile {
    int fd;
    char *path;
    size_t size;
};

Creating a new xnfile is done with xnfile_create.  The absolute path of the file is saved rather than the relative path.  The second argument specifies whether the any writes on this file should be on stable storage before returning.  When writing data to disk, there are a few places where the data could be buffered - used libraries, the OS kernel and on the disk itself.  Buffering improves performance but sometimes we need to ensure that data is written to stable storage and the performance hit is acceptable.  O_DIRECT tells the kernel to bypass the kernel cache and write directly to the device.  O_SYNC and O_DATASYNC does a synchronous write to storage, BUT this data still may be cached by the storage device.  A system failure would result in lost data in that case.  In order to guarantee it's in stable storage and won't disappear on system failure, we also need to call fsync (or fdatasync) to flush or write-through the disk cache.  fsync returns when data is in permanent storage.  The parent directory will be synced everytime the file size is changed, so we will use O_DATASYNC to write only the data synchronously.  In summary, any writes that we want to ensure are in stable storage will be done on a file opened with the flags O_DIRECT and O_DATASYNC, and then fsync will be called to flush the disk cache.

![write caches from program to disk](caches.png)

xnfile_sync_parent is necessary to ensure that file metadata is saved to stable storage.  This is called whenever a files metadata is changed (when first creating a file or when a file size is changed).

xnfile_read and xnfile_write will read and write to a file, respectively.  Both the read and write system calls may return without reading/writing all the requested bytes, so they are called in a loop until all the requested bytes are processed.  read and write will return a -1 for errors and also if the function was interrupted before processing any bytes.  errno will be set to EINTR if the function was interrupted.  Errors should be reported, but if the call was interrupted the function should just be called again.

Read transactions (implemented later) will read data from the files on disk - the system calls mmap and unmmap are wrapped in two functions xnfile_mmap and xnfile_munmap.

xnfile_set_size changes the file size.

xnfile_sync calls fsync to flush the disk cache.  fsync is a very expensive system call, so we will avoid calling it unless necessary.  

xnfile_close will close the file descriptor and free the struct from memory.

## Paging
Rather than dealing with files directly, persistent data structures using in XenonDB will work with page-level
objects.  Pages can be allocated and freed.  The first page in each file is used to store metadata about the file,
including a bitmap to track free and allocated pages.

Many paging functions are just wrappers around the file system functions, and simply pass in the page size as
an argument.

The page table stores a key/value pair, where the key is the page number modulus the table size, and the key is
the page data.  This page data is either the mmapped file or a copy of the disk data in a local buffer.  

## Transactions
Transactions will be implemented using a single-writer / multiple-reader system.  We will
use a form of multi-versioning concurrency control (MVCC) to keep transactions isolated.

New read transactions will read directly from disk if there are no committed write transactions waiting to
write to table storage.  Once a write transaction commits, and before it writes its updates to stable storage,
new read transactions will read from the committed transactions modified pages.  Reader transactions will read from
the same snapshot during the lifetime of that transaction.  Once the write transactions updates are written to disk,
all new read transactions will go back to reading from disk.  Any active read transactions will still continue to read
from the snapshot.  Once all read transactions reading the snapshot end, the write transactions in-memory data will be
freed.  

Copy-on-write will be used when a write transaction modifies a page.
When a write transaction updates data, that page will be copied into a local page table for the write transaction.
Any future reads and writes from that write transaction will use this cached page.  Any other pages updated by
the transaction will also be copied into the local page table.  Reads from unmodified pages will just read directly
from disk.

If the write transaction is rollbacked, the local page table will be freed.  On commit, a log will first be written
with all the updates from that transaction.  After committing, any old read transactions will continue to read from
disk.  Any new read transactions will now first check the committed transactions local page table first before checking
for pages on disk.  Once all old read transactions end, no more transactions will be reading any of the same pages that
the commit modified - new transactions either read directly from the local page table, or are reading pages on disk that
the commit did not modify.  At this point the modified pages can safely be written to disk.  

Once all modified pages are written to disk and synced, subsequent read transactions will now read directly from disk again.
Any read transactions still reading the local page table will continue until they complete.  At that point the local page
table can be freed.  

A write transaction ends when the update log is written to disk.  At this point a new write transaction can begin before the
previous write transaction's local page table is written to disk.  If the old local page table was not written to disk yet, the new
write transaction will check the old local page table before going to disk.  Pages will also be copied from the old local page table
if available since those pages will be more up-to-date than the ones on disk.  The old local page table will persist in memory until
all reader transactions referencing it end, and when the new write transaction commits.  

Multiple page tables and their write transactions can be linked up and persist in memory if there is a lot of read/write traffic.
Write transactions and their tables can chained up (similar to a linked list).  The first page table is the currently active write 
transaction, and further nodes are older write transactions that have not written their updates to stable storage yet.
The terminating page table references the mmapped file on disk.

## Logging
Write-Ahead-Logging (WAL) is used to ensure that commits survive system failures.  Before updates are written to stable storage, a log
of the after-image is written to stable storage.  In the case of a system failure, these log records can be used to recover the file to
a working state.  Hardware failures can still cause lost or corrupt data, but this is out of the scope of XenonDB.

Transaction commits can either be synchronous, or asynchronous.  A synchronous commit will ensure that the log is written to stable storage
before returning to the caller.  An asynchronous commit will buffer the log records from multiple commits to amortize the cost of writing to disk.
The speed-up from using asynchronous commits comes at a cost, however.  If they system fails before the log is written to stable storage, 
the commit will be lost even if the caller received a successful commit message.

In many cases this is an acceptable risk given the increased throughput.  The commit type (sychronous or asynchronous) can be specified when
the storage engine is initialized.


## Slotted Pages
Slotted pages are the basic container used to organize data on a page.

The metadata in the page is followed by an array of pointers.  The pointers contain two integers - the first is the offset
of the value in the page, and the second is the length of that value.  When a new value is inserted into the page, a new pointer and
size is appended to the right-hand side of the array.  Values are appended from the end of the page and to the left-hand side of the current
values.  The page is full when the array and values meet up.


# Improvements and Additions

## Compression
Writing data to disk is a lot slower than compressing that data.

## Encryption
Data should be encrypted in case bad actors get a hold of it.

## Persistent Hash Table
Extendible hash tables can grow with minimal rewriting of data inside the table.

### Overflow Pages
### B+ Tree
### Multiple-write / Multiple-reader
### Distributed Commits
