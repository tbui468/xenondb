# XenonDB - An Educational Storage Engine

## Introduction
This is a 
### Example Usage
### API

## Infrastructure
### Handling errors
C relies on return codes to report errors, and so we'll have to be extra careful to check the return value
every function call that could result in an error.

The macros xn_ok, xn_failed and xn_ensure will take care of error handling for us.  The compiler
directive __attribute__((warn_unused_result)) is applied to all functions that could result in an
error.  This gives us a nice warning if the result of the function call is not checked.  We will use
xn_ensure to check most of those return values.  

## File System Access
### API
The file system module (file.h, file.c) abstracts over the OS file system, and provides an file handle to
the rest of the storage engine.  
### Implementation
### Tests

## Paging
### API
Rather than dealing with files directly, persistent data structures using in XenonDB will work with page-level
objects.  
### Implementation
### Tests

## Transactions
### API
Transactions will be implemented using a single-writer / multiple-reader system.  We will
use a form of multi-versioning concurrency control (MVCC) to keep transactions from stepping on eachothers toes. 
### Implementation
### Tests

## Logging
Write-Ahead-Logging (WAL) is used to ensure that commits survive system failures.  
### API
### Implementation
### Tests

## Slotted Pages
### API
Slotted pages are the basic container used to organize data on a page.
### Implementation
### Tests

# Improvements and Additions

## Compression
### API
Writing data to disk is a lot slower than compressing that data.
### Implementation
### Tests

## Encryption
### API
Data should be encrypted in case bad actors get a hold of it.
### Implementation
### Tests

## Persistent Hash Table
### API
Extendible hash tables can grow with minimal rewriting of data inside the table.
### Implementation
### Tests

### Overflow Pages
### B+ Tree
### Multiple-write / Multiple-reader
### Distributed Commits
