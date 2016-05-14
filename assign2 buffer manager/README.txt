/*********************************************************/
/*                                                       */
/*   CS 525 Programming Assignment #2 : Buffer Manager   */
/*                                                       */
/*********************************************************/

Introduction:
—————————————
This assignment is to implement a buffer manager that manages a fixed number of pages in memory that represent pages from a page file managed by the storage manager implemented in assignment 1. The Buffer manager should be able to handle more than one open buffer pool at the same time. However, there can only be one buffer pool for each page file. Each buffer pool uses one page replacement strategy that is determined when the buffer pool is initialized. In this project, two replacement strategies, FIFO and LRU are implemented.

===============================================================================

Group Members:
——————————————
Rui Zou              A20351034
Wenqing Ye           A20351251
Xin Liu              A20353208
Hua Yang             A20358049

===============================================================================

File List:
——————————
buffer_mgr_stat.c
buffer_mgr_stat.h
buffer_mgr.h
buffer_mgr.c
dberror.c
dberror.h
dt.h
storage_mgr.h
test_assign2_1.c
test_helper.h
Makefile

===============================================================================

Installation Instruction:
—————————————————————————
Use command make in terminal to run the project using the Makefile.
All test cases in the test file test_assign2_1.c are passed.

===============================================================================

Description:
————————————
Data Structure:
———————————————
We created a new struct PageFrame to store attributes of a page frame.
1. pageNum: the number of the page in the page file.
2. data: contents of the  page.
3. isDirty: boolean data to record if the page has something to write back to the disk.
4. fixCount: based on pin and unpin requests.
5. readPage: total number of read.
6. writePage: total number of write.
7. next: the pointer points to the next address of the page frame.

Functions:
——————————
1. initiate buffer pool:
   Creates the buffer pool with only one empty page frame.
2. shutdownBufferPool:
   Closes the buffer pool. Writes back unpinned and dirty pages to the disk and free the page frame.
3. forceFlushPool:
   Writes back unpinned and dirty pages to the disk.
4. markDirty:
   Marks a page as dirty.
5. unpinPage:
   Unpins the page and fixCount-1.
6. forcePage:
   Writes current contents back to the disk.
7. initPageFrame:
   Initiates the new node of page frame.
8. pinPage:
   Pins the page and fixCount+1. The strategy used to pin the page is decided in this function.
9. getFrameContents:
   Returns an array of PageNumbers (of size numPages) where the ith element is the number of the page stored in the ith page frame.
10. getDirtyFlags:
    returns an array of bools (of size numPages) where the ith element is TRUE if the page stored in the ith page frame is dirty.
11. getFixCounts:
    returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame. 
12. getNumReadIO:
    returns the number of pages that have been read from disk since a buffer pool has been initialized. 
13. getNumWriteIO:
    returns the number of pages written to the page file since the buffer pool has been initialized.

===============================================================================

Summary:
————————
1. In this project, we implemented all functions required and passed all test cases listed in the test file. 
2. Two page replacement strategies, FIFO and LRU, are implemented.
3. The coding style is clean and consistent.