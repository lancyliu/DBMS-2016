/*********************************************************/
/*                                                       */
/*   CS 525 Programming Assignment #3 : Record Manager   */
/*                                                       */
/*********************************************************/

Introduction:
—————————————
The goal of this assignment is to create a record manager. The record manager handles tables with a fixed schema. Clients can insert records, delete records, update records, and scan through the records in a table. A scan is associated with a search condition and only returns records that match the search condition. Each table should be stored in a separate page file and the record manager should access the pages of the file through the buffer manager implemented in the last assignment.

===============================================================================

Group Members:
——————————————
Wenqing Ye           A20351251
Xin Liu              A20353208
Hua Yang             A20358049
Rui Zou              A20351034

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
storage_mgr.c
record_mgr.h
record_mgr.c
rm_serializer.c
test_assign3_1.c
test_helper.h
test_expr.c
Makefile
README.txt

===============================================================================

Installation Instruction:
—————————————————————————
Use the following commands to run the program:
1. make
2. ./test_assign3
  ./test_expr
3. make clean

===============================================================================

Description:
————————————
New Created Structs and Functions:
——————————————————————————————————
We created some new structs and functions.

1. struct RM_TableHeader
This struct is to store information about the whole table in the page file. The information are stored in the first page at the beginning of the page. The first page does not include any records. tableName stores name of the table(page file). numRecs stores the total number of records in the table. numPages stores the total number of pages in the page file. numSlots stores the total number of slots in the table, including the free spaces. recsPerPage stores number of slots in each page, the number of slots in each page are the same. numAttrs, key and keySize are parameters of the schema struct.

2. struct RM_BlockHeader
This struct stores information about each page in the page file(table). blockId is the page number. numRecords stores the number of records in each page. numSlots stores the number of slots in each page, which is the maximum number of records each page can have.

3. struct Scan_info
This is a scan handle used for the scan operations. numPages stores the total number of pages in the page file. numSlots stores the number of slots in each page.recordLen stores length of each record. pageId is the page number.slotId is the slot ID number.

4. function initTable
This function is used when we create a new table. We need to store tableHeader information and schema into the table when it is first created. These information are stored in the first page of the page file.
This function first pins a new page and create a table header and initialize the table header. Then it writes the table header to the new page. Then it writes other schema information right after the table header. Finally write back the page to disk.

5. function createNewPage
This function is used to create a new page in the page file to store records when there is no free space to insert a new record.
It first pins a new page and then initialize the block header and write the information at the beginning of the page. Finally write back the page to disk.


Other Functions:
————————————————
1. initRecordManager
Initialize the record manager

2. shutdownRecordManager
Shut down the record manager

3. createTable
Create a page file to store the table and write initial information to the table(page file).

4. openTable
Open the table using *name and retrieve information from page to store in *rel to handle the records in other functions.

5. closeTable
Write back information to page from *rel, then write the page back to disk.

6. deleteTable
Destroy the page file that stores the table.

7. getNumTuples
Get the number of tuples in the table.

8. insertRecord
Insert a new record into the table. If there is no free space for the insertion, add a new page.

9. deleteRecord
Delete a record from the table.

10. updateRecord
Update value of an existing record.

11. getRecord
Get value of an existing record to *record.

12. startScan
Start to scan the table(rel) with the required condition(cond), the information about the scan process stored in RM_ScanHandle *scan.

13. next
Get one records that fulfill the scan requirement. store the information about scan process in RM_ScanHandle *scan.

14. closeScan
Close the scan process.

15. getRecordSize
Get the size of one record.

16. *createSchema
Create a new schema using the data passed to this function.

17. freeSchema
Free the space of the schema.

18. createRecord
Create a new record.

19. freeRecord
Clean the space occupied by the record.

20. getAttr
Get an attribute.

21. setAttr
Set an attribute.

===============================================================================

Additional Error Codes:
———————————————————————
RC_RM_PINPAGE_FAILED 14
failed to pin a page 

RC_RM_MARKDIRTY_FAILED 15
failed to mark dirty

RC_RM_UNPIN_FAILED 16
failed to unpin

RC_RM_CREATE_PAGE_FAILED 17
failed to create a new page

RC_RM_INIT_BUFFER_POOL_FAILED 18
failed to initialize the buffer pool

RC_RM_INIT_TABLE_FAILED 19
failed to initialize the table

RC_RM_SHUTDOWN_BUFFER_POOL_FAILED 20
failed to shut down the buffer pool

RC_RM_FORCE_PAGE_FAILED 21
failed to force page

RC_RM_TABLE_NOT_EXIST 22
the table does not exist

RC_RM_DESTROY_PAGE_FAILED 23
failed to destroy page

RC_RM_PAGE_NOT_EXIST 24
the page does not exist

RC_RM_SLOT_NOT_EXIST 25
the slot does not exist

RC_RM_SCHEMA_NOT_EXIST 26
the schema does not exist

RC_RM_SCHEMA_INIT_FAIL 27
failed to initialize the schema

RC_RM_TABLE_NOT_OPEN 28
the table was not opened

===============================================================================

Summary:
————————
1. In this project, we implemented all functions required and passed all test cases listed in the test file. 
2. The coding style is clean and consistent.