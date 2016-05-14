#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"


/*** new defined structs and functions ***/

//records info about pages, records of the whole table in the page file
//first page of the page file
typedef struct RM_TableHeader
{
    char tableName[30];
    int numRecs;        //total num of records in the table
    int numPages;       //total num of pages of the table
    int numSlots;       //total num of slots, including free space
    int recsPerPage;        //num of records per page
    int numAttrs;
    int key;
    int keySize;
}RM_TableHeader;


//for each page
//records page num and free slots
typedef struct RM_BlockHeader
{
    int blockID;        //page num
    int numRecords;     //num of records in this page
    int numSlots;       //total num of records can have in one page
}RM_BlockHeader;


//data struct used in scan process, stores scan information. 
// scanHandle -> mgmtData = Scan_info. 
typedef struct Scan_info
{
    int numPages;   //total number of pages
    int numSlot;   //number of slot in this page, not total page
    int recordLen; //length of one recordLen
    int pageID;  // current page number;
    int slotID; //current slot ID;
    Expr *cond;  // expression condit 
}Scan_info;


//init table
//write basic info of the table to the first page of the page file
//tableHeader, schema
RC initTable(char *name, Schema *schema, BM_BufferPool *bm)
{
    RM_TableHeader tableHeader;
    
    //page to store table header info
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    //header info stores in the first page
    int result;
    result = (pinPage(bm, page, 0));
    if (result != RC_OK) {
        return RC_RM_PINPAGE_FAILED;
    }
    
    int recordSize = getRecordSize(schema);

	//init RM_TableHeader
    strcpy(tableHeader.tableName, name);
    tableHeader.numRecs = 0;
    tableHeader.numPages = 1;       //only first page store header info
    tableHeader.recsPerPage = (PAGE_SIZE - sizeof(RM_BlockHeader)) / recordSize;        //blockHeader at front of each page, then records
    tableHeader.numAttrs = schema->numAttr;
    tableHeader.key = *schema->keyAttrs;
    tableHeader.numSlots = 0;
    tableHeader.keySize = schema->keySize;
    

    //write table header to the page, first page
    memcpy(page->data, &tableHeader, sizeof(RM_TableHeader));
    
    //then write schema to the page
    int offset = 0;
    int size = 0;
    
    //attrNames
    offset = sizeof(RM_TableHeader);
    size = sizeof(char);
	int i;
    for (i = 0; i < schema->numAttr; i++) {
        memcpy((char*)page->data + offset, *(schema->attrNames + i), size);
        offset += size;
    }
    
    //dataTypes
    /*
    size = sizeof(int);
    for (int i = 0; i < schema->numAttr; i++) {
        memcpy((char*)page->data + offset, schema->dataTypes + i, size);
        offset += size;
    }
     */
    size = sizeof(int) * schema->numAttr;
    memcpy((char*)page->data + offset, schema->dataTypes, size);
    offset += size;
    
    //typeLength
    size = sizeof(int) * schema->numAttr;
    memcpy((char*)page->data + offset, schema->typeLength, size);
    offset += size;
    
    result = (markDirty(bm, page));
    if (result != RC_OK) {
        return RC_RM_MARKDIRTY_FAILED;
    }
    result = (unpinPage(bm, page));
    if (result != RC_OK) {
        return RC_RM_UNPIN_FAILED;
    }
    free(page);
    
    return RC_OK;
}


//create a new page to store records
RC createNewPage(RM_TableData *rel, int pageNum)
{
	
    BM_BufferPool *bm = rel->mgmtData;
    BM_PageHandle *page = (BM_PageHandle*)malloc(sizeof(BM_PageHandle));
    RM_TableHeader *tableHeader = rel->tableHeader;
   
    int result;
    result = (pinPage(bm, page, pageNum));
    if (result != RC_OK) {
        return RC_RM_PINPAGE_FAILED;
    }
   
    //block header at the front of the page
    RM_BlockHeader blockHeader ;
    blockHeader.blockID = pageNum;
    blockHeader.numRecords = 0;
	
	//caculate the total number of slots could have in one page.
	int slotsize = PAGE_SIZE - sizeof(RM_BlockHeader);
	int recordsize = getRecordSize(rel -> schema);	
	int slots = slotsize / recordsize ;
    blockHeader.numSlots = slots;
	
	//(PAGE_SIZE - sizeof(RM_BlockHeader)) / (getRecordSize(rel->schema));

    //update tableHeader
    tableHeader->numPages += 1;
  //  tableHeader->numSlots += getRecordSize(rel->schema);
    
    //write to the page
    memcpy(page->data, &blockHeader, sizeof(RM_BlockHeader));
    

    result = (markDirty(rel->mgmtData, page));
    if (result != RC_OK) {
        return RC_RM_MARKDIRTY_FAILED;
    }
    result = (forcePage(rel->mgmtData, page));
    if (result != RC_OK) {
        return RC_RM_FORCE_PAGE_FAILED;
    }
    result = (unpinPage(rel->mgmtData, page));
    if (result != RC_OK) {
        return RC_RM_UNPIN_FAILED;
    }
    
    free(page);

    return RC_OK;
}



/*** table and manager ***/

//init record manager
extern RC initRecordManager (void *mgmtData)
{
    return RC_OK;
}


//shut down record manager
extern RC shutdownRecordManager ()
{
    return RC_OK;
}


//create table
extern RC createTable (char *name, Schema *schema)
{
    BM_BufferPool *bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
    
    //create a table should first create a page file
    int result;
    result = (createPageFile(name));
    if (result != RC_OK) {
        return RC_RM_CREATE_PAGE_FAILED;
    }
    
    //init buffer pool
    result = (initBufferPool(bm, name, 3, RS_FIFO, NULL));
    if (result != RC_OK) {
        return RC_RM_INIT_BUFFER_POOL_FAILED;
    }
    
    //init the table
    result = (initTable(name, schema, bm));
    if (result != RC_OK) {
        return RC_RM_INIT_TABLE_FAILED;
    }
    
    result = (shutdownBufferPool(bm));
    if (result != RC_OK) {
        return RC_RM_SHUTDOWN_BUFFER_POOL_FAILED;
    }
    
    return RC_OK;
    
}


//open table
extern RC openTable (RM_TableData *rel, char *name)
{
	// data needed to store the information about a table
    BM_BufferPool *bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    Schema *schema = (Schema*)malloc(sizeof(Schema));
    
    int result;  //flag: the result of function.
    result = (initBufferPool(bm, name, 3, RS_FIFO, NULL));
    if (result != RC_OK) {
        return RC_RM_INIT_BUFFER_POOL_FAILED;
    }
    bm->pageFile = name;
    result = (pinPage(bm, page, 0));
    if (result != RC_OK) {
        return RC_RM_PINPAGE_FAILED;
    }
    
    RM_TableHeader *tableHeader;
    tableHeader = (RM_TableHeader*)malloc(sizeof(RM_TableHeader));
    
    //from page data to schema
    //tableHeader info stored in page when creating table
    memcpy(tableHeader, page -> data, sizeof(RM_TableHeader));
	
   // printf("\n%d", tableHeader -> numRecs);
    //from page data to schema
    int numAttr = tableHeader->numAttrs;
    char **attrNames = (char**) malloc(sizeof(char*) * tableHeader->numAttrs);
	
    DataType *dataType = (DataType*)malloc(sizeof(DataType) * tableHeader->numAttrs);
    int *typeLength = (int*) malloc(sizeof(int) * tableHeader->numAttrs);
    //int *keyAttrs = (int*) malloc(sizeof(int));
    int *keyAttrs = &tableHeader->key;
    int keySize = tableHeader->keySize;
    
    //copy each
    int offset = 0;
    int size = 0;

    //attrNames
	int i;
	for(i = 0; i < numAttr; i++)
	{
		attrNames[i] = (char*)malloc(sizeof(char));
	}
	offset = sizeof(RM_TableHeader);
    size = sizeof(char);
	for (i = 0; i < numAttr; i++) {
		//size = sizeof(attrNames[i]);
        memcpy(attrNames[i], (char*)page->data + offset, size);
        offset += size;
    }
    
    size = sizeof(DataType) * numAttr;
    memcpy(dataType, page->data + offset, size);

    //typeLength
    offset += size;
    size = sizeof(int) * numAttr;
    memcpy(typeLength, page -> data + offset, size);

    //write to schema
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataType;
    schema->typeLength = typeLength;
    schema->keyAttrs = keyAttrs;
    schema->keySize = keySize;
   
    //add to tableData
    rel->name = name;
    rel->schema = schema;
    rel->mgmtData = bm;
    rel->tableHeader = tableHeader;
    free(page);
   // printf("open table success\n");
    return RC_OK;
}


//close table
extern RC closeTable (RM_TableData *rel)
{

    if (rel == NULL) {
        return RC_RM_TABLE_NOT_EXIST;
    }
    
    BM_BufferPool *bm = rel->mgmtData;
    BM_PageHandle *page = (BM_PageHandle*)malloc(sizeof(BM_PageHandle));
  //  page -> data = (char*)malloc(sizeof(RM_BlockHeader));
	
    //write table header to page
    int result;
    result = (pinPage(bm, page, 0));
    if (result != RC_OK) {
        return RC_RM_PINPAGE_FAILED;
    }
    memcpy(page->data, rel->tableHeader, sizeof(RM_TableHeader));
    
    result = (markDirty(bm, page));
    if (result != RC_OK) {
        return RC_RM_MARKDIRTY_FAILED;
    }
    result = (forcePage(bm, page));
    if (result != RC_OK) {
        return RC_RM_FORCE_PAGE_FAILED;
    }
    
	//free(page);
	if(rel -> schema != NULL) rel -> schema = NULL;
  
	if(rel -> tableHeader != NULL) rel -> tableHeader = NULL;
 
	result = (unpinPage(bm, page));
	
    if (result != RC_OK) {
        return RC_RM_UNPIN_FAILED;
    }
	
    if(page != NULL) 
	{
		//free(page -> data);
		free(page);
	}
	//shutdownBufferPool(bm);
	//free(bm);
	//free(rel);
    return RC_OK;
}


//delete table
extern RC deleteTable (char *name)
{
    int result;
    result = (destroyPageFile(name));
    if (result != RC_OK) {
        return RC_RM_DESTROY_PAGE_FAILED;
    }
    
    return RC_OK;
}


//get number of tuples
extern int getNumTuples (RM_TableData *rel)
{
    if (rel == NULL) {
        return RC_RM_TABLE_NOT_EXIST;
    }
    
    RM_TableHeader *tableHeader;
    tableHeader = rel->tableHeader;
    
    //return tableHeader->numRecs;
    return tableHeader->numSlots;       //free slots are null values, but is a tuple
}



/*** handling records in a table ***/
extern RC insertRecord (RM_TableData *rel, Record *record)
{
	//printf("test insert record flag\n");
    if (rel == NULL) {
        return RC_RM_TABLE_NOT_EXIST;
    }
    
    BM_BufferPool *bm = rel->mgmtData;
    BM_PageHandle *page = (BM_PageHandle*)malloc(sizeof(BM_PageHandle));
    RM_TableHeader *tableHeader = rel->tableHeader;
    RM_BlockHeader blockHeader;
   // printf("test insert record begin 5\n");
    int result;
    //if no free space to insert

    if (tableHeader->numRecs == tableHeader->numSlots) {
      // printf("add a new page. total page number %d \n",tableHeader -> numPages);
	   createNewPage(rel, tableHeader->numPages);
        //tableHeader->numPages += 1;
        tableHeader->numSlots += tableHeader->recsPerPage;
		//printf("add a new page. total page number %d \n",tableHeader -> numSlots);
		//printf("add a new page. total page number %d \n",tableHeader -> numPages);
    } /*else {
        //if have free space
        result = (pinPage(bm, page, tableHeader->numPages - 1));
        if (result != RC_OK) {
            return RC_RM_PINPAGE_FAILED;
        }
    }*/
    
    /*
    //find the page with free slots
    int availablePage;
    for (int i = 0; i < tableHeader->numPages; i++) {
        blockHeader.blockID = i;
        if (blockHeader.numRecords < blockHeader.numSlots) {
            availablePage = blockHeader.blockID;
            break;
        }
    }
     */

    result = (pinPage(bm, page, tableHeader->numPages - 1));
    if (result != RC_OK) {
        return RC_RM_PINPAGE_FAILED;
    }
    
    //write to the page, insert record
    memcpy(&blockHeader, page->data, sizeof(RM_BlockHeader));
    int offset;
    //int size;
    int recordSize = getRecordSize(rel->schema);

    offset = sizeof(RM_BlockHeader) + blockHeader.numRecords * recordSize;
    memcpy(page->data + offset, record->data, recordSize);

    //update record info
    record->id.page = tableHeader->numPages - 1;
    record->id.slot = blockHeader.numRecords;
    
    //update block info
    blockHeader.numRecords += 1;
    //update table info
    tableHeader->numRecs += 1;
    //update page
    memcpy(page->data, &blockHeader, sizeof(RM_BlockHeader));


    result = (markDirty(bm, page));
    if (result != RC_OK) {
        return RC_RM_MARKDIRTY_FAILED;
    }
    result = (forcePage(bm, page));
    if (result != RC_OK) {
        return RC_RM_FORCE_PAGE_FAILED;
    }
    result = (unpinPage(bm, page));
    if (result != RC_OK) {
        return RC_RM_UNPIN_FAILED;
    }
    
    free(page);
  //  printf("test create record done\n");
    return RC_OK;
}


//delete record
extern RC deleteRecord (RM_TableData *rel, RID id)
{
    
    if (rel == NULL) {
        return RC_RM_TABLE_NOT_EXIST;
    }
    
    BM_BufferPool *bm = rel->mgmtData;
    BM_PageHandle *page = (BM_PageHandle*)malloc(sizeof(BM_PageHandle));
    RM_TableHeader *tableHeader = rel->tableHeader;
    RM_BlockHeader blockHeader;
    
    if (id.page > tableHeader->numPages - 1) {
        return RC_BP_PAGE_NOT_EXIST;
    }
    
    int result;
    result = (pinPage(bm, page, id.page));
    if (result != RC_OK) {
        return RC_RM_PINPAGE_FAILED;
    }
    
    //update block header
    memcpy(&blockHeader, page->data, sizeof(RM_BlockHeader));
    
    if (id.slot > blockHeader.numSlots) {
        return RC_RM_SLOT_NOT_EXIST;
    }
    
    //update blockHeader
    blockHeader.numRecords -= 1;
    //update tableHeader
    tableHeader->numRecs -= 1;
    
    memcpy(page->data, &blockHeader, sizeof(RM_BlockHeader));
    
    int offset;
    int recordSize = getRecordSize(rel->schema);
    offset = sizeof(RM_BlockHeader) + id.slot * recordSize;
    memset(page->data + offset, 0, recordSize);
    
    result = (markDirty(bm, page));
    if (result != RC_OK) {
        return RC_RM_MARKDIRTY_FAILED;
    }
    result = (forcePage(bm, page));
    if (result != RC_OK) {
        return RC_RM_FORCE_PAGE_FAILED;
    }
    result = (unpinPage(bm, page));
    if (result != RC_OK) {
        return RC_RM_UNPIN_FAILED;
    }
    
    free(page);
    
    return RC_OK;
}


//update record
extern RC updateRecord (RM_TableData *rel, Record *record)
{
 
    if (rel == NULL) {
        return RC_RM_TABLE_NOT_EXIST;
    }
    
    BM_BufferPool *bm = rel->mgmtData;
    BM_PageHandle *page = (BM_PageHandle*)malloc(sizeof(BM_PageHandle));
    RM_TableHeader *tableHeader = rel->tableHeader;
    RM_BlockHeader blockHeader;
    
    if (record->id.page > tableHeader->numPages - 1) {
        return RC_BP_PAGE_NOT_EXIST;
    }
    
    int result;
    result = (pinPage(bm, page, record->id.page));
    if (result != RC_OK) {
        return RC_RM_PINPAGE_FAILED;
    }
    
    memcpy(&blockHeader, page->data, sizeof(blockHeader));
    
    if (record->id.slot > blockHeader.numSlots) {
        return RC_RM_SLOT_NOT_EXIST;
    }
    
    //update record
    int offset;
    int recordSize;
    recordSize = getRecordSize(rel->schema);
    offset = sizeof(RM_BlockHeader) + record->id.slot * recordSize;
    memcpy(page->data + offset, record->data, recordSize);
    
    result = (markDirty(bm, page));
    if (result != RC_OK) {
        return RC_RM_MARKDIRTY_FAILED;
    }
    result = (forcePage(bm, page));
    if (result != RC_OK) {
        return RC_RM_FORCE_PAGE_FAILED;
    }
    result = (unpinPage(bm, page));
    if (result != RC_OK) {
        return RC_RM_UNPIN_FAILED;
    }
    
    free(page);
    
    return RC_OK;
}


//get record
extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	
    if (rel == NULL) {
        return RC_RM_TABLE_NOT_EXIST;
    }
    
    BM_BufferPool *bm = rel->mgmtData;
    BM_PageHandle *page = (BM_PageHandle*)malloc(sizeof(BM_PageHandle));
    RM_TableHeader *tableHeader = rel->tableHeader;
    RM_BlockHeader blockHeader;
    
    record->id = id;
    
    if (record->id.page > tableHeader->numPages - 1) {
        return RC_BP_PAGE_NOT_EXIST;
    }
    
    int result;
    result = (pinPage(bm, page, id.page));
    if (result != RC_OK) {
        return RC_RM_PINPAGE_FAILED;
    }
    memcpy(&blockHeader, page->data, sizeof(blockHeader));
    
    if (record->id.slot > blockHeader.numSlots) {
        return RC_RM_SLOT_NOT_EXIST;
    }
    //get record
    //char *getRecord;
    int recordSize = getRecordSize(rel->schema);
    //getRecord = malloc(recordSize);
    int offset;
    offset = sizeof(RM_BlockHeader) + id.slot * recordSize;
    memcpy(record->data, page->data + offset, recordSize);
    //record->id = id;
   
    result = (unpinPage(bm, page));
    if (result != RC_OK) {
        return RC_RM_UNPIN_FAILED;
    }
    
    free(page);
    
    return RC_OK;
}



/*************** scans ***************/
//start to scan the table(RM_TableData *rel) 
//with the required conditon(Expr *cond), 
//the information about the scan process stored in RM_ScanHandle *scan.  

extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    if(rel == NULL) return RC_RM_TABLE_NOT_OPEN ;
    // assign the value of the RM_ScanHandle
    scan -> rel = rel;

    //open file and  buffer pool.
    // when find the wanted record, we should use pin and unpin method
    // in buffer pool. bufferpool should be make when open the table file
    // when startScan, get ready to the bufferpool.
	
    BM_BufferPool *bm = rel -> mgmtData; // 
	
	//open the table file, to get total number of pages in thie file.
    SM_FileHandle fh;
    int result = openPageFile(bm->pageFile, &fh);
    if (result != RC_OK){
        return RC_OPEN_FAILED;
    }
    //inital the information of scan.
    Scan_info *scanInfo = (Scan_info*)malloc(sizeof(Scan_info));
    scanInfo -> numPages = fh.totalNumPages;
    scanInfo -> numSlot = 0;  //number of records in this page
    scanInfo -> recordLen = getRecordSize(rel -> schema);
    scanInfo -> pageID = 1;  //page ID starts from 1, we don't scan the first page which store the table header infromation.
    scanInfo -> slotID = 0;
    scanInfo -> cond = cond;
    
    scan -> mgmtData = (void*)scanInfo;
	
    return RC_OK;
}


//geting one records that fulfill the scan requirement. 

/*store the information about scan process in RM_ScanHandle *scan,
 in each page, caculate the total number of slot, scan each slot ,check match.
 after we scan all the slot in this page, page number ++, scan the next page.
*/
/* we only find one record that match the condition in one call for this method.
 onece we found a record that matches, return RC_OK.
 meanwhile, store the position of the scan process.
 thus, next time we call this function, start from the following position, not from beginning.
*/
extern RC next (RM_ScanHandle *scan, Record *record)
{
    //there are two kinds of operation: equal, smaller.
    Scan_info *scanInfo = (Scan_info*)malloc(sizeof(Scan_info));
    scanInfo = (Scan_info*)scan -> mgmtData;
    //printf("\n%d", scanInfo -> slotID);
	RM_TableData *rel = scan -> rel;
	
    //build the bufferpool, to read page from file.
    BM_BufferPool *bm = (BM_BufferPool*)rel -> mgmtData;
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE(); // store information of current page.
    RM_BlockHeader blockHeader;
    //extract information from scan, just to make it clear.
    Expr *condition;
    condition = (Expr*)scanInfo -> cond;
    //Operator* operat1;
    //operat1 = condition -> expr -> op;
    RM_TableData *table = scan -> rel;
	
    
    RID id;     // one parameter in function evalExpr(), store the id of current record.
    Value *result;  //used in evalExpr () function, store the result value.
    result = (Value*)malloc(sizeof(Value));
    int flag; // used to show that the function return the right value.
    Record *r ;
	createRecord(&r, rel -> schema);

    //while current page number smaller than the total number of page, search for the
    //record that matches.
    if(condition == NULL)
    {
		
        //return all the tuples in the table. do not need to caculate.
        while(scanInfo -> pageID < scanInfo -> numPages)
        {
            pinPage(bm, pageHandle, scanInfo -> pageID);
            //caculate the number of slots in this page, this number also can got by the blockHeader struct.
            memcpy(&blockHeader, pageHandle -> data, sizeof(RM_BlockHeader));
		   //int pageLength = strlen(pageHandle -> data);
           // //int dataLength = pageLength - sizeof(RM_BlockHeader);
			//int slots = dataLength/ (scanInfo -> recordLen);
            scanInfo -> numSlot = blockHeader.numRecords ;//= slots;
            while(scanInfo -> slotID < scanInfo -> numSlot)
            {
                id.page = scanInfo -> pageID;
                id.slot = scanInfo -> slotID;
                flag = getRecord(table, id, r);
                if(flag == RC_OK)
                {
                    record -> id = r -> id;
                    record -> data = r -> data;
                    scanInfo -> slotID += 1;
                    return RC_OK;
                }
                
            }
            unpinPage(bm, pageHandle);
            scanInfo -> slotID = 0;
            scanInfo -> pageID += 1;
        }
		scanInfo -> pageID = 1;
        return RC_RM_NO_MORE_TUPLES;
    }
    else
    {
        while((scanInfo -> pageID) < (scanInfo -> numPages))
        {
            flag = pinPage(bm, pageHandle, scanInfo -> pageID);
            if(flag != RC_OK) return RC_RM_PINPAGE_FAILED;
	
            //caculate the number of slots in this page, this number also can got by the blockHeader struct.
            memcpy(&blockHeader, pageHandle -> data, sizeof(RM_BlockHeader));
		   
            scanInfo -> numSlot = blockHeader.numRecords ; //= slots;
			
			//scan each record in one page.
            while(scanInfo -> slotID < scanInfo -> numSlot)
            {
                id.page = scanInfo -> pageID;
                id.slot = scanInfo -> slotID;
				
				flag = getRecord(table, id, r);  //get the record, and then compare this record with the conditon.
               
			    if(flag != RC_OK) return RC_RM_SLOT_NOT_EXIST;
				/*flag = */evalExpr(r, table -> schema, condition, &result);
                if(result -> v.boolV == 1 )
                {
                    //matches
                    record -> id = r -> id;
                    record -> data = r -> data;
                    scanInfo -> slotID  += 1;
					scan -> mgmtData = scanInfo;
				   return RC_OK;
                    //break; // find a match, return.
                }
                // not match, find the next slot.
                else scanInfo -> slotID += 1;
            }
            // no match in thie page, find the next page.
            flag = unpinPage(bm, pageHandle);
			if(flag != RC_OK) return RC_RM_UNPIN_FAILED;
			//pageHandle = 
            scanInfo -> pageID += 1;
            scanInfo -> slotID = 0; // slotID start from 0 again.
			if( (scanInfo -> pageID < 2) || (scanInfo -> pageID > scanInfo -> numPages) )
			{
				scan -> mgmtData = scanInfo;
				return RC_RM_PAGE_NOT_EXIST;
			}
        }
		scanInfo -> pageID = 1;
		scan -> mgmtData = scanInfo;
        return  RC_RM_NO_MORE_TUPLES;
    }
 
   // scan all the record in the table, but no matches.
    scanInfo -> pageID = 1; // scan all the pages, pageID returns to 1
   scan -> mgmtData = scanInfo;	//store the information in RM_ScanHandle
   return  RC_RM_NO_MORE_TUPLES;
    
}

//close the scan process.
extern RC closeScan (RM_ScanHandle *scan)
{
   // Scan_info *scanInfo = scan -> mgmtData;
   // RM_TableData *rel = scan -> rel;
   // free(scanInfo -> cond);
   // free(scanInfo);
   // free(rel -> schema);
    //free(rel);
   // free(scan);
  // printf("test close scan \n");
   scan -> mgmtData = NULL;
   scan -> rel = NULL;
    return RC_OK;
}



/********************** dealing with schemas *********************/

//get the size of one record.
extern int getRecordSize (Schema *schema)
{
	//printf("test get record size begin\n");
    if(schema == NULL) return RC_RM_SCHEMA_NOT_EXIST ;
    int RecordSize = 0;
    int i = 0;
	//printf("\n%d", schema -> numAttr);
    for(i; i< schema -> numAttr; i++)
    {
        switch( schema -> dataTypes[i])
        {
            case DT_INT:
                RecordSize += sizeof(int);
                break;
            case DT_FLOAT:
                RecordSize += sizeof(float);
                break;
            case DT_BOOL:
                RecordSize += sizeof(bool);
                break;
            case DT_STRING:
                RecordSize += schema -> typeLength[i];
                break;
            default:
                break;
        }
    }
	
	RecordSize += schema -> numAttr; // schema->numAttr is the length of empty space that used to separating attributes
    return RecordSize;
}


//create a new schema.
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{

    Schema *NewSchema = (Schema*)malloc(sizeof(Schema));
    NewSchema -> numAttr = numAttr;
    NewSchema -> attrNames = attrNames;
    NewSchema -> dataTypes = dataTypes;
    NewSchema -> typeLength = typeLength;
    NewSchema -> keyAttrs = keys;
    NewSchema -> keySize = keySize;


    if(NewSchema == NULL) return  NULL;//RC_RM_SCHEMA_INIT_FAIL;
    else return NewSchema;
}

//free the space used by the schema
extern RC freeSchema (Schema *schema)
{
    free(schema -> attrNames);
    free(schema);
    return RC_OK;
}



/*** dealing with records and attribute values ***/

//create a new record
RC createRecord(Record **record,Schema *schema)
{

    int recSize = getRecordSize(schema);//get the size of a record
    Record *rec = (Record*)malloc(sizeof(Record)); //create new record
    //rec -> data = (char*)calloc(recSize,sizeof(char));//allocating the record size
    rec -> data = (char*)malloc(recSize);
	memset(rec -> data, 0, recSize);  // fulfill the emp space with 0. this also should be considered when setAttr
	*record = rec;

    return RC_OK;
}


//clean the space
RC freeRecord(Record *record){
    free(record->data);
    free(record);
    return RC_OK;
}



//this method is used to get an attribute
//this method is used to get the value of one attribute in a record
//int attrNum is the the position os this attribute, and the result stores in value
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
	int offset = 0;
	int i = 0;
	//this value store the value of attribute we get in this record
	Value *val = (Value*)malloc(sizeof(Value));
//	char *result; // string we get from record. then we should convert this string to the right data type.

	//caculate the offset of this attribute in this record
	//this part is similar with the getRecordSize()
	offset += (attrNum +1); // add empty spaces to the offset.
	//add the length of attributes to offset
	for(i = 0; i < attrNum; i++)
	{
		switch( schema -> dataTypes[i])
        {
            case DT_INT:
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                offset += sizeof(float);
                break;
            case DT_BOOL:
                offset += sizeof(bool);
                break;
            case DT_STRING:
                offset += schema -> typeLength[i];
                break;
            default:
                break;
        }
	}
	//offset += record -> data;
	char* result;
	// get the value according to the dataTypes in schema
	switch(schema -> dataTypes[attrNum])
	{
		case DT_INT:
		  val -> dt = DT_INT;
		  result = (char*)malloc(sizeof(int) +1);
		  memcpy(result, record -> data + offset,sizeof(int));
		  result[sizeof(int) +1] = '\0';
		  val -> v.intV = atoi(result);
		  result = NULL;
		 // memcpy(&(val->v.intV),record->data + offset,sizeof(int));
		  break;
		case DT_FLOAT:
		  val -> dt = DT_FLOAT;	 
		  memcpy(&(val->v.floatV),record->data+ offset,sizeof(float));
		  break;
		case DT_BOOL:
		  val -> dt = DT_BOOL;
		   memcpy(&(val->v.boolV),record -> data + offset,sizeof(bool));
		  break;
		case DT_STRING:
		  val -> dt = DT_STRING;
		  int size = schema -> typeLength[i];
          char *result = malloc(size+1);
          strncpy(result, record->data+ offset, size); 
          result[size]='\0';
          val->v.stringV = result;
		  break;
		
	}
	*value = val;
	//free(val);
	return RC_OK;
	
}


//this method is used to set an attribute
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) 
{
 // printf("set attr \n");
  int i,j;  
  int offset = 0;

  //caculate the offset of this attribute
   offset += (attrNum+1);
  for(i = 0; i < attrNum; i++)
	{
		switch( schema -> dataTypes[i])
        {
            case DT_INT:
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                offset += sizeof(float);
                break;
            case DT_BOOL:
                offset += sizeof(bool);
                break;
            case DT_STRING:
                offset += schema -> typeLength[i];
                break;
            default:
                break;
        }
	}

 
  char *position = record -> data;
  position += offset;
  //write the data to the record according to the datatype
  //use sprintf, do not need to consider the gap between real and logical length of data.
  switch(value->dt)
  {
    case DT_INT:
      sprintf(position,"%d",value->v.intV);
	  break;
	  
    case DT_FLOAT:
      
      sprintf(position,"%f",value->v.floatV);     
	  break;
     
    case DT_BOOL:
	
	sprintf(position,"%i",value->v.boolV);
	break;
     
    case DT_STRING: 
	
	sprintf(position,"%s",value->v.stringV);
	break;
	
  }
  return RC_OK;
}











