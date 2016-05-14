#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"

/* Buffer Manager Interface Pool Handling */

//Info about each page frame
typedef struct PageFrame{
    struct BM_PageHandle pageHandle;
    bool isDirty;
	//bool isEmpty;
    int fixCount;
	int index;   //this index won't change during pin or unpin process.
	int clock;  //used in CLOCK strategy.
    struct PageFrame *next;
    struct PageFrame *prev;
} PageFrame;

int readNum;
int writeNum;

//init the buffer pool
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
    PageFrame *newPageFrame;
    PageFrame *head, *tail;
    int i;
    //init each page frame
    for (i = 0; i < numPages; i++) {
        newPageFrame = (PageFrame*)malloc(sizeof(PageFrame));
        newPageFrame->pageHandle.pageNum = NO_PAGE;
        newPageFrame->pageHandle.data = (char*)malloc(sizeof(char)*PAGE_SIZE);
        newPageFrame->isDirty = false;
        newPageFrame->fixCount = 0;
		newPageFrame->index = i;
		newPageFrame->clock = 0;
		//newPageFrame->isEmpty = true;
        newPageFrame->next = NULL;
        newPageFrame->prev = NULL;
        
        if (i == 0) {
            head = tail = newPageFrame;
        }
        else
        {
            head->prev = newPageFrame;
            tail->next = newPageFrame;
            tail->next->prev = tail;
            tail->next->next = NULL;
            tail = tail->next;
        }
    }
    
    //init buffer pool
    if (bm == NULL) {
        return RC_BP_NOT_EXIST;
    }
    if (pageFileName == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    if (numPages < 0) {
        return RC_BP_PAGE_NUM_ILLEGAL;
    }
    if (head == NULL) {
        return RC_BP_NOT_INIT;
    }
    
    bm->pageFile = (char*)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->mgmtData = head;
    
	readNum = 0;
    writeNum = 0;
	
    return RC_OK;
}

//shut down the buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    if (bm == NULL) {
        return RC_BP_NOT_EXIST;
    }
    
    PageFrame *pageFrame, *nextFrame;
    pageFrame = bm->mgmtData;
    
    while (pageFrame != NULL) {
        if (pageFrame->isDirty == true && pageFrame->fixCount == 0) {
            int result = forcePage(bm, &pageFrame->pageHandle);
            if (result != RC_OK) {
                return RC_BP_WRITE_BACK_FAILED;
            }
        }
        
        free(pageFrame -> pageHandle.data);   
        nextFrame = pageFrame -> next;

        free(pageFrame);                    
        pageFrame = nextFrame;
    }
    
    return RC_OK;
}

//write back dirty pages to sidk
RC forceFlushPool(BM_BufferPool *const bm)
{
	
    if (bm == NULL) {
        return RC_BP_NOT_EXIST;
    }
 
    PageFrame *pageFrame = bm->mgmtData;
    
    while (pageFrame != NULL) {
        if ((pageFrame->isDirty == true) && (pageFrame->fixCount == 0)) 
		{
            int result = forcePage(bm, &(pageFrame->pageHandle));
			pageFrame -> isDirty = false;
            if (result != RC_OK) {
                return RC_BP_WRITE_BACK_FAILED;
            }
        }
        
        pageFrame = pageFrame->next;
    }
    
    return RC_OK;
}

/* Buffer Manager Interface Access Pages */

//marks a page as dirty
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	
    if (bm == NULL) {
        return RC_BP_NOT_EXIST;
    }
    if (page == NULL) {
        return RC_BP_PAGE_NOT_EXIST;
    }

    PageFrame *pageFrame = bm->mgmtData;
    
    while (pageFrame != NULL) {
        if (pageFrame->pageHandle.pageNum == page->pageNum) {
            pageFrame->isDirty = true;
            break;
        }
        else
        {
            pageFrame = pageFrame -> next;
        }
    }
    
    return RC_OK;
}

//unpin a page
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{

    if (bm == NULL) {
        return RC_BP_NOT_EXIST;
    }
    if (page == NULL) {
        return RC_BP_PAGE_NOT_EXIST;
    }

    PageFrame *pageFrame = bm->mgmtData;
    
    while (pageFrame != NULL) {
        if (pageFrame->pageHandle.pageNum == page->pageNum) {
            pageFrame->fixCount = 0;
			pageFrame -> clock = 0;
            return RC_OK;
        }
        else
        {
            pageFrame = pageFrame->next;
        }
    }
    
    return RC_OK;
}

//write current page back to disk
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{

    if (bm == NULL) {
        return RC_BP_NOT_EXIST;
    }
    if (page == NULL) {
        return RC_BP_PAGE_NOT_EXIST;
    }

    SM_FileHandle fh;
    
    int result = openPageFile(bm->pageFile, &fh);
    if (result != RC_OK) {
        return RC_OPEN_FAILED;
    }
    result = writeBlock(page->pageNum, &fh, page->data);
    if (result != 0) {
		closePageFile(&fh);
        return RC_WRITE_FAILED;
    }
    
   
    writeNum += 1;
    closePageFile(&fh);
    return RC_OK;
}

/* new added functions for pinning a page */

//LRU, update the buffer pool
RC sortFrame(BM_BufferPool *const bm, struct PageFrame *selectedFrame)
{
    //LRU, already in buffer pool but need to be changed to tail
    PageFrame *Head = bm->mgmtData;
    
    if(selectedFrame == Head->prev)
        return RC_OK;       //at tail, no need to update
    if(selectedFrame == Head) {
        bm->mgmtData = Head->next;
        Head->next = NULL;
        Head->prev->next = Head;
    }
    else
    {
        //move to tail
        selectedFrame->prev->next = selectedFrame->next;
        selectedFrame->next->prev = selectedFrame->prev;
        Head->prev->next = selectedFrame;
        selectedFrame->prev = Head->prev;
        selectedFrame->next = NULL;
        Head->prev = selectedFrame;
    }
    return RC_OK;
}

//check if page is already in buffer pool
RC checkCachedPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    PageFrame *pageFrame = bm->mgmtData;
	
	while(pageFrame != NULL)
	{
		if(pageFrame -> pageHandle.pageNum == NO_PAGE)
		{
			pageFrame = pageFrame -> next;
			continue;
		}
		else if(pageNum == pageFrame->pageHandle.pageNum)
		{
			page->pageNum = pageNum;
            pageFrame->fixCount = 1;
            page->data = pageFrame->pageHandle.data;
            if(bm->strategy == RS_LRU)		//LRU need to change, FIFO no need
                sortFrame(bm, pageFrame);
			if(bm->strategy == RS_CLOCK)
				pageFrame -> clock = 1;
            return RC_OK;
		}
        else 
            pageFrame = pageFrame -> next;			
	}
    
    if ((NULL == pageFrame) || (NULL == page)) {
        return RC_BP_FRAME_NOT_FOUND;
    }
    
    return RC_OK;
}

//find an empty frame to load page
PageFrame* findEmpFrame(PageFrame *head)
{
    PageFrame *pageFrame = head;
    
    while(pageFrame != NULL) {
        if(pageFrame->pageHandle.pageNum == NO_PAGE){
            return pageFrame;
        }
        else
            pageFrame = pageFrame->next;
    }
    
   
    return NULL;
}

//find a suitable frame to pin the page
PageFrame* findSuitableFrame(PageFrame *head)
{
    PageFrame *pageFrame = head;
    
    while(pageFrame != NULL) {
        if(pageFrame->fixCount == 0) {
            return pageFrame;
        }
        else
            pageFrame = pageFrame->next;
    }
    
 
    return NULL;
}

//insert the page to the end of the bufferpool
RC addPageToTail(BM_BufferPool *const bm, struct PageFrame *pageFrame)
{
    PageFrame *head = bm->mgmtData;
    
    if(pageFrame == head->prev)     //already at tail
        return RC_OK;
    if(pageFrame == head) {
        bm->mgmtData = head->next;
        head->next = NULL;
        head->prev->next = head;
    }
    else {
        //move to tail
        pageFrame->prev->next = pageFrame->next;
        pageFrame->next->prev = pageFrame->prev;
        
        head->prev->next = pageFrame;
        pageFrame->prev = head->prev;
        pageFrame->next = NULL;
        head->prev = pageFrame;
    }
    return RC_OK;
}

//strategy FIFO and LRU
RC FIFO(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    PageFrame *head = bm->mgmtData;
    SM_FileHandle fh;
    
    int result = openPageFile(bm->pageFile, &fh);
    if (result != RC_OK){
        return RC_OPEN_FAILED;
    }
    
    //find if there is an empty frame in the buffer pool
    PageFrame *pageFrame = findEmpFrame(head);
    
    //if no empty frame, find a suitable frame
    if (pageFrame == NULL) {
        pageFrame = findSuitableFrame(head);
    }
    
    // Do not have suitable frame in memory
    if (pageFrame == NULL) {
        return RC_BP_NO_AVAILABLE_FRAME;
    }
    
	if (fh.totalNumPages < pageNum +1) {
        int ret = ensureCapacity(pageNum +1, &fh);
        writeNum += pageNum +1 - fh.totalNumPages;
        if (RC_OK != ret) {
            closePageFile(&fh);
            return RC_BP_NO_AVAILABLE_FRAME;
        }
    }
	
    //Write page to disk if dirty
    if(pageFrame->isDirty == true) {
       // ensureCapacity(pageNum +1, &fh);
        writeBlock(pageFrame->pageHandle.pageNum, &fh, pageFrame->pageHandle.data);
        pageFrame -> isDirty = false;
        writeNum++;
    }
    
    readBlock(pageNum, &fh, pageFrame->pageHandle.data);
    readNum++;
    
    page->pageNum = pageNum;
    page->data = pageFrame->pageHandle.data;
	
    pageFrame->fixCount = 1;
    pageFrame->pageHandle.pageNum = pageNum;
    
    //do FIFO and LRU
    sortFrame(bm, pageFrame);
    
    closePageFile(&fh);
    
    return RC_OK;
}

//pin a page
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    //check page is in the memory or not
    int result = checkCachedPage(bm, page, pageNum);
    
    if (result == RC_OK)        //already in memory, updated
        return RC_OK;
    
    //not in memory, read from disk
    switch(bm->strategy) {
        case RS_FIFO:
            FIFO(bm, page, pageNum);
            break;
        case RS_LRU:
            FIFO(bm, page, pageNum);
            break;
        case RS_CLOCK:
            CLOCK(bm, page, pageNum);
            break;
        case RS_LFU:
            return RC_OK;
            break;
        case RS_LRU_K:
            return RC_OK;
            break;
    }
    
    return RC_OK;
}

PageFrame* findSuitableClock(PageFrame *head)
{
	PageFrame *pageFrame = head;
	while(pageFrame != NULL) {
        if((pageFrame->fixCount == 0) && (pageFrame -> clock == 0)) {
            pageFrame -> clock = 1;
			return pageFrame;
        }
        else
		{
			pageFrame -> clock = 0;
			pageFrame = pageFrame->next;
			
		}
    }
    
    //no sutable frame available
    return NULL;
}
// CLOCK strategy, only add one more judement than FIFO, when find suitable frame,
// this frame should have ->clock = 0. and in the searching process, set the frame
// which have been scanned to  -> clock = 0. And ,once picked one frame, should marck
// the ->clock be 1. the new node also should insert to the tail. like FIFO.
RC CLOCK (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	PageFrame *head = bm -> mgmtData;
	SM_FileHandle fh;
	
	int result = openPageFile(bm -> pageFile, &fh);
	if (result != RC_OK){
        return RC_OPEN_FAILED;
    }
	
	//find if there is an empty frame in the buffer pool
    PageFrame *pageFrame = findEmpFrame(head);
	
	//if no empty frame, find a suitable frame
    if (pageFrame == NULL) {
        pageFrame = findSuitableClock(head);
    }
	
	// Do not have suitable frame in memory
    if (pageFrame == NULL) {
        return RC_BP_NO_AVAILABLE_FRAME;
    }
    
	if (fh.totalNumPages < pageNum +1) {
        int ret = ensureCapacity(pageNum +1, &fh);
        writeNum += pageNum +1 - fh.totalNumPages;
        if (RC_OK != ret) {
            closePageFile(&fh);
            return RC_BP_NO_AVAILABLE_FRAME;
        }
    }
	
	//Write page to disk if dirty
    if(pageFrame->isDirty == true) {
       // ensureCapacity(pageNum +1, &fh);
        writeBlock(pageFrame->pageHandle.pageNum, &fh, pageFrame->pageHandle.data);
        pageFrame -> isDirty = false;
        writeNum++;
    }
    
    readBlock(pageNum, &fh, pageFrame->pageHandle.data);
    readNum++;
    
    page->pageNum = pageNum;
    page->data = pageFrame->pageHandle.data;
	
    pageFrame->fixCount = 1;
	pageFrame -> clock = 1;
    pageFrame->pageHandle.pageNum = pageNum;
    
    //do FIFO and LRU and CLOCK.
    sortFrame(bm, pageFrame);
    
    closePageFile(&fh);
    
    return RC_OK;
	
}	


/* Statistics Interface */

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    
    PageNumber *pn;
    int i,j;
    pn = (PageNumber *)malloc(bm->numPages*sizeof(PageNumber));
    
    PageFrame *frames;
    frames = (PageFrame*)bm->mgmtData;
    
	for (i = 0; i < bm->numPages; i++) {
        j = frames -> index;
        if(frames -> pageHandle.pageNum == NO_PAGE)
            pn[j] = NO_PAGE;
        else
            pn[j] = frames -> pageHandle.pageNum;
        frames = frames -> next;
    }
    return pn;
}

/*returns an array of bools (of size numPages) where the ith element is TRUE if the page stored in the ith page frame is dirty.
 Empty page frames are considered as clean.*/
bool *getDirtyFlags (BM_BufferPool *const bm)
{
    bool *dflag;
    int i,j;
    dflag = (bool *)malloc(bm->numPages*sizeof(bool));
    
    PageFrame *frames;
    frames = (PageFrame*)bm->mgmtData;
    
    for (i = 0; i < bm->numPages; i++) {
        j = frames -> index;
        dflag[j] = frames -> isDirty;
        frames = frames -> next;
    }
    
    return dflag;
}

/*returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame.
 Return 0 for empty page frames.*/
int *getFixCounts (BM_BufferPool *const bm)
{
    int *fixCounts;
    int i,j;
    fixCounts = (int *)malloc(bm->numPages*sizeof(int));
    
    PageFrame *frames;
    frames = (PageFrame*)bm->mgmtData;
    
    for (i = 0; i < bm -> numPages; i++) {
        j = frames -> index;
        if(NO_PAGE == frames -> pageHandle.pageNum)
            fixCounts[j] = 0;
        else
            fixCounts[j] = frames -> fixCount;
        frames = frames -> next;
    }
    
    return fixCounts;
}

/* returns the number of pages that have been read from disk since a buffer pool has been initialized.
 You code is responsible to initializing this statistic at pool creating time and update whenever a page is read from the page file into a page frame.*/
int getNumReadIO (BM_BufferPool *const bm)
{
    
    return readNum;
}

/*returns the number of pages written to the page file since the buffer pool has been initialized.*/
int getNumWriteIO (BM_BufferPool *const bm)
{
    
    return writeNum;
}