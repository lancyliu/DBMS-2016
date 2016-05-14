#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>

#include "storage_mgr.h"

/*************************************************************** File Related Methods ***************************************************************/
int fd[20];		//File Descriptor, used to record current open file(Max number say 20)
int i=0;		//The number of current open file

/* initiate storage manager, print welcome msg */
void initStorageManager(void)
{
	printf("Initialize the storage manager.\n");
}

/* Get the size of a file */
int getFileSize(FILE * fp)
{
	rewind(fp);	// set the file position to the beginning of the file fp
    fseek(fp, 0, SEEK_END);	// set the file position of fp to the given offset
    int fsize = ftell(fp);	// ftell returns the current file position of the given stream.
    rewind(fp);
    return fsize;
}

/* Create a new page file fileName. The initial file should be one page. This method should fill this single page with '\0' bytes. */
RC createPageFile(char *fileName)
{
    FILE * fp;
    fp = fopen(fileName, "w");
    if (fp == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    //Let fp point to the start of the file, and check.
    if (fseek(fp, 0, SEEK_SET) == -1)
        return RC_FILE_HANDLE_NOT_INIT;

    char fillPage[PAGE_SIZE] = { 0 };

    //printf("(printed in createPageFile for debug)PAGE_SIZE: , %d\n", PAGE_SIZE);
    //printf("(printed in createPageFile for debug)fillPage size: %d\n", sizeof(fillPage));

    //Initialize the page file with the array 0.
    if (fwrite(fillPage, sizeof(fillPage), 1, fp) != 1)
    {
        return RC_WRITE_FAILED;
    }

    //printf("(printed in createPageFile for debug)File size, %d\n", getFileSize(fp));
    
    fclose(fp);

    return RC_OK; 

}

/* Opens an existing file. Should return RC_FILE_NOT_FOUND if the file does not exist. The second parameter is an existing file handle. If opening the file is successful, then the fields of this file handle should be initialized with the information about the opened file. For instance, you would have to read the total number of pages that are stored in the file from disk. */
RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{    
	struct stat fileStat;	// system struct that is defined to store information about files;
    int pnum;	// total number of pages
	
	fd[i] = open(fileName,O_RDWR);	// open a new file and obtain its file descriptor
    if (fd[i] == -1)
        return RC_FILE_NOT_FOUND;

	if ( fstat(fd[i], &fileStat) < 0)// determine information about a file based on its file descriptor.
		return RC_FILE_HANDLE_NOT_INIT;
	fHandle->fileName = fileName;

    //Calculate the total num of pages
    //printf("(printed in openPageFile for debug)File size, %d\n", fileStat.st_size);
    if(fileStat.st_size == 0)
        pnum = 1;
    else if(fileStat.st_size % PAGE_SIZE)	
		pnum = fileStat.st_size/PAGE_SIZE + 1;
    else
		pnum = fileStat.st_size/PAGE_SIZE;
    
	fHandle->totalNumPages = pnum;
    //printf("(print in openPageFile for debug)totalNumPages, %d\n", fHandle->totalNumPages);

    //Initialization
    fHandle->curPagePos = 0;
	fHandle->mgmtInfo = fd + i;	// fd+i is the address of the array element fd[i]
    //update i
	i=(i+1)%20;

	return 	RC_OK;
}

/* Close an open page file */
RC closePageFile(SM_FileHandle *fHandle)
{
	if(fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;
    int * fd;
    fd = (int *)fHandle->mgmtInfo;	// get the file descriptor

    close(*fd);

    //Clear related file information
	fHandle->fileName = NULL;
	fHandle->curPagePos = -1;
	fHandle->totalNumPages = -1;
	fHandle->mgmtInfo = NULL;	//

	return RC_OK;
}

/* Destroy (delete) a page file. */
RC destroyPageFile (char *fileName)
{
	int ret = remove(fileName);
	if(ret != 0)
		return RC_FILE_NOT_FOUND;
	printf("File deleted successfully\n");
    return RC_OK;
}


/*************************************************************** Read and Write Methods ***************************************************************/


/* The method reads the pageNumth block from a file and stores its content in the memory pointed to by the memPage page handle. If the file has less than pageNum pages, the method should return RC_READ_NON_EXISTING_PAGE. */
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int offset = pageNum * PAGE_SIZE;

    //Page does not exist
    if( pageNum + 1 > fHandle->totalNumPages  ||  pageNum < 0)
        return RC_READ_NON_EXISTING_PAGE;

    //Page exists
    //Move the file pointer to the specific location
	if( lseek(*(int *)fHandle->mgmtInfo, offset, SEEK_SET) == -1)
		return RC_READ_NON_EXISTING_PAGE;
	//Read from the corresponding page
    if( read(*(int *)fHandle->mgmtInfo, memPage, PAGE_SIZE) < 0)
		return RC_READ_NON_EXISTING_PAGE;

	//Read successfully, update the current page position
    fHandle->curPagePos = pageNum;

	return RC_OK;
}

/* Return the current page position in a file */
int getBlockPos (SM_FileHandle *fHandle)
{
    return fHandle->curPagePos;
}

/* Read the first block of a file into memPage */
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if( fHandle == NULL || memPage == NULL)
        return RC_READ_NON_EXISTING_PAGE;

    if( fHandle->totalNumPages < 1)
        return RC_READ_NON_EXISTING_PAGE;
	
	//Move the file pointer to the beginning 
	if( lseek(*(int *)fHandle->mgmtInfo, 0, SEEK_SET) == -1)
		return RC_READ_NON_EXISTING_PAGE;
	//Read
	if( read(*(int *)fHandle->mgmtInfo, memPage, PAGE_SIZE) < 0)
		return RC_READ_NON_EXISTING_PAGE;
		
    fHandle->curPagePos = 0;
	
	return RC_OK;
}

/* Read the last page in a file */
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int offset = (fHandle->totalNumPages - 1) * PAGE_SIZE;

    if( fHandle == NULL || memPage == NULL)
        return RC_READ_NON_EXISTING_PAGE;

	if( lseek(*(int *)fHandle->mgmtInfo, offset, SEEK_SET) == -1)
        return RC_READ_NON_EXISTING_PAGE;

    if( read(*(int *)fHandle->mgmtInfo, memPage, PAGE_SIZE) < 0)
		return RC_READ_NON_EXISTING_PAGE;

	fHandle->curPagePos -= 1;

 	return RC_OK;
}

/* Read the previous page relative to the curPagePos of the file. The curPagePos should be moved to the page that was read. If the user tries to read a block before the first page or after the last page of the file, the method should return RC_READ_NON_EXISTING_PAGE. */
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if( fHandle == NULL || memPage == NULL)
        return RC_READ_NON_EXISTING_PAGE;

    //No previous block found
    if( fHandle->curPagePos <= 0)
		return RC_READ_NON_EXISTING_PAGE;
	//Locate the pointer
    if( lseek(*(int *)fHandle->mgmtInfo, -PAGE_SIZE, SEEK_CUR) == -1)
        return RC_READ_NON_EXISTING_PAGE;
	//Read
    if( read(*(int *)fHandle->mgmtInfo, memPage, PAGE_SIZE) < 0)
		return RC_READ_NON_EXISTING_PAGE;

    //update the current page position
    fHandle->curPagePos -= 1;

	return RC_OK;
}

/* Read the current page relative to the curPagePos of the file. */
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if( fHandle == NULL || memPage == NULL)
        return RC_READ_NON_EXISTING_PAGE;

	if( fHandle->curPagePos + 1 > fHandle->totalNumPages  ||  fHandle->curPagePos < 0)
		return RC_READ_NON_EXISTING_PAGE;

    if( read(*(int *)fHandle->mgmtInfo, memPage, PAGE_SIZE) == -1)
		return RC_READ_NON_EXISTING_PAGE;

	return RC_OK;
}

/* Read the next page relative to the curPagePos of the file. */
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if( fHandle == NULL || memPage == NULL)
        return RC_READ_NON_EXISTING_PAGE;

    //No next block found
	if( fHandle->curPagePos + 1 >= fHandle->totalNumPages)
		return RC_READ_NON_EXISTING_PAGE;

    if( lseek(*(int *)fHandle->mgmtInfo, PAGE_SIZE, SEEK_CUR) == -1)
		return RC_READ_NON_EXISTING_PAGE;

	if( read(*(int *)fHandle->mgmtInfo, memPage, PAGE_SIZE) < 0)
		return RC_READ_NON_EXISTING_PAGE;

    fHandle->curPagePos += 1;
	
	return RC_OK;
}

/* Write a page to disk using either the current position or an absolute position. */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//Page does not exist
	if( pageNum + 1 > fHandle->totalNumPages  ||  pageNum < 0)
		return RC_WRITE_FAILED;
		
	//Page exists
	//Move the file pointer to the specific location
	if( lseek(*(int *)fHandle->mgmtInfo, pageNum * PAGE_SIZE, SEEK_SET) == -1)
        return RC_WRITE_FAILED;
	//Write to the corresponding page
	if( write(*(int *)fHandle->mgmtInfo, memPage, PAGE_SIZE) < 0)
		return RC_WRITE_FAILED;

    //Write successfully, update the current page position
    fHandle->curPagePos = pageNum;

    return RC_OK;
}

/* Write a page to disk using either the current position or an absolute position. */
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

/* Increase the number of pages in the file by one. The new last page should be filled with zero bytes. */
RC appendEmptyBlock(SM_FileHandle *fHandle) {

	char appdPage[PAGE_SIZE] = { 0 };

    if( lseek(*(int *)fHandle->mgmtInfo, 0, SEEK_END) == -1)
        return RC_WRITE_FAILED;

	if( write(*(int *)fHandle->mgmtInfo, appdPage, PAGE_SIZE) < 0)
		return RC_WRITE_FAILED;

    fHandle->totalNumPages += 1;

	return RC_OK;
}

/* If the file has less than numberOfPages pages then increase the size to numberOfPages. */
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
	struct stat fileStat;
	int * fd;
    long delta;
    SM_PageHandle memPage;
	
	//case that need not to increase 
	if( fHandle->totalNumPages > numberOfPages)
        return RC_OK;
		
	//case that need to increase
	fd = (int *)fHandle->mgmtInfo;
    if( fstat(*fd, &fileStat) < 0)
		return RC_FILE_HANDLE_NOT_INIT;
    delta = (numberOfPages * PAGE_SIZE) - fileStat.st_size;	//calculate needed size

	memPage = (SM_PageHandle)calloc(delta, sizeof(char));
	
	//locate the pointer to end of file
    if( lseek(*(int *)fHandle->mgmtInfo, 0, SEEK_END) == -1)
		return RC_WRITE_FAILED;
	//write
    if( write(*(int *)fHandle->mgmtInfo, memPage, delta) < 0)
		return RC_WRITE_FAILED;
	//update the total number of pages of the file
	fHandle->totalNumPages = numberOfPages;
	
	return RC_OK;
}
