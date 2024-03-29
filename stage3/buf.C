#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
    }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++)
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

/**
 * @brief  Finds a free frame using the clock algorithm
 * @note
 * @param  &frame: the frame to return
 * @retval BUFFEREXCEEDED if all buffer frames are pinned, UNIXERR if the call to the I/O layer returned an error when a dirty page was being written to disk and OK otherwise.
 */
const Status BufMgr::allocBuf(int &frame)
{

    int clockHand = 0;
    // Keep track of the number of frames we've checked
    int framesChecked = 0;
    while (true)
    {
        framesChecked++;
        // If we've checked all the frames, return BUFFEREXCEEDED
        if (framesChecked == 2 * numBufs)
        {
            return BUFFEREXCEEDED;
        }
        clockHand = (clockHand + 1) % numBufs;
        BufDesc *tmpbuf = &bufTable[clockHand];
        if (tmpbuf->valid == false)
        {
            frame = clockHand;
            return OK;
        }
        if (tmpbuf->refbit == true)
        {
            tmpbuf->refbit = false;
            continue;
        }
        if (tmpbuf->pinCnt > 0)
        {
            continue;
        }
        if (tmpbuf->dirty == true)
        {
            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[clockHand]));
        }
        hashTable->remove(tmpbuf->file, tmpbuf->pageNo);
        tmpbuf->Clear();
        frame = clockHand;
        return OK;
    }
}

/**
 * @brief  Read the page from disk into the buffer pool frame
 * @note
 * @param  *file: the file on disk
 * @param  PageNo: the page in the file
 * @param  *&page: the page to return
 * @retval OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames are pinned, HASHTBLERROR if a hash table error occurred.
 */
const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
    // Check whether the page is in the buffer pool by invoking the lookup function
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    // If the page is in the buffer pool, increment the pin count, set appropriate refbit and return the page
    if (status == OK)
    {
        bufTable[frameNo].pinCnt++;
        bufTable[frameNo].refbit = true;
        page = &(bufPool[frameNo]);
        return OK;
    }
    // If not in buffer pool
    else
    {
        // Call allocBuf() to allocate a buffer frame
        int frame;
        status = allocBuf(frame);
        if (status != OK)
            return status;
        // Read the page from disk into the buffer pool frame
        status = file->readPage(PageNo, &(bufPool[frame]));
        if (status != OK)
            return status;
        // Insert the page into the hash table
        status = hashTable->insert(file, PageNo, frame);
        if (status != OK)
            return status;
        // Set appropriate fields in the buffer descriptor
        bufTable[frame].Set(file, PageNo);
        // Set the page pointer to point to the page in the buffer pool
        page = &(bufPool[frame]);
        return OK;
    }

    return OK;
}

/**
 * @brief  Decrements the pinCnt of the frame containing (file, PageNo)
 * @note
 * @param  file: the file on disk
 * @param  PageNo: the page in the file
 * @param  dirty: if the page has been written or not
 * @retval OK if no errors occurred, HASHNOTFOUND if the page is not in the buffer pool hash table, PAGENOTPINNED if the pin count is already 0
 */
const Status BufMgr::unPinPage(File *file, const int PageNo,
                               const bool dirty)
{
    // Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true, sets the dirty bit.
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if (status != OK)
        return status;
    if (bufTable[frameNo].pinCnt == 0)
        return PAGENOTPINNED;
    bufTable[frameNo].pinCnt--;
    if (dirty)
        bufTable[frameNo].dirty = true;
    return OK;
}

/**
 * @brief  Allocate an empty page in the file
 * @note
 * @param  *file: the file to allocate
 * @param  &pageNo: the page number to allocate
 * @param  *&page: the new page to allocate
 * @retval OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames are pinned and HASHTBLERROR if a hash table error occurred.
 */
const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
    // Allocate empty page in the specific file
    if (file->allocatePage(pageNo) != OK)
    {
        return UNIXERR;
    }

    int frame = -1;
    Status status;
    // Obtain bufferpool frame
    status = allocBuf(frame);
    if (status != OK) {
        return status;
    }
    // Insert into hashtable
    status = hashTable->insert(file, pageNo, frame);
    if (status != OK)
        return status;
    // Call Set on frame
    bufTable[frame].Set(file, pageNo);
    // Init page
    bufPool[frame].init(pageNo);
    page = &(bufPool[frame]);

    return OK;
}

const Status BufMgr::disposePage(File *file, const int pageNo)
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                     << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

            tmpbuf->file = NULL;
            tmpbuf->pageNo = -1;
            tmpbuf->valid = false;
        }

        else if (tmpbuf->valid == false && tmpbuf->file == file)
            return BADBUFFER;
    }

    return OK;
}

void BufMgr::printSelf(void)
{
    BufDesc *tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
