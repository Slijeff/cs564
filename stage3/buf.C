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
    // Track the amount of time that clock goes through
    int loop = 0;
    unsigned int initialHand = clockHand;

    while (loop < 2)
    {
        advanceClock();
        if (initialHand == clockHand)
        {
            loop++;
        }
        BufDesc *tempBuf = &(bufTable[clockHand]);
        // If not valid
        if (!tempBuf->valid)
        {
            frame = clockHand;
            return OK;
        }
        else
        {
            // If reference bit set
            if (tempBuf->refbit)
            {
                tempBuf->refbit = false;
                // continue to next frame;
                continue;
            }
            else
            {
                // If page is pinned, continue
                if (tempBuf->pinCnt >= 1)
                    continue;
                else
                {
                    // If Dirty bit set, flush to disk
                    // Remove from hash table done by flushFile()
                    if (tempBuf->dirty)
                    {
                        if (flushFile(tempBuf->file) != OK)
                            return UNIXERR;
                        frame = clockHand;
                        return OK;
                    }
                    else
                    {
                        frame = clockHand;
                        return OK;
                    }
                }
            }
        }
    }

    // If after loop no return, all buffer frames are pinned
    return BUFFEREXCEEDED;
}

const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
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
    int frame = 0;
    // Perform frame lookup
    // If page is not in buffer pool hash table
    if (hashTable->lookup(file, PageNo, frame) == HASHNOTFOUND)
    {
        return HASHNOTFOUND;
    }
    else
    {
        // Retrieve the corresponding BufDesc object
        BufDesc *tempBuf = &bufTable[frame];
        // if pin count is already 0
        if (tempBuf->pinCnt == 0)
        {
            return PAGENOTPINNED;
        }

        // Sets the dirty bit
        if (dirty)
        {
            tempBuf->dirty = true;
        }

        // decrement pinCount
        tempBuf->pinCnt--;

        return OK;
    }
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
    // cout << "possible segfault at before allocBuf\n";
    status = allocBuf(frame);
    // cout << "possible segfault at after allocBuf\n";
    if (status != OK)
        return status;
    // Insert into hashtable
    // cout << "possible segfault at bebore hashInsert\n";

    status = hashTable->insert(file, pageNo, frame);
    // cout << "possible segfault at after hashInsert\n";
    if (status != OK)
        return status;
    // Call Set on frame
    // cout << "possible segfault at before Set\n";
    bufTable[frame].Set(file, pageNo);
    // cout << "possible segfault at after Set\n";
    // Init page
    // TODO: may need to clean before init?
    bufPool[frame].init(pageNo);
    // cout << "frame allocated: " << frame << endl;
    page = &(bufPool[frame]);
    // cout << "possible segfault at frameNo\n\n";

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
