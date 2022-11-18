#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File *file;
    Status status;
    FileHdrPage *hdrPage;
    int hdrPageNo;
    int newPageNo;
    Page *newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
        // file doesn't exist. First create it and allocate
        // an empty header page and data page.
        db.createFile(fileName);
        db.openFile(fileName, file);

        Page *temp;
        status = bufMgr->allocPage(file, hdrPageNo, temp);
        if (status != OK)
        {
            return status;
        }
        hdrPage = (FileHdrPage *)temp;
        // fill in the filename
        strcpy(hdrPage->fileName, fileName.c_str());
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK)
        {
            return status;
        }
        // Invoke its init() method to initialize the page
        newPage->init(newPageNo);
        newPage->setNextPage(-1);

        // Finally, store the page number of the data page in firstPage and lastPage attributes of the FileHdrPage.
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->recCnt = 0;
        hdrPage->pageCnt = 1;
        // Unpin the header page and the data page
        bufMgr->unPinPage(file, hdrPageNo, true);
        bufMgr->unPinPage(file, newPageNo, true);
        // Mark the file as dirty and close it
        bufMgr->flushFile(file);
        db.closeFile(file);
        return OK;
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
    return (db.destroyFile(fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string &fileName, Status &returnStatus)
{
    Status status;
    Page *pagePtr;

    cout << "opening file " << fileName << endl;

    //  open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        // reads and pins the header page for the file in the buffer pool
        filePtr->getFirstPage(headerPageNo);
        // initializing the private data members
        bufMgr->readPage(filePtr, headerPageNo, pagePtr);
        headerPage = (FileHdrPage *)pagePtr;
        hdrDirtyFlag = false;
        // read and pin the first page of the file into the buffer pool
        bufMgr->readPage(filePtr, headerPage->firstPage, pagePtr);
        curDirtyFlag = false;

        curRec = NULLRID;
        returnStatus = OK;
    }
    else
    {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        if (status != OK)
            cerr << "error in unpin of date page\n";
    }

    // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK)
        cerr << "error in unpin of header page\n";

    // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
    // if (status != OK) cerr << "error in flushFile call\n";
    // before close the file
    status = db.closeFile(filePtr);
    if (status != OK)
    {
        cerr << "error in closefile call\n";
        Error e;
        e.print(status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
    return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID &rid, Record &rec)
{
    Status status;

    // no page currently pinned
    if (curPage == NULL)
    {
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK)
        {
            return status;
        }
        // bookkeeping
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
        status = curPage->getRecord(rid, rec);
        if (status != OK)
        {
            return status;
        }
        curRec = rid;
        return OK;
    }
    else
    {
        // if already have a working page
        if (rid.pageNo != curPageNo)
        {
            // if page number doesn't match
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
            {
                // bookeeping
                curPage = NULL;
                curPageNo = 0;
                curDirtyFlag = false;
                return status;
            }
            // read in required page
            status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
            if (status != OK)
            {
                return status;
            }
            // bookkeeping
            curPageNo = rid.pageNo;
            curDirtyFlag = false;
            status = curPage->getRecord(rid, rec);
            if (status != OK)
            {
                return status;
            }
            curRec = rid;
            return OK;
        }
        else
        {
            // current page matches rid page
            status = curPage->getRecord(rid, rec);
            if (status != OK)
            {
                return status;
            }
            curRec = rid;
            return OK;
        }
    }
    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
}

HeapFileScan::HeapFileScan(const string &name,
                           Status &status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
                                     const int length_,
                                     const Datatype type_,
                                     const char *filter_,
                                     const Operator op_)
{
    if (!filter_)
    { // no filtering requested
        filter = NULL;
        return OK;
    }

    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int) || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}

const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo)
    {
        if (curPage != NULL)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;
        }
        // restore curPageNo and curRec values
        curPageNo = markedPageNo;
        curRec = markedRec;
        // then read the page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
        curDirtyFlag = false; // it will be clean
    }
    else
        curRec = markedRec;
    return OK;
}

const Status HeapFileScan::scanNext(RID &outRid)
{
    Status status = OK;
    RID nextRid;
    RID tmpRid;
    int nextPageNo;
    Record rec;

    // start from the beginning and get the first record
    if (curPage == NULL)
    {
        status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage);
        if (status != OK)
        {
            return status;
        }
        // not modifying
        curDirtyFlag = false;
        // read first record of first page
        status = curPage->firstRecord(tmpRid);
        // no record in the file
        if (status == NORECORDS)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
            {
                return status;
            }
            curPageNo = 0;
            curPage = NULL;
            // to pass a testcase
            return FILEEOF;
        }

        // convert to record
        status = curPage->getRecord(tmpRid, rec);
        if (status != OK)
        {
            return status;
        }
        if (matchRec(rec))
        {
            outRid = tmpRid;
            curRec = tmpRid;
            return OK;
        }
    }
    else
    {
        // resume from last
        // look for record that satisfies the condition
        while (1)
        {
            status = curPage->nextRecord(curRec, tmpRid);
            if (status != OK && (status == NORECORDS || status == ENDOFPAGE))
            {
                // no more records on the current page
                // attempt to get next available record
                while (status == NORECORDS || status == ENDOFPAGE)
                {
                    curPage->getNextPage(nextPageNo);
                    if (nextPageNo == -1)
                    {
                        return FILEEOF;
                    }
                    // bookkeeping
                    bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                    curPageNo = nextPageNo;
                    curPage = NULL;

                    bufMgr->readPage(filePtr, nextPageNo, curPage);
                    status = curPage->firstRecord(tmpRid);
                }
            }
            curRec = tmpRid;

            // convert to record
            status = curPage->getRecord(curRec, rec);
            if (status != OK)
            {
                return status;
            }

            if (matchRec(rec))
            {
                outRid = curRec;
                return OK;
            }
        }
    }
}

// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page

const Status HeapFileScan::getRecord(Record &rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file.
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true;
    return status;
}

// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record &rec) const
{
    // no filtering requested
    if (!filter)
        return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length - 1) >= rec.length)
        return false;

    float diff = 0; // < 0 if attr < fltr
    switch (type)
    {

    case INTEGER:
        int iattr, ifltr; // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr; // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch (op)
    {
    case LT:
        if (diff < 0.0)
            return true;
        break;
    case LTE:
        if (diff <= 0.0)
            return true;
        break;
    case EQ:
        if (diff == 0.0)
            return true;
        break;
    case GTE:
        if (diff >= 0.0)
            return true;
        break;
    case GT:
        if (diff > 0.0)
            return true;
        break;
    case NE:
        if (diff != 0.0)
            return true;
        break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string &name,
                               Status &status) : HeapFile(name, status)
{
    // Do nothing. Heapfile constructor will bread the header page and the first
    //  data page of the file into the buffer pool

    // Heapfile constructor will read the header page and the first
    // data page of the file into the buffer pool
    // If the first data page of the file is not the last data page of the file
    // unpin the current page and read the last page
    if ((curPage != NULL) && (curPageNo != headerPage->lastPage))
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK)
            cerr << "error in unpin of data page\n";

        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            cerr << "error in readPage \n";

        curDirtyFlag = false;
    }
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK)
            cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid)
{
    Page *newPage;
    int newPageNo;
    Status status, unpinstatus;
    RID rid;

    // check for very large records
    if ((unsigned int)rec.length > PAGESIZE - DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }
    // check if curPage is NULL
    if (curPage != NULL)
    {
        // check if the record can fit on the current page
        if (curPage->getFreeSpace() >= rec.length)
        {
            // insert the record on the current page
            status = curPage->insertRecord(rec, outRid);
            if (status != OK)
                return status;
            // bookkeeping
            curDirtyFlag = true;
            headerPage->recCnt++;
            hdrDirtyFlag = true;
        }
        // if doesn't fit, unpin the current page
        else
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;
            // bookkeeping for current page
            // curPage = NULL;
            curPageNo = 0;
            curDirtyFlag = false;

            // create a new page
            status = bufMgr->allocPage(filePtr, newPageNo, newPage);
            if (status != OK)
                return status;
            // initialize the new page
            newPage->init(newPageNo);
            // bookkeeping for new page
            curPage->setNextPage(newPageNo);
            curPage = newPage;
            curPageNo = newPageNo;
            // insert the record on the new page
            status = curPage->insertRecord(rec, outRid);
            if (status != OK)
                return status;
            // bookkeeping
            curDirtyFlag = true;
            headerPage->recCnt++;
            headerPage->lastPage = newPageNo;
            headerPage->pageCnt++;
            hdrDirtyFlag = true;
        }
    }
    // if curPage is NULL, make the last page the current page
    else
    {
        // read the last page of the file
        status = bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
        if (status != OK)
            return status;
        // bookkeeping for current page
        curPageNo = headerPage->lastPage;
        curDirtyFlag = false;
        // check if the record can fit on the current page
        if (curPage->getFreeSpace() >= rec.length)
        {
            // insert the record on the current page
            status = curPage->insertRecord(rec, outRid);
            if (status != OK)
                return status;
            // bookkeeping
            curDirtyFlag = true;
            headerPage->recCnt++;
            hdrDirtyFlag = true;
        }
        // if doesn't fit, unpin the current page
        else
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;
            // bookkeeping for current page
            // curPage = NULL;
            curPageNo = 0;
            curDirtyFlag = false;

            // create a new page
            status = bufMgr->allocPage(filePtr, newPageNo, newPage);
            if (status != OK)
                return status;
            // initialize the new page
            newPage->init(newPageNo);
            // bookkeeping for new page
            curPage->setNextPage(newPageNo);
            curPage = newPage;
            curPageNo = newPageNo;
            // insert the record on the new page
            status = curPage->insertRecord(rec, outRid);
            if (status != OK)
                return status;
            // bookkeeping
            curDirtyFlag = true;
            headerPage->recCnt++;
            headerPage->lastPage = newPageNo;
            hdrDirtyFlag = true;
        }
    }
    return OK;
}
