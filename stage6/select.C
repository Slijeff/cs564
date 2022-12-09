#include "catalog.h"
#include "query.h"

// forward declaration
const Status ScanSelect(const string &result,
						const int projCnt,
						const AttrDesc projNames[],
						const AttrDesc *attrDesc,
						const Operator op,
						const char *filter,
						const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string &result,
					   const int projCnt,
					   const attrInfo projNames[],
					   const attrInfo *attr,
					   const Operator op,
					   const char *attrValue)
{
	// Qu_Select sets up things and then calls ScanSelect to do the actual work
	cout << "Doing QU_Select " << endl;

	// Get info for attr
	AttrDesc attrDesc;
	if (attr)
	{
		Status status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
		if (status != OK)
			return status;
	}
	const char *filter;
	int itemp;
	float ftemp;
	if ((Datatype)attrDesc.attrType == STRING)
	{
		filter = attrValue;
	}
	else if ((Datatype)attrDesc.attrType == INTEGER)
	{
		itemp = atoi(attrValue);
		filter = (char *)(&itemp);
	}
	else if ((Datatype)attrDesc.attrType == FLOAT)
	{
		ftemp = atof(attrValue);
		filter = (char *)(&ftemp);
	}
	else
	{
		// No filtering needed
		filter = NULL;
	}

	// Get info for projections
	int record_length = 0;
	AttrDesc *projInfos = new AttrDesc[projCnt];
	for (int i = 0; i < projCnt; i++)
	{
		Status status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projInfos[i]);
		if (status != OK)
			return status;
		record_length += projInfos[i].attrLen;
	}

	return ScanSelect(result, projCnt, projInfos, &attrDesc, op, filter, record_length);
}

const Status ScanSelect(const string &result,
#include "stdio.h"
#include "stdlib.h"
						const int projCnt,
						const AttrDesc projNames[],
						const AttrDesc *attrDesc,
						const Operator op,
						const char *filter,
						const int reclen)
{
	cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	// We need both InsertFileScan and HeapFileScan to do this
	// InsertFileScan is used to create the result relation
	// HeapFileScan is used to scan the input relation

	Status status;

	InsertFileScan *ifs = new InsertFileScan(result, status);
	if (status != OK)
		return status;
	HeapFileScan *hfs = new HeapFileScan(projNames[0].relName, status);
	if (status != OK)
		return status;

	status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
	if (status != OK)
		return status;

	// Now we have to scan the input relation and insert the records that satisfy the condition
	// Also, we have to project the records that satisfy the condition
	RID scanRid;
	RID insertRid;
	Record scanRec;
	Record insertRec;
	insertRec.data = new char[reclen];
	insertRec.length = reclen;
	int offset = 0;
	while (hfs->scanNext(scanRid) == OK)
	{

		status = hfs->getRecord(scanRec);
		if (status != OK)
			return status;

		for (int i = 0; i < projCnt; i++)
		{
			memcpy((char *)insertRec.data + offset, (char *)scanRec.data + projNames[i].attrOffset, projNames[i].attrLen);
			offset += projNames[i].attrLen;
		}
		offset = 0;
		status = ifs->insertRecord(insertRec, insertRid);
		if (status != OK)
			return status;
	}
	status = hfs->endScan();
	if (status != OK)
		return status;

	// clean up
	delete ifs;
	delete hfs;

	return OK;
}
