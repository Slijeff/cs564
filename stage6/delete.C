#include "catalog.h"
#include "query.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string &relation,
					   const string &attrName,
					   const Operator op,
					   const Datatype type,
					   const char *attrValue)
{
	Status status;

	HeapFileScan *hfs = new HeapFileScan(relation, status);
	if (status != OK)
		return status;

	// No attribute name specified, delete all records
	if (attrName.empty())
	{
		// REVIEW: type?
		status = hfs->startScan(0, 0, type, NULL, op);
		if (status != OK)
			return status;
		RID rid;
		while (hfs->scanNext(rid) == OK)
		{
			status = hfs->deleteRecord();
			if (status != OK)
				return status;
		}
		status = hfs->endScan();
		if (status != OK)
			return status;
		delete hfs;
		return OK;
	}

	// Get attribute info
	AttrDesc attrInfo;
	status = attrCat->getInfo(relation, attrName, attrInfo);
	if (status != OK)
		return status;

	// Get filter value
	const char *filter;
	if (type == STRING)
	{
		filter = attrValue;
	}
	else if (type == INTEGER)
	{
		int temp = atoi(attrValue);
		filter = (char *)(&temp);
	}
	else if (type == FLOAT)
	{
		float temp = atof(attrValue);
		filter = (char *)(&temp);
	}
	else
	{
		// No filtering needed
		filter = NULL;
	}

	// Scan through the relation
	status = hfs->startScan(attrInfo.attrOffset, attrInfo.attrLen, type, filter, op);
	if (status != OK)
		return status;
	RID rid;
	while (hfs->scanNext(rid) == OK)
	{
		status = hfs->deleteRecord();
		if (status != OK)
			return status;
	}
	status = hfs->endScan();
	if (status != OK)
		return status;
	delete hfs;
	return OK;
}
