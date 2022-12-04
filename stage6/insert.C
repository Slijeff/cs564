#include "catalog.h"
#include "query.h"

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string &relation,
					   const int attrCnt,
					   const attrInfo attrList[])
{
	Status status;
	AttrDesc *attr_descriptions;  // Information about the attributes in current relation
	RelDesc relation_description; // Information about the current relation
	int curr_attr_count;		  // Number of attributes in current relation
	int record_size;			  // Size of the record to be inserted

	// To insert a record into a relation, we need to know the following:
	// 1. The name of the relation
	// 2. The number of attributes in the relation
	// 3. The attributes in the relation
	// 4. The size of the record to be inserted

	// Get the relation description
	status = relCat->getInfo(relation, relation_description);
	if (status != OK)
		return status;

	// Get the attribute descriptions
	status = attrCat->getRelInfo(relation, curr_attr_count, attr_descriptions);
	if (status != OK)
		return status;

	// if the number of attributes in the relation is not equal to the number of attributes in the attrList, return an error
	// REVIEW: not sure the error code to return
	if (curr_attr_count != attrCnt)
		return ATTRTYPEMISMATCH;

	// Get the size of the record to be inserted
	record_size = 0;
	for (int i = 0; i < curr_attr_count; i++)
	{
		for (int j = 0; j < attrCnt; j++)
		{
			if (strcmp(attr_descriptions[i].attrName, attrList[j].attrName) == 0)
			{
				record_size += attr_descriptions[j].attrLen;
			}
		}
	}

	// Create a record to be inserted
	Record record;
	record.data = new char[record_size];
	record.length = record_size;

	// Copy the data from attrList to the record
	for (int i = 0; i < curr_attr_count; i++)
	{
		for (int j = 0; j < attrCnt; j++)
		{
			if (strcmp(attr_descriptions[i].attrName, attrList[j].attrName) == 0)
			{
				Datatype data_type = (Datatype)attr_descriptions[i].attrType;
				if (data_type == STRING)
				{
					memcpy(record.data + attr_descriptions[i].attrOffset, attrList[j].attrValue, attr_descriptions[i].attrLen - 1);
				}
				else if (data_type == INTEGER)
				{
					int temp = atoi((char *)attrList[j].attrValue);
					memcpy(record.data + attr_descriptions[i].attrOffset, &temp, attr_descriptions[i].attrLen);
				}
				else if (data_type == FLOAT)
				{
					float temp = atof((char *)attrList[j].attrValue);
					memcpy(record.data + attr_descriptions[i].attrOffset, &temp, attr_descriptions[i].attrLen);
				}
			}
		}
	}

	// Need insertFileScan to insert
	InsertFileScan *insert_file_scan = new InsertFileScan(relation, status);
	if (status != OK)
		return status;

	// Insert the record
	RID rid;
	status = insert_file_scan->insertRecord(record, rid);
	if (status != OK)
		return status;

	// Clean up memory
	delete[] attr_descriptions;
	delete insert_file_scan;

	// part 6
	return OK;
}
