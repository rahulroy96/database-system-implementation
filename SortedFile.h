#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFileInterface.h"
#include "Pipe.h"
#include "BigQ.h"

#define BUFF_SIZE 100 // pipe cache size

struct SortInfo
{
	OrderMaker *sortOrder;
	int runLength;
};

enum SortState
{
	reading,
	writing,

};

class SortedFile : public DBFileInterface
{
private:
	// const static int BUF_SIZE = 100;
	string fileName;
	File *file;

	OrderMaker *sortOrder;
	OrderMaker *querySortOrder;
	OrderMaker *queryLiteralOrder;

	int runLength;
	int addCount;

	SortState state;

	Pipe *input;
	Pipe *output;
	BigQ *bq;

	off_t readPtr;

	Page *readPage;
	Page *writePage;

	ComparisonEngine *comparisonEngine;

	/*
	 *  Change the state to reading and shutdown the input pipe
	 *  Initialize a new instance of file and merge the records from
	 *  the output pipe with records already in the file.
	 *  Delete the old instance and make the new instance the file.
	 */
	void MergeBigQ();

	// Change the state to writing
	// Initialize the pipes and bigq for writing
	void InitializeWriting();
	void DeleteQueryOrders();

public:
	SortedFile();
	~SortedFile() override;

	/*
	 *  Open the file.
	 *  Since its a newly created file, set the readPtr to 0.
	 *  Write the sortOrder and runLength to meta file
	 */
	int Create(const char *fpath, void *startup) override;

	/*
	 *  Open the file.
	 *  Read the SortOrder and RunLength from the meta file
	 */
	int Open(const char *fpath) override;

	/*
	 *  Closes the file.
	 *  Merge the bigQ if the state is writing.
	 */
	int Close() override;

	/*
	 *  Bulk load data from file located at path loadpath.
	 *  If the current state is reading initialize for writing.
	 */
	void Load(Schema &myschema, const char *loadpath) override;

	/*
	 *  Move the read pointer to point to first page
	 *  Read the first page
	 *  If current state is writing, Merge the records in output pipe
	 */
	void MoveFirst() override;

	/*
	 *  If current state is reading change state to writing and
	 *  initialize the internal input pipe, output pipe and bigQ.
	 *  Add the record to the input pipe.
	 */
	void Add(Record &addme) override;

	/*
	 *  Check if current state is reading, if not merge the records in bigQ
	 *  Populate fetchMe with the next record from the current readPage
	 *  if no record available in current, read the next page
	 *  and return the first record. if no more pages left return 0
	 */
	int GetNext(Record &fetchme) override;

	/*
	 *  If state is not reading, mergeBigQ
	 *  If the queryOrderMaker is null, build the query OrderMaker
	 *  and then do binary search to find the first page.
	 *  For subsequent calls, just do linear search till the literal
	 *  record is greater than the fetched reord in queryOrder.
	 *  if no more records left return 0.
	 */
	int GetNext(Record &fetchme, CNF &cnf, Record &literal) override;
};
#endif
