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
	int runLength;

	SortState state;

	Pipe *input;
	Pipe *output;
	BigQ *bq;

	off_t readPtr;

	Page *readPage;
	Page *writePage;

	// Merge the entries in bigQ and the existing records in file.
	void MergeBigQ();
	void InitializeWriting();

public:
	SortedFile();
	~SortedFile() override;

	int Create(const char *fpath, void *startup) override;
	int Open(const char *fpath) override;
	int Close() override;

	void Load(Schema &myschema, const char *loadpath) override;

	void MoveFirst() override;
	void Add(Record &addme) override;
	int GetNext(Record &fetchme) override;
	int GetNext(Record &fetchme, CNF &cnf, Record &literal) override;
};
#endif
