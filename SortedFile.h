#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFileInterface.h"

struct SortInfo
{
	OrderMaker *myOrder;
	int runLength;
};

class SortedFile : public DBFileInterface
{
private:
	File *file;

	OrderMaker *myOrder;
	int runLength;

	// SortInfo *sortInfo;

	off_t readPtr;
	// off_t writePtr;

	Page *readPage;
	Page *writePage;

	// bool writePageDirty;

	// ComparisonEngine *comparisonEngine;

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
