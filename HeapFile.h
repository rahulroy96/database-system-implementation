#ifndef HEAPFILE_H
#define HEAPFILE_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFileInterface.h"

class HeapFile : public DBFileInterface
{
private:
	File *file;

	off_t readPtr;
	off_t writePtr;

	Page *readPage;
	Page *writePage;

	bool writePageDirty;

	ComparisonEngine *comparisonEngine;

public:
	HeapFile();
	~HeapFile() override;

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
