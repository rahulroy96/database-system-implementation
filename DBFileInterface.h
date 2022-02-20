#ifndef DBFILEINTERFACE_H
#define DBFILEINTERFACE_H

#include "Record.h"
#include "Schema.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

class DBFileInterface
{
public:
	DBFileInterface(){};

	virtual ~DBFileInterface(){};

	virtual int Create(const char *fpath, void *startup) = 0;
	virtual int Open(const char *fpath) = 0;
	virtual int Close() = 0;

	virtual void Load(Schema &myschema, const char *loadpath) = 0;

	virtual void MoveFirst() = 0;
	virtual void Add(Record &addme) = 0;
	virtual int GetNext(Record &fetchme) = 0;
	virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;
};
#endif
