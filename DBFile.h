#ifndef DBFILE_H
#define DBFILE_H

#include "DBFileInterface.h"



class DBFile
{
private:
	DBFileInterface *file;

public:
	DBFile();
	~DBFile();
	int Create(const char *fpath, fType file_type, void *startup);
	int Open(const char *fpath);
	int Close();

	void Load(Schema &myschema, const char *loadpath);

	void MoveFirst();
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);

	int Remove(char *filename);
};
#endif
