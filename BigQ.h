#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class Run
{
	int runlen;
	File *file;
	off_t startPtr;
	off_t currentPtr;
	Page *currentPage;
	
	ComparisonEngine *comparisonEngine;
	OrderMaker *sortOrder;

public:
	Record *head;
	Run(int runlen, off_t startPtr, File *file, OrderMaker *sortOrder);
	~Run();

	void MoveFirst();
	int GetNext();
	void GetHead(Record &rec);
	void SortWrite(vector<Record *> &records, off_t startPtr);
};

class BigQ
{

	Pipe *input;
	Pipe *output;
	OrderMaker *sortorder;
	int runlen;

	File *file;
	const char *fileName = "temp.bin";
	ComparisonEngine *comparisonEngine;

	vector<off_t> runIndices;

public:
	BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ();

	void RunFirstPhase();
	void RunSecondPhase();
};

#endif
