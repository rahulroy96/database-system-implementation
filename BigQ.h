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
	off_t endPtr;
	off_t currentPtr;
	Page *currentPage;

	ComparisonEngine *comparisonEngine;
	OrderMaker *sortOrder;

public:
	Record *head;
	Run(int runlen, off_t startPtr, off_t endPtr, File *file, OrderMaker *sortOrder);
	~Run();

	// Move to the first record in the run
	void MoveFirst();

	// Read the next record as the head of the run
	int GetNext();

	// Consume the head record and copy it to rec
	void GetHead(Record &rec);

	// Sort the given vector and write it to file as pages
	int SortWrite(vector<Record *> &records);
};

class BigQ
{

	Pipe *input;
	Pipe *output;
	OrderMaker *sortorder;
	int runlen;

	File *file;
	const char *fileName = "bigQ.bin";
	ComparisonEngine *comparisonEngine;

	vector<Run *> runs;

public:
	BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ();

	void RunFirstPhase();
	void RunSecondPhase();
};

#endif
