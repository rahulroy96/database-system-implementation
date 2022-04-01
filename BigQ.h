#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class BigQRun
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
	BigQRun(int runlen, off_t startPtr, off_t endPtr, File *file, OrderMaker *sortOrder);
	~BigQRun();

	// Move to the first record in the run
	void MoveFirst();

	// Read the next record as the head of the run.
	// Returns 1 on success and 0 if there is no more records
	int LoadNextHead();

	// Consume the head record and copy it to rec
	void ConsumeHead(Record &rec);

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
	char *fileName;
	ComparisonEngine *comparisonEngine;

	vector<BigQRun *> runs;

public:
	BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ();

	void RunFirstPhase();
	void RunSecondPhase();
};

#endif
