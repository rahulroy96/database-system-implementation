#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp
{
public:
	// blocks the caller until the particular relational operator
	// has run to completion
	virtual void WaitUntilDone() = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages(int n) = 0;
};

class ThreadHelper
{
	static void *EntryPoint(void *arg);
	virtual void Execute() = 0;
};

class SelectFile : public RelationalOp
{

private:
	pthread_t thread;
	Record *buffer;

	DBFile *inFile;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;

public:
	void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone() override;
	void Use_n_Pages(int n) override;

	static void *EntryPoint(void *arg);
	void Execute();
};

class SelectPipe : public RelationalOp
{
	pthread_t thread;
	Record *buffer;

	Pipe *inPipe;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone();
	void Use_n_Pages(int n) {}

	static void *EntryPoint(void *arg);
	void Execute();
};

class Project : public RelationalOp
{
	pthread_t thread;
	Record *buffer;

	Pipe *inPipe;
	Pipe *outPipe;

	int *keepMe;
	int numAttsInput;
	int numAttsOutput;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone();
	void Use_n_Pages(int n) {}

	static void *EntryPoint(void *arg);
	void Execute();
};

class Join : public RelationalOp
{
	pthread_t thread;

	Record *bufferL;
	Record *bufferR;
	Record *mergedRecord;

	Pipe *inPipeL;
	Pipe *inPipeR;
	Pipe *outPipe;

	CNF *selOp;
	Record *literal;

	OrderMaker *sortOrderL;
	OrderMaker *sortOrderR;

	int runLength;

public:
	void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone();
	void Use_n_Pages(int n);

	static void *EntryPoint(void *arg);
	void Execute();
	void SortedMergeJoin();
	void BlockNestedLoopJoin();
};

class DuplicateRemoval : public RelationalOp
{
	pthread_t thread;
	Record *buffer;
	Pipe *inPipe;
	Pipe *outPipe;
	Schema *mySchema;

	int runLength;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone();
	void Use_n_Pages(int n);

	static void *EntryPoint(void *arg);
	void Execute();
};

class Sum : public RelationalOp
{
	pthread_t thread;
	Record *buffer;
	Pipe *inPipe;
	Pipe *outPipe;
	Function *computeMe;

	int runLength;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone();
	void Use_n_Pages(int n);

	static void *EntryPoint(void *arg);
	void Execute();
};

class GroupBy : public RelationalOp
{
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	OrderMaker *groupAtts;
	Function *computeMe;

	int runLength;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone();
	void Use_n_Pages(int n);

	static void *EntryPoint(void *arg);
	void Execute();
	void ComposeRecord();
};

class WriteOut : public RelationalOp
{
	pthread_t thread;
	Record *buffer;
	Pipe *inPipe;
	FILE *outFile;
	Schema *mySchema;

	int runLength;

public:
	void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone();
	void Use_n_Pages(int n);

	static void *EntryPoint(void *arg);
	void Execute();
};
#endif
