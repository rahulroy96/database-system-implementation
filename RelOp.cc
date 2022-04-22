#include "RelOp.h"
#include "BigQ.h"

void *SelectFile::EntryPoint(void *arg)
{
	SelectFile *thisPtr = (SelectFile *)arg;
	thisPtr->Execute();
	return nullptr;
}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	this->buffer = new Record();

	int rc = pthread_create(&thread, NULL, EntryPoint, (void *)this);
	if (rc)
	{
		printf("ERROR; return code from pthread_create() is %d\n", rc);
		exit(-1);
	}
}

void SelectFile::Execute()
{
	inFile->MoveFirst();
	while (inFile->GetNext(*buffer, *selOp, *literal))
	{
		outPipe->Insert(buffer);
	}
	outPipe->ShutDown();

}

void SelectFile::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void SelectFile::Use_n_Pages(int runlen)
{
}

// *****************************************************************************//

void *SelectPipe::EntryPoint(void *arg)
{
	SelectPipe *thisPtr = (SelectPipe *)arg;

	thisPtr->Execute();
	return nullptr;
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	this->buffer = new Record();

	int rc = pthread_create(&thread, NULL, EntryPoint, (void *)this);
	if (rc)
	{
		printf("ERROR; return code from SelectPipe pthread_create() is %d\n", rc);
		exit(-1);
	}
}

void SelectPipe::Execute()
{
	ComparisonEngine comparisonEngine;
	while (inPipe->Remove(buffer))
	{
		if (comparisonEngine.Compare(buffer, literal, selOp) != 0)
			outPipe->Insert(buffer);
	}
	outPipe->ShutDown();
}

void SelectPipe::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

// ***************************************************************************** //

void *Project::EntryPoint(void *arg)
{
	Project *thisPtr = (Project *)arg;

	thisPtr->Execute();
	return nullptr;
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
{
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;

	this->buffer = new Record();

	int rc = pthread_create(&thread, NULL, EntryPoint, (void *)this);
	if (rc)
	{
		printf("ERROR; return code from SelectPipe pthread_create() is %d\n", rc);
		exit(-1);
	}
}

void Project::Execute()
{
	while (inPipe->Remove(buffer))
	{
		buffer->Project(keepMe, numAttsOutput, numAttsInput);
		outPipe->Insert(buffer);
	}
	outPipe->ShutDown();
}

void Project::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

// ***************************************************************************** //

void *Join::EntryPoint(void *arg)
{
	Join *thisPtr = (Join *)arg;

	thisPtr->Execute();
	return nullptr;
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	this->bufferL = new Record();
	this->bufferR = new Record();
	this->mergedRecord = new Record();

	sortOrderL = new OrderMaker();
	sortOrderR = new OrderMaker();
	runLength = 10;

	int rc = pthread_create(&thread, NULL, EntryPoint, (void *)this);
	if (rc)
	{
		printf("ERROR; return code from SelectPipe pthread_create() is %d\n", rc);
		exit(-1);
	}
}

void Join::Execute()
{
	if (selOp->GetSortOrders(*sortOrderL, *sortOrderR))
	{
		// The cnf have values using which the 2 big Q functions can be sorted
		SortedMergeJoin();
	}
	else
	{
		// No sort orderd found, perform block nested loop join
		BlockNestedLoopJoin();
	}

	outPipe->ShutDown();
}

void Join::SortedMergeJoin()
{
	int BUFF_SIZE = 100;
	Pipe sortedOutPipeL(BUFF_SIZE);
	Pipe sortedOutPipeR(BUFF_SIZE);
	BigQ *bigQL = new BigQ(*inPipeL, sortedOutPipeL, *sortOrderL, runLength);
	BigQ *bigQR = new BigQ(*inPipeR, sortedOutPipeR, *sortOrderR, runLength);
	ComparisonEngine comparisonEngine;
	bool nextAvailable = true;
	if (!sortedOutPipeL.Remove(bufferL))
		nextAvailable = false;
	if (!sortedOutPipeR.Remove(bufferR))
		nextAvailable = false;
	Record *temp;
	vector<Record *> left_vector;
	vector<Record *> right_vector;
	int numAttsL = 0;
	int numAttsR = 0;
	int *attsToKeep;

	if (nextAvailable)
	{
		numAttsL = bufferL->GetNumAtts();
		numAttsR = bufferR->GetNumAtts();
		attsToKeep = new int[numAttsL + numAttsR];
		for (int i = 0; i < numAttsL; i++)
			attsToKeep[i] = i;
		for (int i = 0; i < numAttsR; i++)
			attsToKeep[i + numAttsL] = i;
	}
	while (nextAvailable)
	{
		int compResult = comparisonEngine.Compare(bufferL, sortOrderL, bufferR, sortOrderR);
		if (compResult == 0)
		{

			while (true)
			{
				temp = new Record();
				temp->Consume(bufferL);
				left_vector.push_back(temp);
				if (!sortedOutPipeL.Remove(bufferL))
				{
					nextAvailable = false;
					break;
				}
				if (comparisonEngine.Compare(bufferL, temp, sortOrderL) != 0)
				{
					break;
				}
			}
			while (true)
			{
				temp = new Record();
				temp->Consume(bufferR);
				right_vector.push_back(temp);
				if (!sortedOutPipeR.Remove(bufferR))
				{
					nextAvailable = false;
					break;
				}
				if (comparisonEngine.Compare(bufferR, temp, sortOrderR) != 0)
				{
					break;
				}
			}
			for (Record *recl : left_vector)
			{
				for (Record *recr : right_vector)
				{

					mergedRecord->MergeRecords(recl, recr, numAttsL, numAttsR,
											   attsToKeep, numAttsL + numAttsR, numAttsL);
					outPipe->Insert(mergedRecord);
				}
			}
			for (Record *recl : left_vector)
			{
				delete recl;
			}
			left_vector.clear();
			for (Record *recr : right_vector)
			{
				delete (recr);
			}
			right_vector.clear();
		}
		else if (compResult > 0)
		{
			nextAvailable = sortedOutPipeR.Remove(bufferR);
		}
		else if (compResult < 0)
		{
			nextAvailable = sortedOutPipeL.Remove(bufferL);
		}
	}

	while (sortedOutPipeL.Remove(bufferL))
		;

	while (sortedOutPipeR.Remove(bufferR))
		;
}

void Join::BlockNestedLoopJoin()
{
	char *fileName = new char[100];
	sprintf(fileName, "dbfile_join_%d.bin", thread);
	DBFile tempFile;
	tempFile.Open(fileName);
	while (inPipeL->Remove(bufferL))
		tempFile.Add(*bufferL);

	int numAttsL = 0;
	int numAttsR = 0;
	int *attsToKeep;
	bool shouldCalculateNumAttr = true;
	ComparisonEngine comparisonEngine;
	while (inPipeR->Remove(bufferR))
	{
		tempFile.MoveFirst();
		while (tempFile.GetNext(*bufferL))
		{

			if (shouldCalculateNumAttr)
			{
				shouldCalculateNumAttr = false;
				numAttsL = bufferL->GetNumAtts();
				numAttsR = bufferR->GetNumAtts();
				attsToKeep = new int[numAttsL + numAttsR];
				for (int i = 0; i < numAttsL; i++)
					attsToKeep[i] = i;
				for (int i = 0; i < numAttsR; i++)
					attsToKeep[i + numAttsL] = i;
			}
			if (comparisonEngine.Compare(bufferL, bufferR, literal, selOp))
			{
				mergedRecord->MergeRecords(bufferL, bufferR, numAttsL, numAttsR,
										   attsToKeep, numAttsL + numAttsR, numAttsL);
				outPipe->Insert(mergedRecord);
			}
		}
	}

	tempFile.Remove(fileName);
}

void Join::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void Join::Use_n_Pages(int n)
{
	runLength = n;
}

// ***************************************************************************** //

void *DuplicateRemoval::EntryPoint(void *arg)
{
	DuplicateRemoval *thisPtr = (DuplicateRemoval *)arg;
	thisPtr->Execute();
	return nullptr;
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->mySchema = &mySchema;

	this->buffer = new Record();
	runLength = 10;

	int rc = pthread_create(&thread, NULL, EntryPoint, (void *)this);
	if (rc)
	{
		printf("ERROR; return code from SelectPipe pthread_create() is %d\n", rc);
		exit(-1);
	}
}

void DuplicateRemoval::Execute()
{
	int BUFF_SIZE = 100;
	Pipe outBigQPipe(BUFF_SIZE);
	OrderMaker sortOrder(mySchema);
	BigQ bigQ(*inPipe, outBigQPipe, sortOrder, runLength);

	ComparisonEngine comparisonEngine;
	Record *prevRecord = new Record();

	if (outBigQPipe.Remove(buffer))
	{
		prevRecord->Copy(buffer);
		outPipe->Insert(buffer);

		while (outBigQPipe.Remove(buffer))
		{
			if (comparisonEngine.Compare(prevRecord, buffer, &sortOrder) != 0)
			{
				prevRecord->Copy(buffer);
				outPipe->Insert(buffer);
			}
		}
	}
	outPipe->ShutDown();
}

void DuplicateRemoval::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int n)
{
	runLength = n;
}

// ***************************************************************************** //

void *Sum::EntryPoint(void *arg)
{
	Sum *thisPtr = (Sum *)arg;

	thisPtr->Execute();
	return nullptr;
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->computeMe = &computeMe;

	this->buffer = new Record();
	runLength = 8;

	int rc = pthread_create(&thread, NULL, EntryPoint, (void *)this);
	if (rc)
	{
		printf("ERROR; return code from SelectPipe pthread_create() is %d\n", rc);
		exit(-1);
	}
}

void Sum::Execute()
{
	ComparisonEngine comparisonEngine;
	int intResult = 0;
	double doubleResult = 0.0;
	Type returnType;
	int intFinalResult = 0;
	double doubleFinalResult = 0.0;
	while (inPipe->Remove(buffer))
	{
		returnType = computeMe->Apply(*buffer, intResult, doubleResult);
		if (returnType == Int)
		{
			intFinalResult += intResult;
		}
		else if (returnType == Double)
		{
			doubleFinalResult += doubleResult;
		}
	}
	string resultString = "";
	if (returnType == Int)
	{
		resultString += to_string(intFinalResult) + "|";
	}
	else if (returnType == Double)
	{
		resultString += to_string(doubleFinalResult) + "|";
	}

	char *sumaAtrName = "sum";
	char *sumSchemaName = "sumSchema";
	Attribute sumAttribute = {sumaAtrName, returnType};
	Schema sumSchema(sumSchemaName, 1, &sumAttribute);
	buffer->ComposeRecord(&sumSchema, (char *)resultString.c_str());
	outPipe->Insert(buffer);
	outPipe->ShutDown();
}

void Sum::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void Sum::Use_n_Pages(int n)
{
	runLength = n;
}

// ***************************************************************************** //

void *GroupBy::EntryPoint(void *arg)
{
	GroupBy *thisPtr = (GroupBy *)arg;

	thisPtr->Execute();
	return nullptr;
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->computeMe = &computeMe;

	runLength = 10;

	int rc = pthread_create(&thread, NULL, EntryPoint, (void *)this);
	if (rc)
	{
		printf("ERROR; return code from SelectPipe pthread_create() is %d\n", rc);
		exit(-1);
	}
}

void GroupBy::Execute()
{
	int BUFF_SIZE = 100;
	Pipe outBigQPipe(BUFF_SIZE);
	BigQ bigQ(*inPipe, outBigQPipe, *groupAtts, runLength);
	ComparisonEngine comparisonEngine;

	Record *current = new Record();
	Record *prev = new Record();
	Record *sumRecord = new Record();
	Record *outRecord = new Record();

	char *sumaAtrName = "sum";
	char *sumSchemaName = "sumSchema";
	Type returnType = computeMe->GetReturnType();
	Attribute sumAttribute = {sumaAtrName, returnType};
	Schema sumSchema(sumSchemaName, 1, &sumAttribute);

	bool shouldIterate = true;
	if (!outBigQPipe.Remove(current))
		shouldIterate = false;

	int numAttsRecord;
	int numGroupAtts;
	int *groupAttsIndexes;
	int *attsToKeep;
	if (shouldIterate)
	{
		numAttsRecord = current->GetNumAtts();
		numGroupAtts = groupAtts->GetNumAtts();
		groupAttsIndexes = groupAtts->GetWhichAtts();
		attsToKeep = new int[1 + numGroupAtts];
		attsToKeep[0] = 0;
		for (int i = 0; i < numGroupAtts; i++)
			attsToKeep[i + 1] = groupAttsIndexes[i];
	}

	int finalIntSum = 0;
	double finalDoubleSum = 0.0;

	while (shouldIterate)
	{
		int intSum = 0;
		double doubleSum = 0.0;
		computeMe->Apply(*current, intSum, doubleSum);
		if (returnType == Int)
			finalIntSum += intSum;
		else if (returnType == Double)
			finalDoubleSum += doubleSum;

		prev->Consume(current);
		shouldIterate = outBigQPipe.Remove(current);

		if (shouldIterate && comparisonEngine.Compare(current, prev, groupAtts) != 0)
		{
			char charsRes[100];
			if (returnType == Int)
			{
				sprintf(charsRes, "%d|", finalIntSum);
			}
			else if (returnType == Double)
			{
				sprintf(charsRes, "%lf|", finalDoubleSum);
			}
			sumRecord->ComposeRecord(&sumSchema, charsRes);
			outRecord->MergeRecords(sumRecord, prev, 1, numAttsRecord, attsToKeep, numGroupAtts + 1, 1);
			outPipe->Insert(outRecord);
			finalIntSum = 0;
			finalDoubleSum = 0.0;
		}
	}

	char charsRes[100];
	if (returnType == Int)
	{
		sprintf(charsRes, "%d|", finalIntSum);
	}
	else if (returnType == Double)
	{
		sprintf(charsRes, "%lf|", finalDoubleSum);
	}
	sumRecord->ComposeRecord(&sumSchema, charsRes);
	outRecord->MergeRecords(sumRecord, prev, 1, numAttsRecord, attsToKeep, numGroupAtts + 1, 1);
	outPipe->Insert(outRecord);

	outPipe->ShutDown();

}

void GroupBy::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void GroupBy::Use_n_Pages(int n)
{
	runLength = n;
}

// ***************************************************************************** //

void *WriteOut::EntryPoint(void *arg)
{
	WriteOut *thisPtr = (WriteOut *)arg;

	thisPtr->Execute();
	return nullptr;
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema)
{
	this->inPipe = &inPipe;
	this->outFile = outFile;
	this->mySchema = &mySchema;

	this->buffer = new Record();
	runLength = 8;

	int rc = pthread_create(&thread, NULL, EntryPoint, (void *)this);
	if (rc)
	{
		printf("ERROR; return code from SelectPipe pthread_create() is %d\n", rc);
		exit(-1);
	}
}

void WriteOut::Execute()
{
	ComparisonEngine comparisonEngine;
	while (inPipe->Remove(buffer))
	{
		fputs(buffer->GetString(mySchema).c_str(), outFile);
		// buffer->Print(mySchema);
	}
}

void WriteOut::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void WriteOut::Use_n_Pages(int n)
{
	runLength = n;
}

// ***************************************************************************** //