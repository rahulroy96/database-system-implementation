#include <algorithm>
#include <queue>
#include "BigQ.h"

Run ::Run(int runlen, off_t startPtr, off_t endPtr, File *file, OrderMaker *sortOrder)
{
	this->runlen = runlen;
	this->startPtr = startPtr;
	this->file = file;
	this->currentPtr = startPtr;
	this->sortOrder = sortOrder;
	this->endPtr = endPtr;

	head = new Record();
	currentPage = new Page();
	comparisonEngine = new ComparisonEngine();
}
Run ::~Run()
{
	delete currentPage;
	delete comparisonEngine;
	delete head;
}

void Run::MoveFirst()
{
	currentPtr = startPtr;
}

int Run::GetNext()
{
	if (!currentPage->GetFirst(head))
	{
		if (currentPtr + 1 > endPtr || currentPtr + 1 >= file->GetLength())
		{
			return 0;
		}

		file->GetPage(currentPage, currentPtr++);
		return currentPage->GetFirst(head);
	}
	return 1;
}

void Run::GetHead(Record &rec)
{
	return rec.Consume(head);
}

// Returns the number of pages used to write
int Run::SortWrite(vector<Record *> &records)
{
	// Sort the current record list in ascending order.
	sort(records.begin(), records.end(), [this](Record *left, Record *right)
		 { return comparisonEngine->Compare(left, right, sortOrder) < 0; });

	int writePtr = startPtr;
	// Write the records to page
	for (auto &rec : records)
	{
		// If page is full write page to file and start new page.
		if (!currentPage->Append(rec))
		{
			file->AddPage(currentPage, writePtr++);
			currentPage->EmptyItOut();
			currentPage->Append(rec);
		}
	}

	// Add the last page which was not full into file.
	file->AddPage(currentPage, writePtr++);
	endPtr = writePtr;
	currentPage->EmptyItOut();

	return endPtr - startPtr;
}

void *worker(void *arg)
{
	BigQ *thisPtr = (BigQ *)arg;

	thisPtr->RunFirstPhase();
	thisPtr->RunSecondPhase();

	return nullptr;
}

BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
{

	this->input = &in;
	this->output = &out;
	this->sortorder = &sortorder;
	this->runlen = runlen;

	file = new File();
	file->Open(0, (char *)fileName);

	pthread_t thread;
	comparisonEngine = new ComparisonEngine();
	int rc = pthread_create(&thread, NULL, worker, (void *)this);
	if (rc)
	{
		printf("ERROR; return code from pthread_create() is %d\n", rc);
		exit(-1);
	}
}

BigQ::~BigQ()
{
	file->Close();
	delete file;
	delete comparisonEngine;
}

void BigQ ::RunFirstPhase()
{
	// Initialize the records vector
	vector<Record *> records;

	int countPage = 0;
	int currentPageSize = sizeof(int);
	off_t currentStartPage = 0;
	off_t numberOfPages = 0;
	// Run run(runlen, currentStartPage, runlen, file, sortorder);

	Run *tempRun;

	while (true)
	{
		Record *rec = new Record;

		// If no more records in the input pipe break
		if (!input->Remove(rec))
			break;

		// If no space write the run into memory and start new
		// Keep track of the start of each run in runIndices
		if (currentPageSize + rec->GetSize() > PAGE_SIZE && countPage + 1 >= runlen)
		{
			// Create a new run
			tempRun = new Run(runlen, currentStartPage, runlen, file, sortorder);

			// Sort the records and write the page to file.
			// Update the currentStartPage
			numberOfPages = tempRun->SortWrite(records);
			runs.push_back(tempRun);
			currentStartPage += numberOfPages;

			// Free the memory allocated for records
			for (auto &r : records)
				delete r;

			// clear the records array
			records.clear();

			currentPageSize = sizeof(int);
			countPage = 0;
		}
		else if (currentPageSize + rec->GetSize() > PAGE_SIZE && countPage + 1 < runlen)
		{
			// If there is enough space for one more page
			// increment the page count and set its size to zero.
			countPage++;
			currentPageSize = sizeof(int);
		}

		// Insert the record to the vector and update the page size
		records.push_back(rec);
		currentPageSize += rec->GetSize();
	}

	// Write the last run to file
	tempRun = new Run(runlen, currentStartPage, runlen, file, sortorder);
	numberOfPages = tempRun->SortWrite(records);
	runs.push_back(tempRun);

	// delete the records and clear the records vector
	for (auto &r : records)
		delete r;
	records.clear();
}

void BigQ ::RunSecondPhase()
{
	auto comparator = [this](Run *left, Run *right)
	{
		return comparisonEngine->Compare(left->head, right->head, sortorder) >= 0;
	};

	// Create the priority queue with comparator function
	// comparing the head of the run using given sort order.
	priority_queue<Run *, vector<Run *>, decltype(comparator)> pq(comparator);

	for (auto run : runs)
	{
		// Move to the first record in the run.
		run->MoveFirst();

		// Fetch the first record into the head.
		run->GetNext();

		// Push run into the pq.
		pq.push(run);
	}
	runs.clear();

	Record rec;
	Run *tempRun;
	while (!pq.empty())
	{

		// Get the run from the top of the queue.
		tempRun = pq.top();

		// Remove the run which is at the top
		pq.pop();

		// Copy the head record to record
		tempRun->GetHead(rec);

		// Insert the record to the output pipe
		output->Insert(&rec);

		// Check if there is any more records in this run
		if (tempRun->GetNext())
		{
			// There are more records so push it back to pq
			pq.push(tempRun);
		}
		else
		{
			// There are no more records in this run
			// Delete this run and free the memory.
			delete tempRun;
		}
	}

	// Delete the temperory file
	remove((char *)file);

	// Close the output pipe
	output->ShutDown();
}
