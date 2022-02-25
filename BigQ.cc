#include <algorithm>
#include <queue>
#include "BigQ.h"

Run ::Run(int runlen, off_t startPtr, File *file, OrderMaker *sortOrder)
{
	this->runlen = runlen;
	this->startPtr = startPtr;
	this->file = file;
	this->currentPtr = startPtr;
	this->sortOrder = sortOrder;

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
		if (currentPtr + 1 >= startPtr + runlen || currentPtr + 1 >= file->GetLength())
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

void Run::SortWrite(vector<Record *> &records, off_t startPtr)
{
	// Sort the current record list in ascending order.
	sort(records.begin(), records.end(), [this](Record *left, Record *right)
		 { return comparisonEngine->Compare(left, right, sortOrder) < 0; });
	this->startPtr = startPtr;

	// Write the recordds to page
	for (auto &rec : records)
	{
		// If page is full write page to file and start new page.
		if (!currentPage->Append(rec))
		{
			file->AddPage(currentPage, startPtr++);
			currentPage->EmptyItOut();
			currentPage->Append(rec);
		}
	}

	// Add the last page which was not full into file.
	file->AddPage(currentPage, startPtr++);
	currentPage->EmptyItOut();
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

	// Capacity in bytes of run is runlength times PAGE_SIZE
	int capacity = runlen * PAGE_SIZE;
	int current_size = 0;
	off_t current_page = 0;
	Run run(runlen, current_page, file, sortorder);
	while (true)
	{
		Record *rec = new Record;

		// If no more records in the input pipe break
		if (!input->Remove(rec))
			break;

		// If there is space for the record add the record to
		// records vector and update the capacity.
		if (current_size + rec->GetSize() < capacity)
		{
			records.push_back(rec);
			current_size += rec->GetSize();
		}
		else
		{
			// If no space write the run into memory and start new
			// Keep track of the start of each run in runIndices
			runIndices.push_back(current_page);

			// Sort the records and write the page to file.
			run.SortWrite(records, current_page);

			// delete the records and clear the array
			for (auto &r : records)
				delete r;
			records.clear();

			// Write the current record to the array.
			records.push_back(rec);
			current_size = rec->GetSize();
			current_page += runlen;
		}
	}

	// Write the last run to file
	runIndices.push_back(current_page);
	run.SortWrite(records, current_page);

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

	Run *tempRun;
	for (auto startId : runIndices)
	{

		// Create a new instance of the run with the start id.
		tempRun = new Run(runlen, startId, file, sortorder);

		// Move to the first record in the run.
		tempRun->MoveFirst();

		// Fetch the first record into the head.
		tempRun->GetNext();

		// Push run into the pq.
		pq.push(tempRun);
	}

	Record rec;
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

	// Close the output pipe
	output->ShutDown();
}
