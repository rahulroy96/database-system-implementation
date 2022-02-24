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
        if (currentPtr + 1 >= startPtr + runlen)
        {
            return 0;
        }

        file->GetPage(currentPage, currentPtr++);
        return currentPage->GetFirst(head);
    }
    return 1;
}

void Run::GetHead(Record &rec){
	return rec.Consume(head);
}

void Run::SortWrite(vector<Record *> &records, off_t startPtr)
{

	// Sort the current record list in ascending order.
	sort(records.begin(), records.end(), [this](Record *left, Record *right)
		 { return comparisonEngine->Compare(left, right, sortOrder) < 0; });
	this->startPtr = startPtr;
	for (Record *rec : records)
	{
		if (!currentPage->Append(rec))
		{
			file->AddPage(currentPage, startPtr++);
			currentPage->EmptyItOut();
			currentPage->Append(rec);
		}
		delete rec;
	}
	file->AddPage(currentPage, startPtr++);
	currentPage->EmptyItOut();
	records.clear();
}

void *worker(void *arg)
{
	BigQ *thisPtr = (BigQ *)arg;

	thisPtr->RunFirstPhase();
	thisPtr->RunSecondPhase();
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

	Page *page = new Page();
	vector<Record *> records;
	int capacity = runlen * PAGE_SIZE;
	int current_size = 0;
	off_t current_page = 0;
	Run *run = new Run(runlen, current_page, file, sortorder);
	while (true)
	{
		Record *rec = new Record;
		if (!input->Remove(rec))
		{
			break;
		}
		if (current_size + rec->GetSize() < capacity)
		{
			records.push_back(rec);
			current_size += rec->GetSize();
		}
		else
		{
			runIndices.push_back(current_page);
			run->SortWrite(records, current_page);

			records.push_back(rec);
			current_size = rec->GetSize();
			current_page += runlen;
		}
	}
	runIndices.push_back(current_page);
	run->SortWrite(records, current_page);
}

void BigQ ::RunSecondPhase()
{
	auto comparator = [this](Run *left, Run *right) {
        return comparisonEngine->Compare(left->head, right->head, sortorder) >=0;
    };
	priority_queue<Run*, vector<Run*>, decltype(comparator)> pq(comparator);

	Run * tempRun;
	for(auto startId: runIndices){
		
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
	while(!pq.empty()){

		// Get the run from the top of the queue.
		tempRun = pq.top();

		// Remove the run which is at the top
		pq.pop();

		// Copy the head record to record
		tempRun->GetHead(rec);

		// Insert the record to the output pipe
		output->Insert(&rec);

		// Check if there is any more records in this run
		if(tempRun->GetNext()){
			// There are more records so push it back to pq
			pq.push(tempRun);
		}
		else{
			// There are no more records in this run
			// Delete this run and free the memory.
			delete tempRun;
		}
	}

	output->ShutDown();
}
