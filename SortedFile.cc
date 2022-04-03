#include <string.h>
#include <iostream>
#include <fstream>
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFileInterface.h"
#include "SortedFile.h"
#include "Defs.h"
#include "DBFile.h"

using namespace std;

SortedFile::SortedFile()
{
    file = new File();
    readPage = new Page();
    readPtr = 0;
    sortOrder = nullptr;
    querySortOrder = nullptr;
    queryLiteralOrder = nullptr;
    state = reading;
    comparisonEngine = new ComparisonEngine();

    addCount = 0;
}

SortedFile::~SortedFile()
{
    delete file;
    delete readPage;
    delete comparisonEngine;
    if (sortOrder != nullptr)
        delete sortOrder;
    DeleteQueryOrders();
}

int SortedFile::Create(const char *f_path, void *startup)
/*
 *  Open the file.
 *  Since its a newly created file, set the readPtr to 0.
 *  Write the sortOrder and runLength to meta file
 */
{
    // Passing 0 as the size of file to Open will create the file.
    file->Open(0, (char *)f_path);
    fileName = string(f_path);
    readPtr = 0;
    readPage->EmptyItOut();
    SortInfo *sortInfo = (struct SortInfo *)startup;
    runLength = sortInfo->runLength;
    sortOrder = new OrderMaker(sortInfo->sortOrder->ToString());

    // Write filetype to the metadata folder for persistance.
    string metaDataPath = string(f_path);
    metaDataPath.append(".metadata");
    ofstream oFile;
    oFile.open(metaDataPath.c_str());

    if (!oFile.is_open())
    {
        cout << "ERROR: Cant Create meta data file: " << metaDataPath << endl;
        exit(1);
    }
    oFile << sorted << endl;
    oFile << runLength << endl;
    oFile << sortOrder->ToString() << endl;
    oFile.close();
    return 1;
}

int SortedFile::Open(const char *f_path)
/*
 *  Open the file.
 *  Read the SortOrder and RunLength from the meta file
 */
{
    // Open the meta data file
    string metaDataPath = string(f_path);
    metaDataPath.append(".metadata");
    ifstream iFile;
    iFile.open(metaDataPath.c_str());
    if (!iFile.is_open())
    {
        cout << "ERROR: Cant open meta data file: " << metaDataPath << endl;
        exit(1);
    }

    string data;

    // Read the file type and ignore it
    iFile >> data;

    // Read the run Length from the meta file
    iFile >> data;
    runLength = atoi(data.c_str());

    // Read the sortOrder from the meta file
    // and create an instance of OrderMaker out of the string.
    iFile >> data;
    sortOrder = new OrderMaker(data);
    iFile.close();

    // Passing 1 as the size of file so that new file wont be created.
    file->Open(1, (char *)f_path);
    fileName = string(f_path);
    DeleteQueryOrders();
    return 1;
}

int SortedFile::Close()
/*
 *  Closes the file.
 *  Merge the bigQ if the state is writing.
 */
{
    // cout << "addcount: " << addCount << endl;
    if (state == writing)
        MergeBigQ();

    return file->Close();
}

void SortedFile::Load(Schema &f_schema, const char *loadpath)
/*
 *  Bulk load data from file located at path loadpath.
 *  If the current state is reading initialize for writing.
 */
{
    // Change state to writing and initialize the pipes and bigq for writing
    if (state == reading)
        InitializeWriting();

    // Open the file located at the loadpath and check for error
    FILE *tableFile = fopen(loadpath, "r");
    if (tableFile == nullptr)
    {
        cout << "ERROR: Unable to open table file: " << loadpath << endl;
        exit(1);
    }

    // Read records one by one and insert to the input pipe
    Record recToInsert;
    while (recToInsert.SuckNextRecord(&f_schema, tableFile))
        input->Insert(&recToInsert);

    fclose(tableFile);
}

void SortedFile::Add(Record &rec)
/*
 *  If current state is reading change state to writing and
 *  initialize the internal input pipe, output pipe and bigQ.
 *  Add the record to the input pipe.
 */
{
    addCount++;
    // Change state to writing and initialize the pipes and bigq for writing
    if (state == reading)
        InitializeWriting();

    // Insert the record to the input pipe
    Record recToInsert;
    recToInsert.Consume(&rec);
    input->Insert(&recToInsert);
}

void SortedFile::MoveFirst()
/*
 *  Move the read pointer to point to first page
 *  Read the first page
 *  If current state is writing, Merge the records in output pipe
 */
{
    if (state == writing)
        MergeBigQ();

    DeleteQueryOrders();

    readPtr = 0;
    readPage->EmptyItOut();
}

int SortedFile::GetNext(Record &fetchme)
/*
 *  Check if current state is reading, if not merge the records in bigQ
 *  Populate fetchMe with the next record from the current readPage
 *  if no record available in current, read the next page
 *  and return the first record. if no more pages left return 0
 */
{
    if (state != reading)
        MergeBigQ();

    if (!readPage->GetFirst(&fetchme))
    {
        if (readPtr + 1 >= file->GetLength())
            return 0;

        file->GetPage(readPage, readPtr++);
        return readPage->GetFirst(&fetchme);
    }
    return 1;
}

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
/*
 *  If state is not reading, mergeBigQ
 *  If the queryOrderMaker is null, build the query OrderMaker
 *  and then do binary search to find the first page.
 *  For subsequent calls, just do linear search till the literal
 *  record is greater than the fetched reord in queryOrder.
 *  if no more records left return 0.
 */
{
    if (state != reading)
        MergeBigQ();

    // If query order is not build, its the first call to
    // this function so do binary search to find the first record
    if (querySortOrder == nullptr)
    {
        // cout << "Building Query Order" << endl;
        querySortOrder = new OrderMaker();
        queryLiteralOrder = new OrderMaker();
        cnf.GetQueryOrders(*sortOrder, *querySortOrder, *queryLiteralOrder);

        // If query order has some attributes do binary search to find the
        // page which might have the record that matches cnf
        if (querySortOrder->GetNumAtts())
        {
            off_t left = 0;
            off_t right = file->GetLength() - 2;

            // Do binary search on the page
            while (left < right - 1)
            {
                // Set the readPtr to
                readPtr = (left + right) / 2;
                file->GetPage(readPage, readPtr);
                readPage->GetFirst(&fetchme);
                int comparisonValue = comparisonEngine->Compare(&fetchme, querySortOrder, &literal, queryLiteralOrder);
                if (comparisonValue == 0)
                    right = readPtr;
                else if (comparisonValue < 0)
                    left = readPtr;
                else
                    right = readPtr - 1;
            }
            readPtr = left;
            file->GetPage(readPage, readPtr++);
        }
    }

    while (GetNext(fetchme))
    {

        // If the record mathes the given cnf return 1.
        if (comparisonEngine->Compare(&fetchme, &literal, &cnf) != 0)
            return 1;

        // If the literal record is greater than the fetched
        // record in query order return 0
        if (querySortOrder->GetNumAtts() &&
            comparisonEngine->Compare(&fetchme, querySortOrder, &literal, queryLiteralOrder) > 0)
            return 0;
    }

    return 0;
}

void SortedFile::InitializeWriting()
/*
 *  Initialize the pipes and bigq for writing
 *  Set the state to writing.
 */
{
    state = writing;
    input = new Pipe(BUFF_SIZE);
    output = new Pipe(BUFF_SIZE);
    bq = new BigQ(*input, *output, *sortOrder, runLength);
    DeleteQueryOrders();
}

void SortedFile::DeleteQueryOrders()
{
    if (querySortOrder != nullptr)
    {
        delete querySortOrder;
        querySortOrder = nullptr;
    }
    if (queryLiteralOrder != nullptr)
    {
        delete queryLiteralOrder;
        queryLiteralOrder = nullptr;
    }
}

void SortedFile::MergeBigQ()
/*
 *  Change the state to reading and shutdown the input pipe
 *  Initialize a new instance of file and merge the records from
 *  the output pipe with records already in the file.
 *  Delete the old instance and make the new instance the file.
 */
{
    // Verify that current state is writing.
    if (state != writing)
        return;

    // Change the state to reading. This is done in the begining so that
    // internally calling GetNext and MoveFirst doesnt lead to infinite loop
    state = reading;

    // Shutdown the input pipe.
    input->ShutDown();

    // Create the new file instance as heap file
    // since we are inserting one by one.
    DBFile mergeFile;
    string mergeFileName = fileName + ".merge";
    mergeFile.Create(mergeFileName.c_str(), heap, nullptr);

    // Read the first record from the output pipe.
    Record pipeRecord;
    int pipeHasRecord = output->Remove(&pipeRecord);

    // Read the first record already in the file.
    MoveFirst();
    Record fileRecord;
    int fileHasRecord = GetNext(fileRecord);
    int count = 0;
    int pipeCount = 0;
    int fileCount = 0;

    // Loop until there are records in both the file and output pipe
    while (pipeHasRecord && fileHasRecord)
    {
        count++;
        // if file records is smaller in given sortorder insert it to mergefile
        // read the next record from the file
        if (comparisonEngine->Compare(&fileRecord, &pipeRecord, sortOrder) < 0)
        {
            fileCount++;
            mergeFile.Add(fileRecord);
            fileHasRecord = GetNext(fileRecord);
        }
        else
        {
            pipeCount++;
            mergeFile.Add(pipeRecord);
            pipeHasRecord = output->Remove(&pipeRecord);
        }
    }

    // Insert the remaining records in the pipe to mergeFile
    while (pipeHasRecord)
    {
        pipeCount++;
        count++;
        mergeFile.Add(pipeRecord);
        pipeHasRecord = output->Remove(&pipeRecord);
    }

    // Insert the remaining records in file to the mergeFile
    while (fileHasRecord)
    {
        fileCount++;
        count++;
        mergeFile.Add(fileRecord);
        fileHasRecord = GetNext(fileRecord);
    }

    // Move the read pointer back to first record.
    MoveFirst();

    // Close the merge file.
    // Remove old file and rename new file with original filename.
    mergeFile.Close();
    file->Close();
    remove(fileName.c_str());
    rename(mergeFileName.c_str(), fileName.c_str());
    file->Open(1, (char *)fileName.c_str());

    // Delete the pipes and queue.
    delete bq;
    delete input;
    delete output;
}
