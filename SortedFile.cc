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
    state = reading;
}

SortedFile::~SortedFile()
{
    delete file;
    // delete bq;
    // delete input;
    // delete output;
    delete readPage;
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
    // string sortOrderString = sortInfo->sortOrder->toString();
    // sortOrder = new OrderMaker(sortOrderString);
    sortOrder = sortInfo->sortOrder;

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
    oFile << sortOrder->toString() << endl;
    oFile.close();
    return 1;
}

int SortedFile::Open(const char *f_path)
/*
 *  Open the file.
 *  Read the SortOrder and RunLength from the meta file
 */
{
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

    // sortOrder->Print();
    // cout << sortOrder->toString();

    // Passing 0 as the size of file to Open will create the file.
    file->Open(1, (char *)f_path);
    return 1;
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

    // Change state to writing and initialize the pipes and bigq for writing
    if (state == reading)
        InitializeWriting();

    // Insert the record to the input pipe
    Record recToInsert;
    recToInsert.Consume(&rec);
    input->Insert(&recToInsert);
}

int SortedFile::Close()
/*
 *  Closes the file.
 *  Merge the bigQ if the state is writing.
 */
{
    if (state == writing)
        MergeBigQ();

    return file->Close();
}

void SortedFile::MoveFirst()
/*
 *  Move the read pointer to point to first page
 *  Read the first page
 */
{
    if (state == writing)
        MergeBigQ();

    readPtr = 0;
    readPage->EmptyItOut();
}

int SortedFile::GetNext(Record &fetchme)
/*
 *  Check if current state is reading, if not merge the records in bigQ
 *  Populate fetchMe with the next record from the current readPage
 *  if no record available in current, read the next page
 *  and return the first record. if no more pages left return 0
 *  If writePage is dirty and no other pages left to read,
 *  write it to file
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
 *  Repeatedly fetch records from the page till match is found.
 *  Return 1 when comparison engine accepts a record.
 *  if no more records left return 0.
 */
{

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
}

void SortedFile::MergeBigQ()
/*
 *  Change the state to reading and shutdown the input pipe
 *  Initialize a new instance of file and merge the records from
 *  the output pipe with records already in the file.
 *  Delete the old instance and make the new instance the file.
 */
{
    // Verify tha current state is writing.
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
    string mergeFileName = fileName.append(".merge");
    mergeFile.Create(mergeFileName.c_str(), heap, nullptr);

    // Read the first record from the output pipe.
    Record pipeRecord;
    int pipeHasRecord = output->Remove(&pipeRecord);

    // Read the first record already in the file.
    MoveFirst();
    Record fileRecord;
    int fileHasRecord = GetNext(fileRecord);

    ComparisonEngine comparisonEngine;

    // Loop until there are records in both the file and output pipe
    while (pipeHasRecord && fileHasRecord)
    {
        // if file records is smaller in given sortorder insert it to mergefile
        // read the next record from the file
        if (comparisonEngine.Compare(&fileRecord, &pipeRecord, sortOrder) < 0)
        {
            mergeFile.Add(fileRecord);
            fileHasRecord = GetNext(fileRecord);
        }
        else
        {
            mergeFile.Add(pipeRecord);
            pipeHasRecord = output->Remove(&pipeRecord);
        }
    }

    // Insert the remaining records in the pipe to mergeFile
    while (pipeHasRecord)
    {
        mergeFile.Add(pipeRecord);
        pipeHasRecord = output->Remove(&pipeRecord);
    }

    // Insert the remaining records in file to the mergeFile
    while (fileHasRecord)
    {
        mergeFile.Add(fileRecord);
        fileHasRecord = GetNext(fileRecord);
    }

    // Move the read pointer back to first record.
    MoveFirst();

    // Close the merge file.
    // Remove old file and rename new file with original filename.
    mergeFile.Close();
    remove(fileName.c_str());
    rename(mergeFileName.c_str(), fileName.c_str());

    // Delete the pipes and queue.
    delete bq;
    delete input;
    delete output;
}
