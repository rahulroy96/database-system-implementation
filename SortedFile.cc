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

using namespace std;

SortedFile::SortedFile()
{
    file = new File();
    readPage = new Page();
    writePage = new Page();
    readPtr = 0;
    // writePtr = 0;
    // writePageDirty = false;
    // comparisonEngine = new ComparisonEngine();
    myOrder = nullptr;
    runLength = 0;
    // sortInfo = nullptr;
}

SortedFile::~SortedFile()
{
    delete file;
    // delete readPage;
    // delete writePage;
    // delete comparisonEngine;
}

int SortedFile::Create(const char *f_path, void *startup)
/*
    Open the file.
    Since its a newly created file, set the writePtr & readPtr to 0.
    Set the dirty flag to false.
*/
{
    // Passing 0 as the size of file to Open will create the file.
    file->Open(0, (char *)f_path);
    SortInfo *sortInfo = (struct SortInfo *)startup;

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
    oFile << sortInfo->runLength << endl;
    oFile << sortInfo->myOrder->toString()<<endl;
    oFile.close();
    return 1;
}

void SortedFile::Load(Schema &f_schema, const char *loadpath)
/*
    Bulk load data from file located at path loadpath
*/
{
    FILE *tableFile = fopen(loadpath, "r");
    if (tableFile == nullptr)
    {
        cout << "ERROR: Unable to open table file: " << loadpath << endl;
        exit(1);
    }

    Record rec;
    while (rec.SuckNextRecord(&f_schema, tableFile))
    {
        this->Add(rec);
    }
    fclose(tableFile);
}

int SortedFile::Open(const char *f_path)
/*
    Open the file.
    Set the readptr to the first record in the file.
    Set the writeptr to end of the file.
    Empty Out the write page and set the dirty flag to false.
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
    iFile >> data;
    // cout << data << endl;
    iFile >> data;
    runLength = atoi(data.c_str());
    // cout << runLength << endl;
    iFile >> data;
    myOrder = new OrderMaker(data);
    // cout << myOrder->toString() << endl;
    iFile.close();

    // Passing 0 as the size of file to Open will create the file.
    file->Open(1, (char *)f_path);
    cout << "Length of Run: " << runLength << endl;
    return 1;
}

void SortedFile::MoveFirst()
/*
    Move the read pointer to point to first page
    Read the first page
*/
{
    // readPtr = 0;
    // readPage->EmptyItOut();
}

int SortedFile::Close()
/*
    Closes the file.
    Add the writePage to file if it is dirty.
*/
{
    // if (writePageDirty)
    // {
    //     file->AddPage(writePage, writePtr++);
    //     writePage->EmptyItOut();
    // }

    return file->Close();
}

void SortedFile::Add(Record &rec)
/*
    Checks if there is enough space inside a page to add the record,
    if not, add page to file, empty it out and add the record.
*/
{
    Record recToInsert;
    recToInsert.Consume(&rec);
    // if (!writePage->Append(&recToInsert))
    // {
    //     file->AddPage(writePage, writePtr++);
    //     writePage->EmptyItOut();
    //     writePage->Append(&recToInsert);
    // }
    // writePageDirty = true;
}

int SortedFile::GetNext(Record &fetchme)
/*
    Populate fetchMe with the next record from the current readPage
    if no record available in current, read the next page
    and return the first record. if no more pages left return 0
    If writePage is dirty and no other pages left to read,
    write it to file
*/
{
    // if (!readPage->GetFirst(&fetchme))
    // {
    //     if (readPtr + 1 >= file->GetLength() && writePageDirty)
    //     {
    //         file->AddPage(writePage, writePtr++);
    //         writePage->EmptyItOut();
    //     }

    //     if (readPtr + 1 >= file->GetLength())
    //     {
    //         return 0;
    //     }

    //     file->GetPage(readPage, readPtr++);
    //     return readPage->GetFirst(&fetchme);
    // }
    return 1;
}

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
/*
    Repeatedly fetch records from the page till match is found.
    Return 1 when comparison engine accepts a record.
    if no more records left return 0.
*/
{
    // while (this->GetNext(fetchme))
    // {
    //     if (comparisonEngine->Compare(&fetchme, &literal, &cnf) != 0)
    //         return 1;
    // }
    return 0;
}
