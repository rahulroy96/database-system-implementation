#include <string.h>
#include <iostream>
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFileInterface.h"
#include "HeapFile.h"
#include "Defs.h"

using namespace std;

HeapFile::HeapFile()
{
    file = new File();
    readPage = new Page();
    writePage = new Page();
    readPtr = 0;
    writePtr = 0;
    writePageDirty = false;
    comparisonEngine = new ComparisonEngine();
}

HeapFile::~HeapFile()
{
    delete file;
    delete readPage;
    delete writePage;
    delete comparisonEngine;
}

int HeapFile::Create(const char *f_path, void *startup)
/*
    Open the file.
    Since its a newly created file, set the writePtr & readPtr to 0.
    Set the dirty flag to false.
*/
{
    // Passing 0 as the size of file to Open will create the file.
    file->Open(0, (char *)f_path);
    readPtr = 0;
    writePtr = 0;
    writePageDirty = false;
    return 1;
}

void HeapFile::Load(Schema &f_schema, const char *loadpath)
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

int HeapFile::Open(const char *f_path)
/*
    Open the file.
    Set the readptr to the first record in the file.
    Set the writeptr to end of the file.
    Empty Out the write page and set the dirty flag to false.
*/
{
    // Passing 0 as the size of file to Open will create the file.
    file->Open(1, (char *)f_path);
    cout << "Length of opened file: " << file->GetLength() << endl;
    readPtr = 0;
    readPage->EmptyItOut();
    writePtr = file->GetLength();
    writePage->EmptyItOut();
    writePageDirty = false;
    return 1;
}

void HeapFile::MoveFirst()
/*
    Move the read pointer to point to first page
    Read the first page
*/
{
    readPtr = 0;
    readPage->EmptyItOut();
}

int HeapFile::Close()
/*
    Closes the file. 
    Add the writePage to file if it is dirty.
*/
{
    if (writePageDirty)
    {
        file->AddPage(writePage, writePtr++);
        writePage->EmptyItOut();
    }

    return file->Close();
}

void HeapFile::Add(Record &rec)
/*
    Checks if there is enough space inside a page to add the record, 
    if not, add page to file, empty it out and add the record.
*/
{
    Record recToInsert;
    recToInsert.Consume(&rec);
    if (!writePage->Append(&recToInsert))
    {
        file->AddPage(writePage, writePtr++);
        writePage->EmptyItOut();
        writePage->Append(&recToInsert);
    }
    writePageDirty = true;
}

int HeapFile::GetNext(Record &fetchme)
/*
    Populate fetchMe with the next record from the current readPage
    if no record available in current, read the next page 
    and return the first record. if no more pages left return 0
    If writePage is dirty and no other pages left to read, 
    write it to file 
*/
{
    if (!readPage->GetFirst(&fetchme))
    {
        if (readPtr + 1 >= file->GetLength() && writePageDirty)
        {
            file->AddPage(writePage, writePtr++);
            writePage->EmptyItOut();
        }

        if (readPtr + 1 >= file->GetLength())
        {
            return 0;
        }

        file->GetPage(readPage, readPtr++);
        return readPage->GetFirst(&fetchme);
    }
    return 1;
}

int HeapFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
/*
    Repeatedly fetch records from the page till match is found.
    Return 1 when comparison engine accepts a record.
    if no more records left return 0.
*/
{
    while (this->GetNext(fetchme))
    {
        if (comparisonEngine->Compare(&fetchme, &literal, &cnf) != 0)
            return 1;
    }
    return 0;
}
