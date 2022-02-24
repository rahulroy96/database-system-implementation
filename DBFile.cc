#include <iostream>
#include <fstream>
#include <string.h>

#include "DBFile.h"
#include "HeapFile.h"
#include "Defs.h"

using namespace std;

DBFile::DBFile()
{
    file = nullptr;
}

DBFile::~DBFile()
{
    delete file;
}

int DBFile::Create(const char *f_path, fType f_type, void *startup)
{
    if (f_path == nullptr)
        return 0;

    if (f_type == heap)
    {
        file = new HeapFile();
    }
    else if (f_type == sorted)
    {
        // TODO sorted
    }
    else if (f_type == tree)
    {
        // TODO tree
    }
    else
    {
        // filetype not defined
        cout << "ERROR: Invalid filetype\n";
        exit(1);
    }

    // Write filetype to the metadata folder for persistance.
    string metaDataPath = string(f_path);
    metaDataPath.append(".metadata");
    ofstream oFile;
    oFile.open(metaDataPath.c_str());
    // oFile.open("a.txt");
    if (!oFile.is_open())
    {
        cout << "ERROR: Cant Create meta data file: " << metaDataPath << endl;
        exit(1);
    }
    oFile << heap;
    oFile.close();
    // use the create function of the filetype class.
    return file->Create(f_path, startup);
}

void DBFile::Load(Schema &f_schema, const char *loadpath)
{
    file->Load(f_schema, loadpath);
}

int DBFile::Open(const char *f_path)
{
    if (f_path == NULL)
        return 0;
    string metaDataPath = string(f_path);
    metaDataPath.append(".metadata");
    ifstream iFile;
    iFile.open(metaDataPath.c_str());
    if (!iFile.is_open())
    {
        cout << "ERROR: Cant open meta data file: " << metaDataPath << endl;
        exit(1);
    }
    string f_type_string;
    iFile >> f_type_string;
    iFile.close();
    if (atoi(f_type_string.c_str()) == heap)
    {
        file = new HeapFile();
    }
    else if (atoi(f_type_string.c_str()) == sorted)
    {
        // TODO sorted
    }
    else if (atoi(f_type_string.c_str()) == tree)
    {
        // TODO tree
    }
    file->Open(f_path);

    return 1;
}

void DBFile::MoveFirst()
{
    file->MoveFirst();
}

int DBFile::Close()
{
    return file->Close();
}

void DBFile::Add(Record &rec)
{
    file->Add(rec);
}

int DBFile::GetNext(Record &fetchme)
{
    return file->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
    return file->GetNext(fetchme, cnf, literal);
}
