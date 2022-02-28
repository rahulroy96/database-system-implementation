#include <gtest/gtest.h>

#include "DBFile.h"
#include "BigQ.h"

const char *catalog_path = "catalog";
const char *tpch_dir = "../tpch-dbgen/"; // dir where dbgen tpch files (extension *.tbl) can be found
const char *dbfile_dir = "dbfile_dir/";

TEST(DBFile, CreateNegative)
{
    DBFile dbFile;
    ASSERT_EQ(dbFile.Create(nullptr, heap, nullptr), 0);
}

TEST(DBFile, CreatePositive)
{
    DBFile dbFile;
    ASSERT_EQ(dbFile.Create("test.bin", heap, nullptr), 1);

    FILE *f = fopen("test.bin", "r");
    ASSERT_TRUE(f != nullptr);
}

TEST(DBFile, OpenNegative)
{
    DBFile dbFile;
    ASSERT_EQ(dbFile.Open(nullptr), 0);
}

TEST(DBFile, OpenPositive)
{
    DBFile dbFile;
    ASSERT_EQ(dbFile.Open("test.bin"), 1);
    FILE *f = fopen("test.bin", "r");
    ASSERT_TRUE(f != nullptr);
}


TEST(BigQRun, SortWriteEmpty)
{
    File *file = new File();
    file->Open(0, (char *)"bigQTest.bin");
    BigQRun run(10, 0, 10, file, nullptr);
    vector<Record *> records;
    ASSERT_EQ(run.SortWrite(records), 0);
}


TEST(BigQRun, SortWriteSingle)
{
    File *file = new File();
    file->Open(0, (char *)"bigQTest.bin");
    Schema nationSchema(catalog_path, "nation");
    OrderMaker sortorder(&nationSchema);
    BigQRun run(10, 0, 10, file, &sortorder);
    vector<Record *> records;
    Record *testRecord = new Record();
    char rpath[100];
    sprintf(rpath, "%s%s.bin", dbfile_dir, "nation");
    DBFile dbfile;
	dbfile.Open(rpath);
	dbfile.MoveFirst();
    dbfile.GetNext(*testRecord);
    records.push_back(testRecord);
    ASSERT_EQ(run.SortWrite(records), 1);
    ASSERT_EQ(run.LoadNextHead(), 1);
    delete testRecord;
    delete file;
}

TEST(BigQRun, LoadNextHeadPositive)
{
    File *file = new File();
    file->Open(0, (char *)"bigQTest.bin");
    Schema nationSchema(catalog_path, "nation");
    OrderMaker sortorder(&nationSchema);
    BigQRun run(10, 0, 10, file, &sortorder);
    vector<Record *> records;
    Record *testRecord = new Record();
    char rpath[100];
    sprintf(rpath, "%s%s.bin", dbfile_dir, "nation");
    DBFile dbfile;
	dbfile.Open(rpath);
	dbfile.MoveFirst();
    dbfile.GetNext(*testRecord);
    records.push_back(testRecord);
    ASSERT_EQ(run.SortWrite(records), 1);
    run.MoveFirst();
    ASSERT_EQ(run.LoadNextHead(), 1);
}


TEST(BigQRun, LoadNextHeadNegative)
{
    File *file = new File();
    file->Open(0, (char *)"bigQTest.bin");
    BigQRun run(10, 0, 10, file, nullptr);
    ASSERT_EQ(run.LoadNextHead(), 0);
}




int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}