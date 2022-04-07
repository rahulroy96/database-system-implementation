#include <gtest/gtest.h>

#include "DBFile.h"
#include "BigQ.h"
#include "RelOp.h"
#include "Record.h"
using ::testing::MatchesRegex;

extern "C"
{
    int yyparse(void);                     // defined in y.tab.c
    int yyfuncparse(void);                 // defined in yyfunc.tab.c
    void init_lexical_parser(char *);      // defined in lex.yy.c (from Lexer.l)
    void close_lexical_parser();           // defined in lex.yy.c
    void init_lexical_parser_func(char *); // defined in lex.yyfunc.c (from Lexerfunc.l)
    void close_lexical_parser_func();      // defined in lex.yyfunc.c
}
extern struct AndList *final;
extern struct FuncOperator *finalfunc;
extern FILE *yyin;
const char *settings = "test.cat";

void get_cnf(char *input, Schema *left, CNF &cnf_pred, Record &literal)
{
    init_lexical_parser(input);
    if (yyparse() != 0)
    {
        cout << " Error: can't parse your CNF " << input << endl;
        exit(1);
    }
    cnf_pred.GrowFromParseTree(final, left, literal); // constructs CNF predicate
    close_lexical_parser();
}

void get_path(char *catalog_path, char *dbfile_dir, char *tpch_dir)
{
    FILE *fp = fopen(settings, "r");
    if (fp)
    {
        char line[80];
        fgets(line, 80, fp);
        sscanf(line, "%s\n", catalog_path);
        fgets(line, 80, fp);
        sscanf(line, "%s\n", dbfile_dir);
        fgets(line, 80, fp);
        sscanf(line, "%s\n", tpch_dir);
        fclose(fp);
        if (!(catalog_path && dbfile_dir && tpch_dir))
        {
            cerr << " Test settings file 'test.cat' not in correct format.\n";
            exit(1);
        }
    }
    else
    {
        cerr << " Test settings files 'test.cat' missing \n";
        exit(1);
    }
}

TEST(HeapFile, CreateNegative)
{
    DBFile dbFile;
    ASSERT_EQ(dbFile.Create(nullptr, heap, nullptr), 0);
}

TEST(HeapFile, CreatePositive)
{
    DBFile dbFile;
    ASSERT_EQ(dbFile.Create("test.bin", heap, nullptr), 1);

    FILE *f = fopen("test.bin", "r");
    ASSERT_TRUE(f != nullptr);
}

TEST(HeapFile, OpenNegative)
{
    DBFile dbFile;
    ASSERT_EQ(dbFile.Open(nullptr), 0);
}

TEST(HeapFile, OpenPositive)
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
    char catalog_path[100];
    char dbfile_dir[100];
    char tpch_dir[100];
    get_path(catalog_path, dbfile_dir, tpch_dir);
    File *file = new File();
    file->Open(0, (char *)"bigQTest.bin");
    Schema nationSchema((char *)catalog_path, "nation");
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
    char catalog_path[100];
    char dbfile_dir[100];
    char tpch_dir[100];
    get_path(catalog_path, dbfile_dir, tpch_dir);
    File *file = new File();
    file->Open(0, (char *)"bigQTest.bin");
    Schema nationSchema((char *)catalog_path, "nation");
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

TEST(SortedFile, CreateNegative)
{
    DBFile dbFile;
    ASSERT_EQ(dbFile.Create(nullptr, sorted, nullptr), 0);
}

TEST(SortedFile, CreatePositive)
{
    DBFile dbFile;

    int runlen = 10;
    OrderMaker o("1,1,0");
    struct
    {
        OrderMaker *o;
        int l;
    } startup = {&o, runlen};
    ASSERT_EQ(dbFile.Create("test.bin", sorted, &startup), 1);

    FILE *f = fopen("test.bin", "r");
    ASSERT_TRUE(f != nullptr);
}

TEST(SortedFile, OpenNegative)
{
    DBFile dbFile;
    ASSERT_EQ(dbFile.Open(nullptr), 0);
}

TEST(SortedFile, OpenPositive)
{
    DBFile dbFile;
    ASSERT_EQ(dbFile.Open("test.bin"), 1);
    FILE *f = fopen("test.bin", "r");
    ASSERT_TRUE(f != nullptr);
}

Attribute IA = {"int", Int};
Attribute SA = {"string", String};
Attribute DA = {"double", Double};

void get_cnf(char *input, Schema *left, Function &fn_pred)
{
    init_lexical_parser_func(input);
    if (yyfuncparse() != 0)
    {
        cout << " Error: can't parse your arithmetic expr. " << input << endl;
        exit(1);
    }
    fn_pred.GrowFromParseTree(finalfunc, *left); // constructs CNF predicate
    close_lexical_parser_func();
}

void get_cnf(char *input, Schema *left, Schema *right, CNF &cnf_pred, Record &literal)
{
    init_lexical_parser(input);
    if (yyparse() != 0)
    {
        cout << " Error: can't parse your CNF " << input << endl;
        exit(1);
    }
    cnf_pred.GrowFromParseTree(final, left, right, literal); // constructs CNF predicate
    close_lexical_parser();
}

TEST(RelOps, SelectFilePositive)
{

    int psAtts = 5;
    int pipesz = 100;
    char catalog_path[100];
    char dbfile_dir[100];
    char tpch_dir[100];
    get_path(catalog_path, dbfile_dir, tpch_dir);
    DBFile dbf_ps;
    char rpath[100];
    sprintf(rpath, "%s%s.bin", dbfile_dir, "partsupp");
    dbf_ps.Open(rpath);
    Pipe _ps(pipesz);
    CNF cnf_ps;
    Record lit_ps;
    char *pred_ps = "(ps_supplycost < 1.031)";
    get_cnf(pred_ps, new Schema(catalog_path, "partsupp"), cnf_ps, lit_ps);
    SelectFile SF_ps;
    SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);
    SF_ps.WaitUntilDone();
    Record rec;
    int cnt = 0;
    while (_ps.Remove(&rec))
    {
        cnt++;
    }
    dbf_ps.Close();
    ASSERT_TRUE(cnt == 31);
}

TEST(RelOps, ProjectPositive)
{
    int psAtts = 5;
    int pAtts = 9;
    int pipesz = 100;
    char catalog_path[100];
    char dbfile_dir[100];
    char tpch_dir[100];
    get_path(catalog_path, dbfile_dir, tpch_dir);

    DBFile dbf;
    char rpath[100];
    sprintf(rpath, "%s%s.bin", dbfile_dir, "part");
    dbf.Open(rpath);

    CNF cnf_p;
    Record lit_p;
    char *pred_ps = "(p_retailprice > 931.00) AND (p_retailprice < 931.31)";
    get_cnf(pred_ps, new Schema(catalog_path, "part"), cnf_p, lit_p);

    Pipe _out(pipesz), _p(pipesz);
    int keepMe[] = {0, 1, 7};
    int numAttsIn = pAtts;
    int numAttsOut = 3;

    SelectFile SF_p;
    Project P_p;
    SF_p.Run(dbf, _p, cnf_p, lit_p);
    P_p.Run(_p, _out, keepMe, numAttsIn, numAttsOut);

    SF_p.WaitUntilDone();
    P_p.WaitUntilDone();

    Attribute att3[] = {IA, SA, DA};
    Schema out_sch("out_sch", numAttsOut, att3);
    Record rec;

    int cnt = 0;
    while (_out.Remove(&rec))
    {
        cnt++;
    }
    dbf.Close();
    ASSERT_TRUE(cnt == 22);
}

TEST(RelOps, SumPositive)
{
    int psAtts = 5;
    int pAtts = 9;
    int pipesz = 100;
    char catalog_path[100];
    char dbfile_dir[100];
    char tpch_dir[100];
    get_path(catalog_path, dbfile_dir, tpch_dir);

    DBFile dbf;
    char rpath[100];
    sprintf(rpath, "%s%s.bin", dbfile_dir, "supplier");
    dbf.Open(rpath);

    CNF cnf;
    Record lit;

    char *pred_s = "(s_suppkey = s_suppkey)";
    get_cnf(pred_s, new Schema(catalog_path, "supplier"), cnf, lit);

    Function func;
    char *str_sum = "(s_acctbal + (s_acctbal * 1.05))";
    get_cnf(str_sum, new Schema(catalog_path, "supplier"), func);

    Pipe _out(pipesz), _s(pipesz);

    Sum T;
    SelectFile SF;
    SF.Run(dbf, _s, cnf, lit);
    T.Run(_s, _out, func);

    SF.WaitUntilDone();
    T.WaitUntilDone();

    Record rec;

    int cnt = 0;
    Schema out_sch("out_sch", 1, &DA);
    while (_out.Remove(&rec))
    {
        ASSERT_STREQ(rec.GetString(&out_sch).c_str(), "double: [92462274.732500]\n");
        cnt++;
    }
    dbf.Close();
    ASSERT_TRUE(cnt == 1);
}

TEST(RelOps, JoinPositive)
{
    int psAtts = 5;
    int sAtts = 7;
    int pipesz = 100;
    char catalog_path[100];
    char dbfile_dir[100];
    char tpch_dir[100];
    get_path(catalog_path, dbfile_dir, tpch_dir);

    DBFile dbf_s;
    char rpath[100];
    sprintf(rpath, "%s%s.bin", dbfile_dir, "supplier");
    dbf_s.Open(rpath);

    CNF cnf_s;
    Record lit_s;
    char *pred_s = "(s_suppkey = s_suppkey)";
    Schema schema_s(catalog_path, "supplier");
    get_cnf(pred_s, &schema_s, cnf_s, lit_s);

    DBFile dbf_ps;
    char rpaths[100];
    sprintf(rpaths, "%s%s.bin", dbfile_dir, "partsupp");
    dbf_ps.Open(rpaths);

    CNF cnf_ps;
    Record lit_ps;
    char *pred_ps = "(ps_suppkey = ps_suppkey)";
    Schema schema_ps(catalog_path, "partsupp");
    get_cnf(pred_ps, &schema_ps, cnf_ps, lit_ps);

    Pipe _out(1), _s(pipesz), _ps(pipesz), _s_ps(pipesz);

    int outAtts = sAtts + psAtts;
    Attribute ps_supplycost = {"ps_supplycost", Double};
    Attribute joinatt[] = {IA, SA, SA, IA, SA, DA, SA, IA, IA, IA, ps_supplycost, SA};
    Schema join_sch("join_sch", outAtts, joinatt);

    Sum T;
    Function func;
    char *str_sum = "(ps_supplycost)";
    get_cnf(str_sum, &join_sch, func);

    SelectFile SF_ps, SF_s;
    Join J;
    CNF cnf_p_ps;
    Record lit_p_ps;
    get_cnf("(s_suppkey = ps_suppkey)", &schema_s, &schema_ps, cnf_p_ps, lit_p_ps);

    SF_s.Run(dbf_s, _s, cnf_s, lit_s);
    SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps); // 161 recs qualified
    J.Run(_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);
    T.Run(_s_ps, _out, func);

    SF_ps.WaitUntilDone();
    J.WaitUntilDone();
    T.WaitUntilDone();

    Schema sum_sch("sum_sch", 1, &DA);
    Record rec;

    int cnt = 0;
    while (_out.Remove(&rec))
    {
        ASSERT_STREQ(rec.GetString(&sum_sch).c_str(), "double: [400420638.539988]\n");
        cnt++;
    }
    dbf_ps.Close();
    dbf_s.Close();
    ASSERT_TRUE(cnt == 1);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}