CC = g++ -std=c++14 -O2 -Wno-deprecated -fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment

tag = -i

ifdef linux
tag = -n
endif

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o RelOp.o Function.o DBFile.o HeapFile.o SortedFile.o Pipe.o y.tab.o lex.yy.o lex.yyfunc.o yyfunc.tab.o test.o 
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o RelOp.o Function.o DBFile.o HeapFile.o SortedFile.o Pipe.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o -ll -lpthread

test_sorted.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o HeapFile.o SortedFile.o Pipe.o y.tab.o lex.yy.o test_sorted.o 
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o HeapFile.o SortedFile.o Pipe.o y.tab.o lex.yy.o test_sorted.o -ll -lpthread

test_bigq.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o HeapFile.o SortedFile.o Pipe.o y.tab.o lex.yy.o test_bigq.o 
	$(CC) -o test_bigq.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o HeapFile.o SortedFile.o Pipe.o y.tab.o lex.yy.o test_bigq.o -ll -lpthread

test_heap.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o HeapFile.o SortedFile.o y.tab.o lex.yy.o test_heap.o 
	$(CC) -o test_heap.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o HeapFile.o SortedFile.o y.tab.o lex.yy.o test_heap.o -ll -lpthread

Gtest.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o HeapFile.o SortedFile.o BigQ.o y.tab.o lex.yy.o Gtest.o Pipe.o 
	$(CC) -o Gtest.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o HeapFile.o SortedFile.o BigQ.o y.tab.o lex.yy.o Gtest.o Pipe.o -ll -lgtest

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o -ll
	
test.o: test.cc test.h
	$(CC) -g -c test.cc

test_sorted.o: test_sorted.cc test_sorted.h
	$(CC) -g -c test_sorted.cc

test_bigq.o: test_bigq.cc test_bigq.h
	$(CC) -g -c test_bigq.cc

test_heap.o: test_heap.cc test_heap.h
	$(CC) -g -c test_heap.cc

Gtest.o: Gtest.cc
	$(CC) -g -c Gtest.cc

BigQ.o: BigQ.cc BigQ.h
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

main.o: main.cc
	$(CC) -g -c main.cc
	
Comparison.o: Comparison.cc Comparison.h
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc ComparisonEngine.h
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc DBFile.h
	$(CC) -g -c DBFile.cc

HeapFile.o: HeapFile.cc HeapFile.h
	$(CC) -g -c HeapFile.cc

SortedFile.o: SortedFile.cc SortedFile.h
	$(CC) -g -c SortedFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc



Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) -e "s|  __attribute__ ((__unused__))$$|# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif|" y.tab.c 
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"  yyfunc.tab.c 
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
	rm -f yyfunc.tab.*
	rm -f lex.yyfunc*
