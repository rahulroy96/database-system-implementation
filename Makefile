CC = g++ -O2 -Wno-deprecated -fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment

tag = -i

ifdef linux
tag = -n
endif

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o HeapFile.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o HeapFile.o -lfl -lpthread

test_heap.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test_heap.o HeapFile.o
	$(CC) -o test_heap.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test_heap.o HeapFile.o -lfl

Gtest.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o BigQ.o y.tab.o lex.yy.o Gtest.o HeapFile.o Pipe.o 
	$(CC) -o Gtest.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o BigQ.o y.tab.o lex.yy.o Gtest.o HeapFile.o Pipe.o -lfl -lgtest

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o -lfl
	
test.o: test.cc test.h
	$(CC) -g -c test.cc

test_heap.o: test_heap.cc test_heap.h
	$(CC) -g -c test_heap.cc

Gtest.o: Gtest.cc
	$(CC) -g -c Gtest.cc

BigQ.o: BigQ.cc BigQ.h
	$(CC) -g -c BigQ.cc

main.o: main.cc
	$(CC) -g -c main.cc
	
Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc DBFile.h
	$(CC) -g -c DBFile.cc

HeapFile.o: HeapFile.cc HeapFile.h
	$(CC) -g -c HeapFile.cc

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

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
