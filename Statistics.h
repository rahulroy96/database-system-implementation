#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include <set>
#include <string>

using namespace ::std;

class RelationStats
{

	char *relName;
	u_int64_t numTuples;
	map<string, int> attributeMap;
	set<string> relNames;

public:
	/*
	 * Default constructor to create a new object
	 */
	RelationStats() {}

	/*
	 * Copy constructor to create a new object out of the given object
	 */
	RelationStats(RelationStats &copyMe);

	/*
	 * Constructor for creating new relation with name and number of tuples
	 */
	RelationStats(char *relName, int numTuples);

	/*
	 * Copy Relation stats from another object.
	 */
	void CopyRelationStats(RelationStats &copyMe, char *newName, string prefix);

	/*
	 * Adds a new attribute to the relation with numDistincts values
	 */
	void AddAtt(char *attName, int numDistincts);

	/*
	 * Returns the numDistincts for the attribute
	 */
	int GetDistinctAtt(char *attName);

	/*
	 * Returns the number of tuples
	 */
	int GetNumTuples();

	/*
	 * Sets the number of tuples
	 */
	void SetNumTuples(u_int64_t numTuples);

	/*
	 *  returns the string representation of the object
	 *  so that this can be written to a file.
	 */
	string GetString();

	/*
	 *  returns the Name of the relation represented by the object
	 */
	char *GetName();

	/*
	 *  Sets the Name of the relation represented by the object
	 */
	void SetName(char *relName);

	/*
	 *	Checks if the attribute belongs in this relation
	 *	Return 1 if attribute belongs to this relation else returns 0
	 */
	int IsAttributeInRelation(char *attrName);

	/*
	 *	Checks if a relation is there in the joined RelationStats
	 */
	bool IsRelationPresent(string relName);

	/*
	 *	Checks if the relation is a joined relation
	 */
	bool IsJoin();

	/*
	 *	Returns the number of relations in the joined relation
	 */
	int GetJoinSize();

	/*
	 *	Adds a new relation name to the joined relation name list
	 */
	void AddJoinRelation(string relName);

	/*
	 *	Merges the attributes and relNames of one relation with another
	 */
	void MergeRelation(RelationStats *relation);
};

class Statistics
{
	set<string> relationNames;
	map<string, RelationStats *> relationMap;

	/*
	 *	Checks if the attribute belongs in the relations given in the relNames array
	 *	Return 1 if attribute belongs in any of the relations else returns 0
	 *	relStat will have the object if successfully found
	 */
	RelationStats *GetRelationOfAttribute(Operand *op, char *relNames[], int numToJoin);

	double And(AndList *andList, char **relName, int numJoin);
	double Or(OrList *orList, char **relName, int numJoin);
	double Comparison(ComparisonOp *comparisonOp, char **relName, int numJoin);
	void UpdatedJoinRelNames(char *relNames[], char *newRelNames[], int &numToJoin);

public:
	Statistics();
	Statistics(Statistics &copyMe); // Performs deep copy
	~Statistics();

	/*
	 *	Add a new relation to the statistics with relName
	 *	and numTuples as number of tuples.
	 */
	void AddRel(char *relName, int numTuples);

	/*
	 *	Adds a new attribute to the relation given by relName.
	 *	If the numDistincts is equal to -1, it is taken as the number of tuples.
	 */
	void AddAtt(char *relName, char *attName, int numDistincts);

	/*
	 *	Copies the relation given by oldName and creates a new one with newName
	 *	The new relationStat object is added to the relationMap
	 */
	void CopyRel(char *oldName, char *newName);

	/*
	 * Read the statistics from the file given by the path fromWhere
	 * If the file doesnt exist create an empty file and return
	 */
	void Read(char *fromWhere);

	/*
	 * Writes the statistics to the file at the path fromWhere.
	 */
	void Write(char *fromWhere);

	/*
	 * Applies the operation givesn in the parseTree. 
	 * The names of the relations are given in the array. 
	 * The number of relations to join is given in numToJoin
	 */
	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);

	/*
	 * Calculates the estimated number of tuples after the operations are performed. 
	 */
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	void Print();
};

#endif
