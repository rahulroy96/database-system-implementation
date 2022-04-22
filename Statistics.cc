#include "Statistics.h"
// #include <sstream>
#include <iostream>
#include <fstream>
#include <vector>

/*
 * Constructor for creating new relation with name and number of tuples
 */
RelationStats::RelationStats(char *relName, int numTuples)
{
    this->relName = relName;
    this->numTuples = numTuples;
    this->relNames.insert(relName);
}

/*
 * Copy constructor to create a new object out of the given object
 */
RelationStats::RelationStats(RelationStats &copyMe)
{

    relName = copyMe.relName;
    numTuples = copyMe.numTuples;

    for (auto it = copyMe.attributeMap.begin(); it != copyMe.attributeMap.end(); it++)
    {
        attributeMap[it->first] = it->second;
    }

    for (auto it = copyMe.relNames.begin(); it != copyMe.relNames.end(); it++)
    {
        this->relNames.insert(*it);
    }
}

/*
 * Copy relations
 */
void RelationStats::CopyRelationStats(RelationStats &copyMe, char *newName, string prefix)
{
    relName = newName;
    this->relNames.insert(newName);
    numTuples = copyMe.numTuples;
    for (auto it = copyMe.attributeMap.begin(); it != copyMe.attributeMap.end(); it++)
    {
        string attName = prefix;
        attName.append(it->first);
        attributeMap[attName] = it->second;
    }
}

/*
 * Adds a new attribute to the relation with numDistincts values
 */
void RelationStats::AddAtt(char *attName, int numDistincts)
{
    string attNameString = string(attName);
    if (numDistincts == -1)
        numDistincts = this->numTuples;
    attributeMap[attNameString] = numDistincts;
}

/*
 *  returns the string representation of the object
 *  so that this can be written to a file.
 */
string RelationStats::GetString()
{
    string value = relName;
    value.append("\n");
    value.append(to_string(numTuples));

    for (auto it = relNames.begin(); it != relNames.end(); it++)
    {
        value.append("\n");
        value.append(*it);
    }
    value.append("\nEND_JOINLIST");
    for (auto it = attributeMap.begin(); it != attributeMap.end(); it++)
    {
        value.append("\n");
        value.append(it->first);
        value.append(" ");
        value.append(to_string(it->second));
    }
    value.append("\n");
    return value;
}

/*
 * Returns the numDistincts for the attribute
 */
int RelationStats::GetDistinctAtt(char *attName)
{
    string attNameString = string(attName);
    return attributeMap[attNameString];
}

/*
 * Returns the number of tuples
 */
int RelationStats::GetNumTuples()
{
    return numTuples;
}

/*
 * Sets the number of tuples
 */
void RelationStats::SetNumTuples(u_int64_t numTuples)
{
    this->numTuples = numTuples;
}

/*
 *  returns the Name of the relation represented by the object
 */
char *RelationStats::GetName()
{
    return relName;
}

/*
 *  Sets the Name of the relation represented by the object
 */
void RelationStats::SetName(char *relName)
{
    this->relName = relName;
}

int RelationStats::IsAttributeInRelation(char *attName)
{
    string attNameString = string(attName);
    map<string, int>::iterator it = attributeMap.find(attNameString);
    if (it != attributeMap.end())
        return 1;
    else
        return 0;
}

/*
 *	Checks if a relation is there in the joined RelationStats
 */
bool RelationStats::IsRelationPresent(string relName)
{
    return relNames.find(relName) != relNames.end();
}

/*
 *	Checks if the relation is a joined relation
 */
bool RelationStats::IsJoin()
{
    return relNames.size() > 1;
}

/*
 *	Returns the number of relations in the joined relation
 */
int RelationStats::GetJoinSize()
{
    return relNames.size();
}

/*
 *	Adds a new relation name to the joined relation name list
 */
void RelationStats::AddJoinRelation(string relName)
{
    relNames.insert(relName);
}

/*
 *	Merges the attributes and jouned relNames of one relation with another
 */
void RelationStats::MergeRelation(RelationStats *relation)
{
    attributeMap.insert(relation->attributeMap.begin(), relation->attributeMap.end());
    relNames.insert(relation->relNames.begin(), relation->relNames.end());
}

Statistics::Statistics()
{
}

/*
 * Copy constructor to create a new object out of the given object
 */
Statistics::Statistics(Statistics &copyMe)
{
    for (auto it = copyMe.relationMap.begin(); it != copyMe.relationMap.end(); it++)
    {
        RelationStats *relStats = new RelationStats(*it->second);
        relationMap[it->first] = relStats;
    }
}

Statistics::~Statistics()
{
}

/*
 * Creates a new relation with name relName and
 * number of tuples equal to numTuples
 */
void Statistics::AddRel(char *relName, int numTuples)
{
    RelationStats *newRealation = new RelationStats(relName, numTuples);
    string relNameString = string(relName);
    relationNames.insert(relName);
    relationMap[relNameString] = newRealation;
}

/*
 * Add a new attribute to the relation relName
 * Use the AddAtt method of the RelationStats object
 * of the relation to add the attribute.
 */
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    string relNameString = string(relName);
    relationMap[relNameString]->AddAtt(attName, numDistincts);
}

/*
 * Copy the relation given by oldName and create a new relation with newName
 */
void Statistics::CopyRel(char *oldName, char *newName)
{
    string oldNameString = string(oldName);
    string newNameString = string(newName);
    RelationStats *newRelStats = new RelationStats();
    string prefix = newNameString;
    prefix.append(".");
    newRelStats->CopyRelationStats(*relationMap[oldNameString], newName, prefix);

    relationMap[newNameString] = newRelStats;
}

/*
 * Read the statistics from the file given by the path fromWhere
 * If the file doesnt exist create an empty file and return.
 */
void Statistics::Read(char *fromWhere)
{

    FILE *foo = fopen(fromWhere, "r");
    // this is enough space to hold any tokens
    char space[200];

    fscanf(foo, "%s", space);
    int totscans = 1;

    // see if the file starts with the correct keyword
    if (strcmp(space, "BEGIN"))
    {
        cout << "Unfortunately, this does not seem to be a statistics file.\n";
        exit(1);
    }
    RelationStats *relStat;

    while (fscanf(foo, "%s", space) != EOF)
    {
        relStat = new RelationStats();
        char *relName = strdup(space);
        relStat->SetName(relName);
        fscanf(foo, "%s", space);
        int numTuples = atoi(space);
        relStat->SetNumTuples(numTuples);
        while (fscanf(foo, "%s", space) != EOF)
        {
            if (!strcmp(space, "END_JOINLIST"))
            {
                break;
            }
            char *joinRelName = strdup(space);
            relStat->AddJoinRelation(joinRelName);
        }
        while (fscanf(foo, "%s", space) != EOF)
        {
            if (!strcmp(space, "END"))
            {
                break;
            }
            char *attName = strdup(space);
            fscanf(foo, "%s", space);
            int numDistints = atoi(space);
            relStat->AddAtt(attName, numDistints);
        }
        relationMap[string(relName)] = relStat;
        fscanf(foo, "%s", space);
        if (strcmp(space, "BEGIN"))
        {
            break;
        }
    }

    fclose(foo);
}

/*
 * Writes the statistics to the file at the path fromWhere
 */
void Statistics::Write(char *fromWhere)
{
    ofstream oFile;
    oFile.open(fromWhere, fstream::out | fstream::trunc);

    if (!oFile.is_open())
    {
        cout << "Cant open the file" << endl;
        return;
    }

    for (auto it = relationMap.begin(); it != relationMap.end(); it++)
    {
        oFile << "BEGIN\n";
        oFile << (it->second)->GetString();
        oFile << "END\n";
    }
    oFile.close();
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    char *newRelNames[100];
    UpdatedJoinRelNames(relNames, newRelNames, numToJoin);

    // Call the estimation here
    double estimatedTuples = Estimate(parseTree, newRelNames, numToJoin);

    // If it reaches this stage it means join can be done, then at this stage we need to merge the relations together
    int index = 1;

    string relationName = newRelNames[0];

    RelationStats *firstRelationObj = relationMap[relationName];

    relationMap.erase(relationName);

    RelationStats *tempRelation;

    firstRelationObj->SetNumTuples(estimatedTuples + 0.1);

    while (index < numToJoin)
    {

        tempRelation = relationMap[newRelNames[index]];

        relationMap.erase(newRelNames[index]);

        // merge the attributes from both the relations.
        firstRelationObj->MergeRelation(tempRelation);

        index++;
    }

    relationMap[relationName] = firstRelationObj;
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    char *newRelationNames[100];
    UpdatedJoinRelNames(relNames, newRelationNames, numToJoin);

    double tupleProduct = 1.0;

    double factor = 1.0;

    int index = 0;
    RelationStats *relation;

    while (index < numToJoin)
    {
        relation = relationMap[newRelationNames[index]];
        tupleProduct *= (double)relation->GetNumTuples();
        index++;
    }

    if (parseTree == NULL)
        return tupleProduct;

    factor = And(parseTree, newRelationNames, numToJoin);

    double returnVal = (double)tupleProduct * factor;

    // cout << "Return Val: " << returnVal << endl;

    return returnVal;
}

void Statistics::UpdatedJoinRelNames(char *relNames[], char *newRelNames[], int &numToJoin)
{
    map<char *, RelationStats *> relationStatsName;

    int index = 0;
    map<string, RelationStats *>::iterator it;
    while (index < numToJoin)
    {
        bool found = false;
        for (it = relationMap.begin(); it != relationMap.end(); it++)
        {
            if (it->second->IsRelationPresent(relNames[index]))
            {
                relationStatsName[it->second->GetName()] = it->second;
                found = true;
                break;
            }
        }
        if (!found)
        {
            // for (it = relationMap.begin(); it != relationMap.end(); it++)
            // {
            //     cout << it->second->GetString() << endl;
            // }
            cout << relNames[index] << " not found!!!" << endl;
            exit(1);
        }
        index++;
    }
    numToJoin = relationStatsName.size();
    // cout << numToJoin << endl;
    index = 0;
    map<char *, RelationStats *>::iterator it2 = relationStatsName.begin();
    for (; it2 != relationStatsName.end(); it2++)
    {
        newRelNames[index++] = it2->first;
    }
}

/*
 *	Checks if the attribute belongs in the relations given in the relNames array
 *	Return 1 if attribute belongs in any of the relations else returns 0
 *	relStat will have the object if successfully found
 */
RelationStats *Statistics::GetRelationOfAttribute(Operand *op, char *relNames[], int numToJoin)
{
    if (op == nullptr)
    {
        cout << "Operator is null" << endl;
        return nullptr;
    }

    if (relNames == nullptr)
    {
        cout << "Relation Names is null" << endl;
        return nullptr;
    }

    for (int i = 0; i < numToJoin; i++)
    {
        if (relationMap[relNames[i]]->IsAttributeInRelation(op->value))
        {

            return relationMap[relNames[i]];
        }
    }
    // cout << "\nop value: " << op->value << endl;
    // for (int i = 0; i < numToJoin; i++)
    // {
    //     cout << relationMap[relNames[i]]->GetString();
    // }
    // cout << "didnt find the relation" << endl;
    return nullptr;
}

/*
 * Print the statistics to the standard out
 */
void Statistics ::Print()
{
    map<string, RelationStats *>::iterator it;
    for (it = relationMap.begin(); it != relationMap.end(); it++)
        cout << (it->second)->GetString() << endl;
}

double Statistics ::And(AndList *andList, char **relName, int numJoin)
{
    if (andList == NULL)
        return 1.0;

    double leftValue = 1.0;

    double rightValue = 1.0;

    leftValue = Or(andList->left, relName, numJoin);

    // cout << "Left Value at AND: " << leftValue << endl;

    rightValue = And(andList->rightAnd, relName, numJoin);

    // cout << "Right Value at AND: " << rightValue << endl;
    // cout << "Return value from AND: " << (leftValue * rightValue) << endl;
    return ((double)leftValue * rightValue);
}

double Statistics ::Or(OrList *orList, char **relName, int numJoin)
{
    if (orList == NULL)
        return 0.0;

    double orLeftComp = 0.0;

    double orRightValue = 0.0;

    double orFactor = 0.0;

    // calculate the left ComparisonOp of the OrList;
    orLeftComp = Comparison(orList->left, relName, numJoin);

    int count = 1;

    // check if the OR, is comparing the same attribute with multiple values, in which case we can use the estimate of the first one multiplied by the
    // number of times it will be ORed, because the number of times it will be ORed increases its probablity of appearing in the final result.

    OrList *orRightTemp = orList->rightOr;

    char *orAtt = orList->left->left->value;

    while (orRightTemp)
    {
        if (strcmp(orRightTemp->left->left->value, orAtt) == 0)
            count++;

        orRightTemp = orRightTemp->rightOr;
    }

    if (count > 1)
        return (double)count * orLeftComp;

    orRightValue = Or(orList->rightOr, relName, numJoin);

    // cout << "OR Left Comp: " << orLeftComp << endl;
    // cout << "OR Right Value: " << orRightValue << endl;

    orFactor = (double)(1.0 - ((1.0 - orLeftComp) * (1.0 - orRightValue)));

    // cout << "OrFactor: " << orFactor << endl;
    // cout << "Return from OR: " << orFactor << endl;
    return orFactor;
}

double Statistics ::Comparison(ComparisonOp *comparisonOp, char **relName, int numJoin)
{
    RelationStats *leftRelation;

    RelationStats *rightRelation;

    double distinctLeft = 0.0, distinctRight = 0.0;

    double factor = 0.0;

    int code = comparisonOp->code;

    if (comparisonOp->left->code == NAME)
    {
        leftRelation = GetRelationOfAttribute(comparisonOp->left, relName, numJoin);
        if (leftRelation == nullptr)
        {
            cout << comparisonOp->left->value << " not present in any relation" << endl;
            distinctLeft = (double)1.0;
        }
        else
        {
            distinctLeft = (double)leftRelation->GetDistinctAtt(comparisonOp->left->value);
        }
    }
    else
    {
        distinctLeft = (double)-1.0;
    }

    if (comparisonOp->right->code == NAME)
    {
        rightRelation = GetRelationOfAttribute(comparisonOp->right, relName, numJoin);
        if (rightRelation == nullptr)
        {
            cout << comparisonOp->right->value << " not present in any relation" << endl;
            distinctRight = (double)1.0;
        }
        else
        {

            distinctRight = (double)rightRelation->GetDistinctAtt(comparisonOp->right->value);
        }
    }
    else
    {
        distinctRight = -1;
    }

    // cout << distinctLeft << " - " << distinctRight << endl;

    if (code == LESS_THAN || code == GREATER_THAN)
    {
        factor = (double)1.0 / (double)3.0;
    }
    else if (code == EQUALS)
    {
        if (distinctLeft > distinctRight)
            factor = (double)((double)1.0 / (double)distinctLeft);
        else
            factor = (double)((double)1.0 / (double)distinctRight);
    }
    else
    {
        cout << "Error" << endl;
    }

    // cout << "Factor: " << factor << endl;

    return factor;
}
