// created by jiaying
#ifndef LEMONDB_COPYTABLEQUERY_H
#define LEMONDB_COPYTABLEQUERY_H

#include "../Query.h"

class CopyTableQuery : public Query {
    static constexpr const char *qname = "COPYTABLE";
    const std::string newTable;

public:
    CopyTableQuery(std::string table, std::string new_table): 
        Query(std::move(table)), newTable(std::move(new_table)) {}

    QueryResult::Ptr execute() override;

    std::string toString() override;
};

#endif