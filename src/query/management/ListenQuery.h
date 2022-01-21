#ifndef LEMONDB_LISTENQUERY_H
#define LEMONDB_LISTENQUERY_H

#include "../Query.h"

class ListenQuery : public Query {
    static constexpr const char *qname = "LISTEN";
    const std::string fileName;

public:
    ListenQuery(std::string filename): 
        Query(""), fileName(std::move(filename)) {}

    QueryResult::Ptr execute() override;

    std::string toString() override;
};

#endif