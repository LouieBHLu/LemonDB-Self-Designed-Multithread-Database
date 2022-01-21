// created by jiaying
#ifndef LEMONDB_TRUNCATETABLEQUERY_H
#define LEMONDB_TRUNCATETABLEQUERY_H

#include "../Query.h"

class TruncateTableQuery : public Query {
    static constexpr const char *qname = "TRUNCATE";
    const std::string fileName;

public:
    using Query::Query;

    QueryResult::Ptr execute() override;

    std::string toString() override;
};

#endif