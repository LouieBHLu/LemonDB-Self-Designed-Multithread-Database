// created by jiaying
// Modified by lu

#ifndef PROJECT_MINQUERY_H
#define PROJECT_MINQUERY_H

#include "../Query.h"
#include "../../db/Database.h"

class MinQuery : public ComplexQuery {
  static constexpr const char *qname = "MIN";
  std::vector<Table::ValueType> min;
public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif // PROJECT_MINQUERY_H