// created by jiaying
// Modified by lu

#ifndef PROJECT_MAXQUERY_H
#define PROJECT_MAXQUERY_H

#include "../Query.h"
#include "../../db/Database.h"

class MaxQuery : public ComplexQuery {
  static constexpr const char *qname = "MAX";
  std::vector<Table::ValueType> max;
public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif // PROJECT_MAXQUERY_H