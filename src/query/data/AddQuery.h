// created by jiaying
// 
#ifndef PROJECT_ADDQUERY_H
#define PROJECT_ADDQUERY_H

#include "../Query.h"
#include "../../db/Database.h"

class AddQuery : public ComplexQuery {
  static constexpr const char *qname = "ADD";

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif // PROJECT_ADDQUERY_H