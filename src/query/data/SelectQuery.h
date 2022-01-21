//
// Created by lu on 21/10/19.
//

#ifndef PROJECT_SELECTQUERY_H
#define PROJECT_SELECTQUERY_H

#include "../Query.h"
#include "../../db/Database.h"

class SelectQuery : public ComplexQuery {
  static constexpr const char *qname = "SELECT";

public:
  std::vector <std::string> Data;
  Table::FieldIndex fieldId;
  std::string output;
  
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif // PROJECT_SELECTQUERY_H
