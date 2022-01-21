// TODO
// created by jiaying

#include "TruncateTableQuery.h"
#include <fstream>
#include "../../db/Database.h"

constexpr const char *TruncateTableQuery::qname;

QueryResult::Ptr TruncateTableQuery::execute() {
  using namespace std;
  Database &db = Database::getInstance();
  try {
    db[this->targetTable].clear();
    return make_unique<SuccessMsgResult>(qname);
  } catch (const exception &e) {
    return make_unique<ErrorMsgResult>(qname, e.what());
  }
}

std::string TruncateTableQuery::toString() {
    return "QUERY = TRUNCATE, Table = \"" + targetTable + "\"";
}

