// created by jiaying
// Modify by Lu

#include "CopyTableQuery.h"

#include "../../db/Database.h"

constexpr const char *CopyTableQuery::qname;

QueryResult::Ptr CopyTableQuery::execute() {
  using namespace std;
  Database &db = Database::getInstance();
  try {
    db.copyTable(this->targetTable, this->newTable);
    return make_unique<SuccessMsgResult>(qname);
    } catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, targetTable, "No such table."s);
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, e.what());
    }
}

std::string CopyTableQuery::toString() {
    return "QUERY = COPYTABLE, Table = \"" + targetTable + "\", newTable = \"" + newTable + "\"";
}

