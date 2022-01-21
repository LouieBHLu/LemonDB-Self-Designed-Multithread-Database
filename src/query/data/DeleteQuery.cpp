#include "DeleteQuery.h"

#include <algorithm>

#include "../../db/Database.h"
#include "../QueryResult.h"

constexpr const char *DeleteQuery::qname;

QueryResult::Ptr DeleteQuery::execute(){
    using namespace std;
    
    Database &db = Database::getInstance();
    Table::SizeType counter = 0;
    
    try {
        auto &table = db[this->targetTable];
        vector<Table::KeyType> key;
        auto result = initCondition(table);

        if(result.second){
            for(auto it = table.begin(); it != table.end(); ++it){
                if(this->evalCondition(*it)){
                    key.push_back(it->key());
                    ++counter;
                }
            } 
        }
        table.del(key);
        return make_unique<RecordCountResult>(counter); 
    } catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                        "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
        // Cannot convert operand to string
        return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                        "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                        "Unkonwn error '?'."_f % e.what());
    }
}

std::string DeleteQuery::toString(){
    return "QUERY = DELETE " + this->targetTable + "\"";
}
