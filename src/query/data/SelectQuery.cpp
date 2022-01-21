#include "SelectQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include "../../threads/thread.h"

#include <algorithm>
#include <string>
using namespace std;

constexpr const char *SelectQuery::qname;
static long thread_num;    
static long range;   
static Table* table;
static SelectQuery* p;
static std::vector<Table::FieldNameType> ops;
static std::vector<std::vector <std::string>> data_per_thread;

#include<iostream>
void* select_per_thread(void* thread_id) {
    auto it = table->begin() + int((long)thread_id * range);
    auto end = it + int(range);
    if ((long)thread_id == thread_num - 1) {
        end = table->end();
    }
    for (; it != end; ++it) {
        if (p->evalCondition(*it)) {
            std::string temp = "";
            temp += "( " + std::string(it->key());
            for (size_t cnt = 1; cnt < ops.size(); ++cnt) {
                temp += " " + to_string((*it)[table->getFieldIndex(ops[cnt])]);
            }
            temp += " )";
            data_per_thread[(unsigned long)thread_id].push_back(temp);
        }
    }
    return nullptr;
}

QueryResult::Ptr SelectQuery::execute() {
    using namespace std;
    Database &db = Database::getInstance();
    try {
        table = &db[this->targetTable];
        auto result = initCondition(*table);
        output = "";
        if (result.second) {
            thread_num = getThreadNum();
            if (thread_num == 1 || table->size() < 50) {
                for (auto it = table->begin(); it != table->end(); ++it) {
                    if (this->evalCondition(*it)) {
                        string temp = "";
                        temp += "( " + string(it->key());
                        for (size_t cnt = 1; cnt < operands.size(); ++cnt) {
                            temp += " " + to_string((*it)[table->getFieldIndex(this->operands[cnt])]);
                        }
                        temp += " )";
                        Data.push_back(temp);
                    }
                }
            }
            else 
            {
                p = this;
                range = (long)table->size() / thread_num;
                ops = operands;
                auto thread_address = new pthread_t[(unsigned long)thread_num];
                for (size_t cnt = 0; cnt < data_per_thread.size(); ++cnt) {
                    data_per_thread[cnt].clear();
                }
                data_per_thread.clear();
                data_per_thread.resize((unsigned long)thread_num);
                for (size_t i = 0; i < (size_t)thread_num; ++i) {
                    data_per_thread[i] = vector<string> ();
                    if (pthread_create(&thread_address[i], NULL, select_per_thread, (void *)i)) {
                        return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(), "Fail to create threads");
                    }
                }
                for (int i = 0; i < thread_num; ++i) {
                    pthread_join(*(thread_address + i), NULL);
                }
                delete[] thread_address;
                for (size_t i = 0; i < (size_t)thread_num; ++i) {
                    for (size_t cnt = 0; cnt < data_per_thread[i].size(); ++cnt) {
                        Data.push_back(data_per_thread[i][cnt]);
                    }
                }
            }

            sort(Data.begin(), Data.end());
            for (auto str: Data) {
                if (str != *Data.begin() && !str.empty()) output += "\n";
                output += str;
            }
        }
        if (!Data.empty()) return std::make_unique<SuccessMsgResult>(output);
        else return make_unique<NullQueryResult>(); 
    }
    catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
        // Cannot convert operand to string
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unkonwn error '?'."_f % e.what());
    }
}

std::string SelectQuery::toString() {
    return "QUERY = INSERT " + this->targetTable + "\"";
}
