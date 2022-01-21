#include "DuplicateQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include "../../threads/thread.h"

#include <algorithm>

using namespace std;
constexpr const char *DuplicateQuery::qname;
static pthread_mutex_t m;

static Table* table;
static long Count;
static long thread_num;
static DuplicateQuery* p;
static vector<string> *ops;
static long range;

static std::vector<Table::KeyType> copy_keys;
static std::vector<std::vector<Table::ValueType>> d;



void* duplicate_per_thread(void *thread_id){
    auto begin = table->begin() + int((long) thread_id * range);
    auto end = begin + int(range);
    if((long)thread_id == thread_num - 1) end = table->end();

    int tmp_cnt = 0;
    vector<Table::KeyType> temp_keys;
    vector<vector<Table::ValueType>> temp_data;
    temp_keys.clear();
    temp_data.clear();
    for(auto it = begin; it != end; it++){
        if (p->evalCondition(*it)){
            if(!table->hasKey(it->key() + "_copy")){
                ++tmp_cnt;
                temp_keys.push_back((*it).key());
                vector<Table::ValueType> dup_data;
                dup_data.clear();
                for (size_t j = 0; j < table->field().size(); ++j){
                    dup_data.push_back((*it)[j]);
                }
                temp_data.emplace_back(move(dup_data));
            }
        }
    }

    pthread_mutex_lock(&m);
    copy_keys.insert(copy_keys.end(), temp_keys.begin(), temp_keys.end());
    d.insert(d.end(), temp_data.begin(), temp_data.end());
    Count += tmp_cnt;
    pthread_mutex_unlock(&m);
    return nullptr;
}

QueryResult::Ptr DuplicateQuery::execute(){
    if (!this->operands.empty())
    return make_unique<ErrorMsgResult>(
            qname, this->targetTable.c_str(),
            "Invalid number of operands (? operands)."_f % operands.size()
    );

    Database &db = Database::getInstance();
    try{
        table = &db[this->targetTable];
        auto result = initCondition(*table);
        Count = 0;
        
        if(result.second){
            thread_num = getThreadNum();
            p = this;
            ops = &this->operands;
            pthread_mutex_init(&m, 0);
            if(thread_num >= 2 && (int)table->size() > 20){
                auto threads = new pthread_t[(unsigned long)thread_num];
                range = (long) table->size() / thread_num;
                for (long i = 0; i < thread_num; i++){
                    if(pthread_create(&threads[i], NULL, duplicate_per_thread, (void *)i) != 0) return make_unique<ErrorMsgResult>(qname, this->targetTable, "Fail to create thread %d");
                }
                for (int i = 0; i < thread_num; i++) pthread_join(threads[i], NULL);
                delete[] threads;
            }
            else
            {
                range = (long)table->size();
                duplicate_per_thread(0);
            }
            pthread_mutex_destroy(&m);
        }

        for(size_t i = 0 ;i < copy_keys.size();i++) {
            table->insertByIndex(copy_keys[i].append("_copy"), move(d[i]));
        }
        copy_keys.clear();
        d.clear();        
        return make_unique<RecordCountResult>(Count);
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

std::string DuplicateQuery::toString(){
    return "QUERY = DUPLICATE " + this->targetTable + "\"";
}