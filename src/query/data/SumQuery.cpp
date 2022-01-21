#include "SumQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include "../../threads/thread.h"

#include <algorithm>
using namespace std;

constexpr const char *SumQuery::qname;
static long range;
static long thread_num;
static unsigned long operands_num;
static Table *table;
static SumQuery* p;
static vector<string> *ops;
static std::vector<Table::ValueType> Count;
static pthread_mutex_t m;

void *sum_per_thread(void *thread_id){
    auto begin = table->begin() + int((long) thread_id * range);
    auto end = begin + int(range);
    if((long) thread_id == thread_num - 1) end = table->end();
    vector<int> tmp_cnt(operands_num,0);

    for(auto it = begin; it != end; it++){
        if(p->evalCondition(*it)){
            for(size_t index = 0; index < operands_num; index++){
                auto field_id = table->getFieldIndex((*ops)[index]);
                tmp_cnt[index] += (*it)[field_id];
            }
        }
    }
    pthread_mutex_lock(&m);
    for(size_t i = 0; i < operands_num; i++) Count[i] += tmp_cnt[i];
    pthread_mutex_unlock(&m);
    return nullptr;
}

QueryResult::Ptr SumQuery::execute(){
    if (this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    for(const auto &operand : this->operands){
        if(operand=="KEY")
            return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid operands: KEY."s
            );
    }

    Database &db = Database::getInstance();
    try{
        table = &db[this->targetTable];
        auto result = initCondition(*table);
        Count.assign(operands.size(),0);

        if(result.second){
            thread_num = getThreadNum();
            p = this;
            ops = &this->operands;
            operands_num = this->operands.size();
            pthread_mutex_init(&m, 0);

            if(thread_num >= 2 && (int)table->size() > 20){
                auto threads = new pthread_t[(unsigned long)thread_num];
                range = (long) table->size() / thread_num;
                for (long i = 0; i < thread_num; i++){
                    if(pthread_create(&threads[i], NULL, sum_per_thread, (void *)i) != 0) return make_unique<ErrorMsgResult>(qname, this->targetTable, "Fail to create thread %d");
                }
                for (int i = 0; i < thread_num; i++) pthread_join(threads[i], NULL);
                delete[] threads;
            }
            else
            {
                range = (long)table->size();
                sum_per_thread(0);
            }
            pthread_mutex_destroy(&m);
        }
        return make_unique<SumQueryResult>(std::move(Count));
    } catch (const TableNameNotFound &e) {
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

std::string SumQuery::toString(){
    return "QUERY = SUM " + this->targetTable + "\"";
}