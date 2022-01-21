#include "MaxQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include "../../threads/thread.h"

#include <algorithm>
using namespace std;

constexpr const char *MaxQuery::qname;

static long thread_num;     
static long range;    
static bool exist;
static Table* table;
static MaxQuery* p;
static vector<string> *ops;
static std::vector<std::vector<Table::ValueType>> MaxThread;

void* max_per_thread(void* thread_id){
    auto begin = table->begin() + int((long) thread_id * range);
    auto end = begin + int(range);
    if((long) thread_id == thread_num - 1) end = table->end();
    
    for(auto it = begin; it != end; it++){
        if(p->evalCondition(*it)){
          exist = true;
          for (size_t cnt = 0; cnt < ops->size(); ++cnt) {
              if (MaxThread[(size_t)thread_id][cnt] < (*it)[(*ops)[cnt]]) {
                  MaxThread[(size_t)thread_id][cnt] = (*it)[(*ops)[cnt]];
              }
          }
        }
    }
    return nullptr;
}

QueryResult::Ptr MaxQuery::execute() {
  if (this->operands.empty())
    return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                       "No operand (? operands)."_f %
                                           operands.size());
  Database &db = Database::getInstance();

  try {
    exist = false;
    table = &db[this->targetTable];
    auto result = initCondition(*table);

    if(result.second){
      thread_num = getThreadNum();
      for(size_t i = 0; i < operands.size(); i++) max.push_back(INT32_MIN);

      if (thread_num == 1 || table->size() < 50) {
        for (auto it = table->begin(); it != table->end(); ++it) {
            if (this->evalCondition(*it)) {
                exist = true;
                for (size_t cnt = 0; cnt < operands.size(); ++cnt) {
                    if (max[cnt] < (*it)[operands[cnt]]) {
                        max[cnt] = (*it)[operands[cnt]];
                    }
                }
            }
        }
      }
      else
      {
        p = this;
        range = (long) table->size() / thread_num;
        ops = &this->operands;
        auto threads = new pthread_t[(size_t)thread_num];
        for (size_t i = 0; i < MaxThread.size(); ++i) MaxThread[i].clear();
        MaxThread.clear();
        MaxThread.resize((size_t)thread_num * operands.size());
        for (size_t i = 0; i < (size_t)thread_num; ++i) {
            for (size_t cnt = 0; cnt < operands.size(); ++cnt) {
                MaxThread[i].push_back(INT32_MIN);
            }
            if (pthread_create(&threads[i], NULL, max_per_thread, (void *)i)) {
                return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(), "Fail to create threads");
            }
        }
        for (int i = 0; i < thread_num; ++i) pthread_join(*(threads + i), NULL);
        
        delete[] threads;
        for (size_t i = 0; i < (size_t)thread_num; ++i){
            for (size_t cnt = 0; cnt < operands.size(); ++cnt) {
                if (max[cnt] < MaxThread[i][cnt]) {
                    max[cnt] = MaxThread[i][cnt];
                }
            }
        }
      }
    }

    if(exist) return std::make_unique<SuccessMsgResult>(std::move(max));
    else return make_unique<NullQueryResult>();
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

std::string MaxQuery::toString() {
  return "QUERY = MAX " + this->targetTable + "\"";
}
