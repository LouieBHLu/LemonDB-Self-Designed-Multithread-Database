#include "MinQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include "../../threads/thread.h"

#include <algorithm>
using namespace std;

constexpr const char *MinQuery::qname;

static long thread_num;     
static long range;    
static bool exist;
static Table* table;
static MinQuery* p;
static vector<string> *ops;
static std::vector<std::vector<Table::ValueType>> MinThread;

void* min_per_thread(void* thread_id){
    auto begin = table->begin() + int((long) thread_id * range);
    auto end = begin + int(range);
    if((long) thread_id == thread_num - 1) end = table->end();
    
    for(auto it = begin; it != end; it++){
        if(p->evalCondition(*it)){
          exist = true;
          for (size_t cnt = 0; cnt < ops->size(); ++cnt) {
              if (MinThread[(size_t)thread_id][cnt] > (*it)[(*ops)[cnt]]) {
                  MinThread[(size_t)thread_id][cnt] = (*it)[(*ops)[cnt]];
              }
          }
        }
    }
    return nullptr;
}

QueryResult::Ptr MinQuery::execute() {
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
      for(size_t i = 0; i < operands.size(); i++) min.push_back(INT32_MAX);

      if (thread_num == 1 || table->size() < 50) {
        for (auto it = table->begin(); it != table->end(); ++it) {
            if (this->evalCondition(*it)) {
                exist = true;
                for (size_t cnt = 0; cnt < operands.size(); ++cnt) {
                    if (min[cnt] > (*it)[operands[cnt]]) {
                        min[cnt] = (*it)[operands[cnt]];
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
        for (size_t i = 0; i < MinThread.size(); ++i) MinThread[i].clear();
        MinThread.clear();
        MinThread.resize((size_t)thread_num * operands.size());
        for (size_t i = 0; i < (size_t)thread_num; ++i) {
            for (size_t cnt = 0; cnt < operands.size(); ++cnt) {
                MinThread[i].push_back(INT32_MAX);
            }
            if (pthread_create(&threads[i], NULL, min_per_thread, (void *)i)) {
                return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(), "Fail to create threads");
            }
        }
        for (int i = 0; i < thread_num; ++i) pthread_join(*(threads + i), NULL);
        
        delete[] threads;
        for (size_t i = 0; i < (size_t)thread_num; ++i){
            for (size_t cnt = 0; cnt < operands.size(); ++cnt) {
                if (min[cnt] > MinThread[i][cnt]) {
                    min[cnt] = MinThread[i][cnt];
                }
            }
        }
      }
    }

    if(exist) return std::make_unique<SuccessMsgResult>(std::move(min));
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

std::string MinQuery::toString() {
  return "QUERY = MIN " + this->targetTable + "\"";
}
