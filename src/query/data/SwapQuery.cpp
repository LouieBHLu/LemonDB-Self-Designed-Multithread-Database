#include "SwapQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include "../../threads/thread.h"

#include <algorithm>
using namespace std;

constexpr const char *SwapQuery::qname;

static Table *table;
static long range;
static long thread_num;
static SwapQuery *p;
static vector<string> *ops;
static int Count;
static pthread_mutex_t m;

static Table::FieldIndex fieldId1;
static Table::FieldIndex fieldId2;

void* swap_per_thread(void* thread_id){
    auto begin = table->begin() + int((long) thread_id * range);
    auto end = begin + int(range);
    if((long) thread_id == thread_num - 1) end = table->end();
    
    int tmp_cnt = 0;
    for(auto it = begin; it != end; it++){
        if(p->evalCondition(*it)){
            auto tmp = (*it)[fieldId1];
            (*it)[fieldId1] = (*it)[fieldId2];
            (*it)[fieldId2] = tmp;
            tmp_cnt++;
        }
    }
    pthread_mutex_lock(&m);
    Count += tmp_cnt;
    pthread_mutex_unlock(&m);
    return nullptr;
}

QueryResult::Ptr SwapQuery::execute() {
  if(this->operands.size() != 2)
      return make_unique<ErrorMsgResult>(
              qname, this->targetTable.c_str(),
              "Invalid number of operands (? operands)."_f % operands.size()
      );
  if(this->operands[0]=="KEY" || this->operands[1]=="KEY"){
      return make_unique<ErrorMsgResult>(
              qname, this->targetTable.c_str(),
              "Invalid operands: KEY."s
      );
    }

  Database &db = Database::getInstance();
  try{
    table = &db[this->targetTable];
    fieldId1 = table->getFieldIndex(this->operands[0]);
    fieldId2 = table->getFieldIndex(this->operands[1]);
    Count = 0;
    auto result = initCondition(*table);

    if(result.second){
      thread_num = getThreadNum();
      p = this;
      ops = &this->operands;
      pthread_mutex_init(&m, 0);

      if(thread_num >= 2 && (int)table->size() > 20){
          auto threads = new pthread_t[(unsigned long)thread_num];
          range = (long) table->size() / thread_num;
          for (long i = 0; i < thread_num; i++){
              if(pthread_create(&threads[i], NULL, swap_per_thread, (void *)i) != 0) return make_unique<ErrorMsgResult>(qname, this->targetTable, "Fail to create thread %d");
          }
          for (int i = 0; i < thread_num; i++) pthread_join(threads[i], NULL);
          delete[] threads;
      }
      else
      {
          range = (long)table->size();
          swap_per_thread(0);
      }
      pthread_mutex_destroy(&m);
    }
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

std::string SwapQuery::toString() {
  return "QUERY = SWAP " + this->targetTable + "\"";
}
