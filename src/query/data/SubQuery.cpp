#include "SubQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include "../../threads/thread.h"

#include <algorithm>
using namespace std;

constexpr const char *SubQuery::qname;

static Table *table;
static long range;
static int Count;
static long thread_num;
static SubQuery *p;
static vector<string> *ops;
static pthread_mutex_t m;

void* sub_per_thread(void* thread_id){
  auto begin = table->begin() + int((long) thread_id * range);
  auto end = begin + int(range);
  if((long) thread_id == thread_num - 1) end = table->end();

  int tmp_cnt = 0;
  for(auto it = begin; it != end; it++){
      if(p->evalCondition(*it)){
        Table::ValueType ans = (*it)[(*ops)[0]];
        for (Table::SizeType i = 1; i < ops->size() - 1; i++) {
          ans = ans - (*it)[(*ops)[i]];
        }
          (*it)[(*ops)[ops->size() - 1]] = ans;
          ++tmp_cnt;
      }
  }
  pthread_mutex_lock(&m);
  Count += tmp_cnt;
  pthread_mutex_unlock(&m);
  return nullptr;
}

QueryResult::Ptr SubQuery::execute() {
  if (this->operands.size() < 2)
      return make_unique<ErrorMsgResult>(
              qname, this->targetTable.c_str(),
              "Too less operands (? operands)."_f % operands.size());
  
  Database &db = Database::getInstance();
  Count = 0;

  try {
    table = &db[this->targetTable];
    // desField = table->getFieldIndex(this->operands.back());
    auto result = initCondition(*table);

    if (result.second) {
      thread_num = getThreadNum();
      p = this;
      ops = &this->operands;
      pthread_mutex_init(&m, 0);
      
      if(thread_num >= 2 && (int)table->size() > 20){
          auto threads = new pthread_t[(unsigned long)thread_num];
          range = (long) table->size() / thread_num;
          for (long i = 0; i < thread_num; i++){
              if(pthread_create(&threads[i], NULL, sub_per_thread, (void *)i) != 0) return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(), "Fail to create thread %d");
          }
          for (int i = 0; i < thread_num; i++) pthread_join(threads[i], NULL);
          delete[] threads;
      }
      else
      {
          range = (long)table->size();
          sub_per_thread(0);
      }
      pthread_mutex_destroy(&m);
    }
    return std::make_unique<RecordCountResult>(Count);
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

std::string SubQuery::toString() {
  return "QUERY = SUB " + this->targetTable + "\"";
}
