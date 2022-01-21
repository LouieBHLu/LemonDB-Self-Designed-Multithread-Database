//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_QUERYRESULT_H
#define PROJECT_QUERYRESULT_H

#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>

#include "../db/Table.h"
#include "../utils/formatter.h"

class QueryResult {
public:
  typedef std::unique_ptr<QueryResult> Ptr;

  virtual bool success() = 0;

  virtual bool display() = 0;

  virtual ~QueryResult() = default;

  friend std::ostream &operator<<(std::ostream &os, const QueryResult &table);

protected:
  virtual std::ostream &output(std::ostream &os) const = 0;
};

class FailedQueryResult : public QueryResult {
  const std::string data;

public:
  bool success() override { return false; }

  bool display() override { return false; }
};

class SuceededQueryResult : public QueryResult {
public:
  // virtual void merge(QueryResult::Ptr p) = 0;

  virtual void complete(){}

  bool success() override { return true; }
};

class NullQueryResult : public SuceededQueryResult {
public:
  bool display() override { return false; }

protected:
  std::ostream &output(std::ostream &os) const override { return os << "\n"; }
};

class ErrorMsgResult : public FailedQueryResult {
  std::string msg;

public:
  bool display() override { return false; }

  ErrorMsgResult(const char *qname, const std::string &msg) {
    this->msg = R"(Query "?" failed : ?)"_f % qname % msg;
  }

  ErrorMsgResult(const char *qname, const std::string &table,
                 const std::string &msg) {
    this->msg = R"(Query "?" failed in Table "?" : ?)"_f % qname % table % msg;
  }

protected:
  std::ostream &output(std::ostream &os) const override {
    return os << msg << "\n";
  }
};

class SuccessMsgResult :public SuceededQueryResult {
  std::string msg;
  bool stdout;
public:
  bool display() override { return stdout; }

  explicit SuccessMsgResult(const int number) {
    stdout = true;
    this->msg = R"(ANSWER = ?)"_f % number;
  }

  explicit SuccessMsgResult(std::vector<int> results) {
    stdout = true;
    std::stringstream ss;
    ss << "ANSWER = ( ";
    for (auto result : results) {
      ss << result << " ";
    }
    ss << ") ";
    this->msg = ss.str();
  }

  explicit SuccessMsgResult(const char *qname) {
    stdout = false;
    this->msg = R"(Query "?" success.)"_f % qname;
  }

  SuccessMsgResult(const char *qname, const std::string &msg) {
    stdout = false;
    this->msg = R"(Query "?" success : ?)"_f % qname % msg;
  }

  SuccessMsgResult(const char *qname, const std::string &table,
                   const std::string &msg) {
    stdout = false;
    this->msg = R"(Query "?" success in Table "?" : ?)"_f % qname % table % msg;
  }

  explicit SuccessMsgResult(const std::string &output) {
      stdout= true;
      this->msg = R"(?)"_f % output;
  }

protected:
  std::ostream &output(std::ostream &os) const override {
    return os << msg << "\n";
  }
};

class RecordCountResult :public SuceededQueryResult {
  const int affectedRows;

public:
  bool display() override{ return true; }

  explicit RecordCountResult(int count) : affectedRows(count) {}

protected:
  std::ostream &output(std::ostream &os) const override{
    return os << "Affected ? rows."_f % affectedRows << "\n";
  }
};

class RecordSelectResult : public SuceededQueryResult {
  std::vector<std::vector<std::string>> results;

public:
  bool display() override{ return true; }

  RecordSelectResult(){}

  RecordSelectResult(std::vector<std::vector<std::string>>&& results): results(std::move(results)){}

  RecordSelectResult(const std::vector<std::vector<std::string>>& results): results(results){}

  // void merge(QueryResult::Ptr p) override {
  //     // TODO
  //     printf("merging\n");
  //   }

  void sort_results(){
      std::sort(results.begin(), results.end(), [](const std::vector<std::string>& r1, const std::vector<std::string>& r2){
        return r1[0] < r2[0];
      });
  }

protected:
  std::ostream &output(std::ostream &os) const override{
    for(const std::vector<std::string>& row : results){
      os << "( ";
      for(const std::string& one : row) os << one << " ";
      os << ")\n";
    }
    return os;
  }

};

// class DuplicateQueryResult: public SuceededQueryResult {
// public:
//   std::vector<Table::Datum> d;

//   bool display() override {return true;}
  
//   Table::SizeType getsize(){return d.size();}; 

//   DuplicateQueryResult(std::vector<Table::Datum> &&d): d(d) {}

// protected:
//   std::ostream &output(std::ostream &os) const override{
//     return os << "dup task result\n";
//   }
// };

class SumQueryResult : public SuceededQueryResult{
  std::vector<int> results;
public:
  bool display() override {return true;}
  
  SumQueryResult(std::vector<int> &&answers): results(answers){}

protected:
    std::ostream &output(std::ostream &os) const override{
      std::stringstream ss;
      ss << "ANSWER = ( ";
      for(auto result : results) ss << result << " ";
      ss << ") ";
      std::string m = ss.str();
      return os << m << "\n";
    }
};

class ListenQueryResult : public SuceededQueryResult{
  std::string filename;

public:
  bool display() override {return true;}

  ListenQueryResult(std::string filename) : filename(filename) {}

protected:
  std::ostream &output(std::ostream &os) const override{
    return os << "ANSWER = ( listening from ? )"_f % filename << "\n";
  }
};

// class MaxQueryResult : public SuceededQueryResult{
//   std::vector<Table::ValueType> maxValues;
// public:
//   bool display() override {return true;}

//   MaxQueryResult(std::vector<Table::ValueType> &&answers): maxValues(answers){}

// protected:
//     // TODO: test output format
//     std::ostream &output(std::ostream &os) const override{
//       std::stringstream ss;
//       ss << "ANSWER = ( ";
//       for(auto max : maxValues) ss << max << " ";
//       ss << ") ";
//       std::string m = ss.str();
//       return os << m << "\n";
//     }
// };

// class MinQueryResult : public SuceededQueryResult{
//   std::vector<Table::ValueType> minValues;
// public:
//   bool display() override {return true;}

//   MinQueryResult(std::vector<Table::ValueType> &&answers): minValues(answers){}
// protected:

//     std::ostream &output(std::ostream &os) const override{
//     std::stringstream ss;
//     ss << "ANSWER = ( ";
//     for(auto min : minValues) ss << min << " ";
//     ss << ") ";
//     std::string m = ss.str();
//     return os << m << "\n";
//   }

// };

// class AddQueryResult : public SuceededQueryResult{
//   int affectedRows;
// public:
//   bool display() override {return true;}

//   AddQueryResult(int answer): affectedRows(answer){}
// protected:

//     std::ostream &output(std::ostream &os) const override{
//       return os << "Affected ? rows."_f % affectedRows << "\n";
//     }
// };

// class SubQueryResult : public SuceededQueryResult{
//   int affectedRows;
// public:
//   bool display() override {return true;}

//   SubQueryResult(int answer): affectedRows(answer){}
// protected:

//   std::ostream &output(std::ostream &os) const override{
//     return os << "Affected ? rows."_f % affectedRows << "\n";
//   }

// };



/* Not necessary, use RecordCountResult instead */
// class SwapQueryResult : public SuceededQueryResult{
//   int affectedRows;
// public:
//   bool display() override {return true;}
  
//   SwapQueryResult(int answer): affectedRows(answer){}
// protected:
//   std::ostream &output(std::ostream &os) const override{
//     return os << "Affected ? rows."_f % affectedRows << "\n";
//   }

// };

#endif // PROJECT_QUERYRESULT_H
