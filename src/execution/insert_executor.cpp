//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan){
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  child_executor_ = std::move(child_executor);
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() {
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(is_end){
    return false;
  }
  int insert_count = 0;
  while(child_executor_->Next(tuple,rid)){
    if(table_info_->table_->InsertTuple(*tuple,rid,exec_ctx_->GetTransaction())){
      for(auto index : table_indexes_){
        auto key = tuple->KeyFromTuple(table_info_->schema_,index->key_schema_,index->index_->GetKeyAttrs());
        index->index_->InsertEntry(key,*rid,exec_ctx_->GetTransaction());
      }
      insert_count++;
    }
  }
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER,insert_count);
  *tuple = Tuple(values,&GetOutputSchema());
  is_end = true;
  return true;
}

}  // namespace bustub
