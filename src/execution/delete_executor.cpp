//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void DeleteExecutor::Init() {
  child_executor_->Init();
  try {
    bool flag = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                       LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
    if (!flag) {
      throw ExecutionException("Lock Table Fail");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("Lock Table Fail");
  }
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  int delete_count = 0;
  while (child_executor_->Next(tuple, rid)) {
    try {
      bool flag = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                       table_info_->oid_, *rid);
      if (!flag) {
        throw ExecutionException("Lock Row Fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("Lock Row Fail");
    }

    if (table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      for (auto index : table_indexes_) {
        auto key = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
      }
      delete_count++;
    }
  }
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, delete_count);
  *tuple = Tuple(values, &GetOutputSchema());
  is_end_ = true;
  return true;
}

}  // namespace bustub
