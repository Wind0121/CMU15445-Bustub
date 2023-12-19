//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)) {}

void SeqScanExecutor::Init() {
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool flag = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                         LockManager::LockMode::INTENTION_SHARED, plan_->table_oid_);
      if (!flag) {
        throw ExecutionException("Get Table Lock Fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("Get Table Lock Fail");
    }
  }
  table_iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_ == table_info_->table_->End()) {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      try {
        const auto s_row_lock_set = exec_ctx_->GetTransaction()->GetSharedRowLockSet()->at(plan_->table_oid_);
        for (const auto &lock_rid : s_row_lock_set) {
          bool flag = exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->table_oid_, lock_rid);
          if (!flag) {
            throw ExecutionException("Unlock Fail");
          }
        }

        bool flag = exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), plan_->table_oid_);
        if (!flag) {
          throw ExecutionException("Unlock Fail");
        }
      } catch (TransactionAbortException &e) {
        throw ExecutionException("Unlock Fail");
      }
    }
    return false;
  }
  // Get Row Lock
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool flag = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                       plan_->table_oid_, table_iterator_->GetRid());
      if (!flag) {
        throw ExecutionException("Get Row Lock Fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("Get Row Lock Fail");
    }
  }
  *tuple = *table_iterator_;
  *rid = table_iterator_->GetRid();
  table_iterator_++;
  return true;
}

}  // namespace bustub
