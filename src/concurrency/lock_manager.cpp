//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 1.检查条件
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if(txn->GetState() == TransactionState::SHRINKING){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if(txn->GetState() == TransactionState::SHRINKING){
        if(lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED){
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED
          || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if(txn->GetState() == TransactionState::SHRINKING){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
      }
      break;
  }
  // 2.获取请求队列
  table_lock_map_latch_.lock();
  if(table_lock_map_.find(oid) == table_lock_map_.end()){
    table_lock_map_.emplace(oid,std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  // 3.判断是否为更新
  for(auto lock_request : lock_request_queue->request_queue_){
    if(lock_request->txn_id_ == txn->GetTransactionId()){
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      // 如果已经存在相同类型的锁，直接返回true
      if(lock_request->lock_mode_ == lock_mode){
        return true;
      }
      // 检查upgrade是否满足条件
      bool flag = false;
      switch (lock_request->lock_mode_) {
        case LockMode::SHARED:
          if(lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE){
            flag = true;
          }
          break;
        case LockMode::EXCLUSIVE:
          flag = true;
          break;
        case LockMode::INTENTION_SHARED:
          if(lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE
              && lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE){
            flag = true;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if(lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE){
            flag = true;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if(lock_mode != LockMode::EXCLUSIVE){
            flag = true;
          }
          break;
      }
      if(flag){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // 不能有多个txn同时进行upgrade
      if(lock_request_queue->upgrading_ != INVALID_TXN_ID){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::UPGRADE_CONFLICT);
      }
      // 删除已有的lock
      lock_request_queue->request_queue_.remove(lock_request);
      InsertOrDeleteTableLockSet(txn,lock_request,false);
      // 获取新的lock请求，并放到最高优先级
      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(),lock_mode,oid);
      std::list<std::shared_ptr<LockRequest>>::iterator it;
      for(it = lock_request_queue->request_queue_.begin();it != lock_request_queue->request_queue_.end();it++){
        if(!(*it)->granted_){
          break;
        }
      }
      lock_request_queue->request_queue_.insert(it,upgrade_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      // 此处基本一样
      while(!GrantLock(upgrade_lock_request,lock_request_queue)){
        lock_request_queue->cv_.wait(lock);
        if(txn->GetState() == TransactionState::ABORTED){
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_lock_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      // 要重置upgrading_
      upgrade_lock_request->granted_ = true;
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      InsertOrDeleteTableLockSet(txn,upgrade_lock_request,true);
      if(lock_mode != LockMode::EXCLUSIVE){
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }
  // 4.等待获取锁并返回
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(),lock_mode,oid);
  lock_request_queue->request_queue_.push_back(lock_request);

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);

  while(!GrantLock(lock_request,lock_request_queue)){
    lock_request_queue->cv_.wait(lock);
    if(txn->GetState() == TransactionState::ABORTED){
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  lock_request->granted_ = true;
  InsertOrDeleteTableLockSet(txn,lock_request,true);
  if(lock_mode != LockMode::EXCLUSIVE){
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // 1.检查条件
  // 是否有未释放的行锁
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  if (!(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set->at(oid).empty()) ||
      !(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set->at(oid).empty())) {
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  // 是否不存在锁
  table_lock_map_latch_.lock();
  if(table_lock_map_.find(oid) == table_lock_map_.end()){
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  // 2.获取请求队列
  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  for(auto lock_request : lock_request_queue->request_queue_){
    if(lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_){
      // 3.将锁从请求队列中移除
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();

      if((lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (lock_request->lock_mode_ == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)){
        if(txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED){
          txn->SetState(TransactionState::SHRINKING);
        }
      }

      lock_request_queue->latch_.unlock();
      InsertOrDeleteTableLockSet(txn,lock_request,false);
      return true;
    }
  }
  // 是否不存在锁
  txn->SetState(TransactionState::ABORTED);
  lock_request_queue->latch_.unlock();
  throw TransactionAbortException(txn->GetTransactionId(),AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 1.检查条件
  if(lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if(txn->GetState() == TransactionState::SHRINKING){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if(txn->GetState() == TransactionState::SHRINKING){
        if(lock_mode != LockMode::SHARED){
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if(lock_mode == LockMode::SHARED ){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if(txn->GetState() == TransactionState::SHRINKING){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
      }
      break;
  }
  // 获取行锁时必须获取表锁
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  // 2.获取请求队列
  row_lock_map_latch_.lock();
  if(row_lock_map_.find(rid) == row_lock_map_.end()){
    row_lock_map_.emplace(rid,std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();
  // 3.判断是否为更新
  for(auto lock_request : lock_request_queue->request_queue_){
    if(lock_request->txn_id_ == txn->GetTransactionId()){
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_,std::adopt_lock);
      if(lock_request->lock_mode_ == lock_mode){
        return true;
      }
      // 检查upgrade条件
      if(lock_request->lock_mode_ == LockMode::EXCLUSIVE){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::INCOMPATIBLE_UPGRADE);
      }
      if(lock_request->lock_mode_ == LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // 不能同时upgrade
      if(lock_request_queue->upgrading_ != INVALID_TXN_ID){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::UPGRADE_CONFLICT);
      }
      // 删除已有的lock
      lock_request_queue->request_queue_.remove(lock_request);
      InsertOrDeleteRowLockSet(txn,lock_request,false);
      // 获取新的lock请求，并放到最高优先级
      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(),lock_mode,oid,rid);
      std::list<std::shared_ptr<LockRequest>>::iterator it;
      for(it = lock_request_queue->request_queue_.begin();it != lock_request_queue->request_queue_.end();it++){
        if(!(*it)->granted_){
          break;
        }
      }
      lock_request_queue->request_queue_.insert(it,upgrade_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      // 此处基本一样
      while(!GrantLock(upgrade_lock_request,lock_request_queue)){
        lock_request_queue->cv_.wait(lock);
        if(txn->GetState() == TransactionState::ABORTED){
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_lock_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      // 要重置upgrading_
      upgrade_lock_request->granted_ = true;
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      InsertOrDeleteRowLockSet(txn,upgrade_lock_request,true);
      if(lock_mode != LockMode::EXCLUSIVE){
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }
  // 4.等待获取锁并返回
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(),lock_mode,oid,rid);
  lock_request_queue->request_queue_.push_back(lock_request);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_,std::adopt_lock);

  while(!GrantLock(lock_request,lock_request_queue)){
    lock_request_queue->cv_.wait(lock);
    if(txn->GetState() == TransactionState::ABORTED){
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  lock_request->granted_ = true;
  InsertOrDeleteRowLockSet(txn,lock_request,true);
  if(lock_mode != LockMode::EXCLUSIVE){
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  if(row_lock_map_.find(rid) == row_lock_map_.end()){
    txn->SetState(TransactionState::ABORTED);
    row_lock_map_latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  for(auto lock_request : lock_request_queue->request_queue_){
    if(lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_){
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();

      if((lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (lock_request->lock_mode_ == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)){
        if(txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED){
          txn->SetState(TransactionState::SHRINKING);
        }
      }

      lock_request_queue->latch_.unlock();
      InsertOrDeleteRowLockSet(txn,lock_request,false);
      return true;
    }
  }

  txn->SetState(TransactionState::ABORTED);
  lock_request_queue->latch_.unlock();
  throw TransactionAbortException(txn->GetTransactionId(),AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  txn_node_set_.insert(t1);
  txn_node_set_.insert(t2);
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  while(std::find(waits_for_[t1].begin(),waits_for_[t1].end(),t2) != waits_for_[t1].end()){
    auto it = std::find(waits_for_[t1].begin(),waits_for_[t1].end(),t2);
    waits_for_[t1].erase(it);
  }
}

auto LockManager::DeleteNode(txn_id_t txn_id) -> void{
  waits_for_.erase(txn_id);
  txn_node_set_.erase(txn_id);

  for(const auto& txn_node_id : txn_node_set_){
    RemoveEdge(txn_node_id,txn_id);
  }

}

auto LockManager::Dfs(txn_id_t txn_id) -> bool{
  if(safe_txn_set_.find(txn_id) != safe_txn_set_.end()){
    return false;
  }
  active_txn_set_.insert(txn_id);
  std::sort(waits_for_[txn_id].begin(),waits_for_[txn_id].end());
  for(const txn_id_t& next_txn_id : waits_for_[txn_id]){
    if(active_txn_set_.find(next_txn_id) != active_txn_set_.end()){
      return true;
    }
    if(Dfs(next_txn_id)){
      return true;
    }
  }

  active_txn_set_.erase(txn_id);
  safe_txn_set_.insert(txn_id);
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  active_txn_set_.clear();
  safe_txn_set_.clear();

  for(const txn_id_t& now_txn_id : txn_node_set_){
    if(Dfs(now_txn_id)){
      *txn_id = *active_txn_set_.begin();
      for(const txn_id_t& active_txn_id : active_txn_set_){
        *txn_id = std::max(*txn_id,active_txn_id);
      }
      return true;
    }else{
      active_txn_set_.clear();
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for(const auto& txn_id : txn_node_set_){
    for(const auto& next_txn_id : waits_for_[txn_id]){
      edges.emplace_back(txn_id,next_txn_id);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();

      for(const auto& it : table_lock_map_){
        std::vector<txn_id_t> granted;
        it.second->latch_.lock();
        for(const auto& lock_request : it.second->request_queue_){
          if(lock_request->granted_){
            granted.push_back(lock_request->txn_id_);
          }else{
            for(const auto& granted_txn_id : granted){
              AddEdge(lock_request->txn_id_,granted_txn_id);
            }
          }
          map_txn_oid_[lock_request->txn_id_].push_back(lock_request->oid_);
        }
        it.second->latch_.unlock();
      }

      for(const auto& it : row_lock_map_){
        std::vector<txn_id_t> granted;
        it.second->latch_.lock();
        for(const auto& lock_request : it.second->request_queue_){
          if(lock_request->granted_){
            granted.push_back(lock_request->txn_id_);
          }else{
            for(const auto& granted_txn_id : granted ){
              AddEdge(lock_request->txn_id_,granted_txn_id);
            }
          }
          map_txn_rid_[lock_request->txn_id_].push_back(lock_request->rid_);
        }
        it.second->latch_.unlock();
      }

      table_lock_map_latch_.unlock();
      row_lock_map_latch_.unlock();

      txn_id_t should_abort_txn_id;
      while(HasCycle(&should_abort_txn_id)){
        Transaction* should_abort_txn = TransactionManager::GetTransaction(should_abort_txn_id);
        should_abort_txn->SetState(TransactionState::ABORTED);
        DeleteNode(should_abort_txn_id);

        if(map_txn_oid_.find(should_abort_txn_id) != map_txn_oid_.end()){
          for(const table_oid_t& oid : map_txn_oid_[should_abort_txn_id]){
            table_lock_map_latch_.lock();
            auto lock_request_queue = table_lock_map_[oid];
            lock_request_queue->latch_.lock();
            table_lock_map_latch_.unlock();
            lock_request_queue->cv_.notify_all();
            lock_request_queue->latch_.unlock();
          }
        }

        if(map_txn_rid_.find(should_abort_txn_id) != map_txn_rid_.end()){
          for(const RID& rid : map_txn_rid_[should_abort_txn_id]){
            row_lock_map_latch_.lock();
            auto lock_request_queue = row_lock_map_[rid];
            lock_request_queue->latch_.lock();
            row_lock_map_latch_.unlock();
            lock_request_queue->cv_.notify_all();
            lock_request_queue->latch_.unlock();
          }
        }
      }

      waits_for_.clear();
      txn_node_set_.clear();
      active_txn_set_.clear();
      safe_txn_set_.clear();
      map_txn_oid_.clear();
      map_txn_rid_.clear();

    }
  }
}

auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request,
               const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool{
  for(auto& it : lock_request_queue->request_queue_){
    if(it->granted_){
      switch (lock_request->lock_mode_) {
        case LockMode::SHARED:
          if(it->lock_mode_ != LockMode::INTENTION_SHARED && it->lock_mode_ != LockMode::SHARED){
            return false;
          }
          break;
        case LockMode::EXCLUSIVE:
          return false;
          break;
        case LockMode::INTENTION_SHARED:
          if(it->lock_mode_ == LockMode::EXCLUSIVE){
            return false;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if(it->lock_mode_ != LockMode::INTENTION_SHARED && it->lock_mode_ != LockMode::INTENTION_EXCLUSIVE){
            return false;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if(it->lock_mode_ != LockMode::INTENTION_SHARED){
            return false;
          }
          break;
      }
    }else if(it.get() != lock_request.get()){
      return false;
    }else{
      return true;
    }
  }
  
  return false;
}

auto LockManager::InsertOrDeleteTableLockSet(Transaction *txn,  const std::shared_ptr<LockRequest> &lock_request,
                                             bool insert)-> void{
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if(insert){
        txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      }else{
        txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if(insert){
        txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      }else{
        txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
      if(insert){
        txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      }else{
        txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if(insert){
        txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      }else{
        txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if(insert){
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      }else{
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
  }
}

auto LockManager::InsertOrDeleteRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                           bool insert)-> void{
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if(insert){
        if(txn->GetSharedRowLockSet()->find(lock_request->oid_) == txn->GetSharedRowLockSet()->end()){
          txn->GetSharedRowLockSet()->emplace(lock_request->oid_,std::unordered_set<RID>());
        }
        txn->GetSharedRowLockSet()->find(lock_request->oid_)->second.emplace(lock_request->rid_);
      }else{
        if(txn->GetSharedRowLockSet()->find(lock_request->oid_) == txn->GetSharedRowLockSet()->end()){
          return;
        }
        txn->GetSharedRowLockSet()->find(lock_request->oid_)->second.erase(lock_request->rid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if(insert) {
        if (txn->GetExclusiveRowLockSet()->find(lock_request->oid_) == txn->GetExclusiveRowLockSet()->end()) {
          txn->GetExclusiveRowLockSet()->emplace(lock_request->oid_, std::unordered_set<RID>());
        }
        txn->GetExclusiveRowLockSet()->find(lock_request->oid_)->second.emplace(lock_request->rid_);
      }else{
        if (txn->GetExclusiveRowLockSet()->find(lock_request->oid_) == txn->GetExclusiveRowLockSet()->end()) {
          return;
        }
        txn->GetExclusiveRowLockSet()->find(lock_request->oid_)->second.erase(lock_request->rid_);
      }
      break;
    default:
      break;
  }
}


}  // namespace bustub
