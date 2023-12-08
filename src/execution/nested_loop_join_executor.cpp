//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while(left_executor_->Next(&tuple,&rid)){
    left_tuples_.push_back(tuple);
  }
  while(right_executor_->Next(&tuple,&rid)){
    right_tuples_.push_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(left_idx_ < static_cast<int>(left_tuples_.size())) {
    while (right_idx_ >= 0 && right_idx_ < static_cast<int>(right_tuples_.size())) {
      if (Match(&left_tuples_[left_idx_], &right_tuples_[right_idx_])) {
        std::vector<Value> values;
        for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          values.push_back(left_tuples_[left_idx_].GetValue(&left_executor_->GetOutputSchema(), idx));
        }
        for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          values.push_back(right_tuples_[right_idx_].GetValue(&right_executor_->GetOutputSchema(), idx));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        *rid = tuple->GetRid();
        right_idx_++;
        is_match = true;
        return true;
      }
      right_idx_++;
    }
    if (!is_match && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        values.push_back(left_tuples_[left_idx_].GetValue(&left_executor_->GetOutputSchema(), idx));
      }
      for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(idx).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      *rid = tuple->GetRid();
      right_idx_ = 0;
      left_idx_++;
      is_match = false;
      return true;
    }
    right_idx_ = 0;
    left_idx_++;
    is_match = false;
  }
  return false;
}

auto NestedLoopJoinExecutor::Match(Tuple *left_tuple,Tuple *right_tuple)-> bool{
  auto value = plan_->Predicate().EvaluateJoin(left_tuple,left_executor_->GetOutputSchema(),
                                               right_tuple,right_executor_->GetOutputSchema());
  return !value.IsNull() && value.GetAs<bool>();
}

}  // namespace bustub
