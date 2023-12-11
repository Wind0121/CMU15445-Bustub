//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  Tuple tuple;
  RID rid;
  while(right_child_->Next(&tuple,&rid)){
    auto join_key = plan_->RightJoinKeyExpression().Evaluate(&tuple,plan_->GetRightPlan()->OutputSchema());
    hash_join_table_[HashUtil::HashValue(&join_key)].push_back(tuple);
  }
  while(left_child_->Next(&tuple,&rid)){
    auto join_key = plan_->LeftJoinKeyExpression().Evaluate(&tuple,plan_->GetLeftPlan()->OutputSchema());
    if(hash_join_table_.count(HashUtil::HashValue(&join_key)) > 0) {
      auto right_tuples = hash_join_table_[HashUtil::HashValue(&join_key)];
      for(const auto& right_tuple : right_tuples){
        auto right_join_key = plan_->RightJoinKeyExpression().Evaluate(&right_tuple,plan_->GetRightPlan()->OutputSchema());
        if(right_join_key.CompareEquals(join_key) == CmpBool::CmpTrue){
          std::vector<Value> values;
          values.reserve(plan_->GetLeftPlan()->OutputSchema().GetColumnCount() +
                         plan_->GetRightPlan()->OutputSchema().GetColumnCount());
          for(uint32_t idx = 0;idx < plan_->GetLeftPlan()->OutputSchema().GetColumnCount();idx++){
            values.push_back(tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(),idx));
          }
          for(uint32_t idx = 0;idx < plan_->GetRightPlan()->OutputSchema().GetColumnCount();idx++){
            values.push_back(right_tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(),idx));
          }
          output_tuples_.emplace_back(values,&GetOutputSchema());
        }
      }
    }else if(plan_->GetJoinType() == JoinType::LEFT){
      std::vector<Value> values;
      values.reserve(plan_->GetLeftPlan()->OutputSchema().GetColumnCount() +
                     plan_->GetRightPlan()->OutputSchema().GetColumnCount());
      for(uint32_t idx = 0;idx < plan_->GetLeftPlan()->OutputSchema().GetColumnCount();idx++){
        values.push_back(tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(),idx));
      }
      for(uint32_t idx = 0;idx < plan_->GetRightPlan()->OutputSchema().GetColumnCount();idx++){
        values.push_back(ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(idx).GetType()));
      }
      output_tuples_.emplace_back(values,&GetOutputSchema());
    }
  }
  output_tuples_iter_ = output_tuples_.cbegin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(output_tuples_iter_ == output_tuples_.cend()){
    return false;
  }
  *tuple = *output_tuples_iter_;
  output_tuples_iter_++;
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
