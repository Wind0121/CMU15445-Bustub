//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID emit_rid;
  while (child_executor_->Next(&left_tuple, &emit_rid)) {
    Value val = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
    Tuple key = Tuple({val}, &index_info_->key_schema_);
    std::vector<RID> result;
    index_info_->index_->ScanKey(key, &result, exec_ctx_->GetTransaction());
    if (!result.empty()) {
      Tuple right_tuple;
      std::vector<Value> values;
      table_info_->table_->GetTuple(result[0], &right_tuple, exec_ctx_->GetTransaction());
      for (uint32_t idx = 0; idx < child_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), idx));
      }
      for (uint32_t idx = 0; idx < plan_->InnerTableSchema().GetColumnCount(); idx++) {
        values.push_back(right_tuple.GetValue(&plan_->InnerTableSchema(), idx));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      *rid = tuple->GetRid();
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t idx = 0; idx < child_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), idx));
      }
      for (uint32_t idx = 0; idx < plan_->InnerTableSchema().GetColumnCount(); idx++) {
        values.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(idx).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      *rid = tuple->GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
