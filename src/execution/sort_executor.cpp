#include "execution/executors/sort_executor.h"
#include "type/value.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }
  std::sort(
      tuples_.begin(), tuples_.end(),
      [order_bys = plan_->GetOrderBy(), schema = child_executor_->GetOutputSchema()](const Tuple &a, const Tuple &b) {
        for (const auto &order_by : order_bys) {
          switch (order_by.first) {
            case OrderByType::INVALID:
            case OrderByType::DEFAULT:
            case OrderByType::ASC:
              if (order_by.second->Evaluate(&a, schema).CompareLessThan(order_by.second->Evaluate(&b, schema)) ==
                  CmpBool::CmpTrue) {
                return true;
              } else if (order_by.second->Evaluate(&a, schema)
                             .CompareGreaterThan(order_by.second->Evaluate(&b, schema)) == CmpBool::CmpTrue) {
                return false;
              }
              break;
            case OrderByType::DESC:
              if (order_by.second->Evaluate(&a, schema).CompareLessThan(order_by.second->Evaluate(&b, schema)) ==
                  CmpBool::CmpTrue) {
                return false;
              } else if (order_by.second->Evaluate(&a, schema)
                             .CompareGreaterThan(order_by.second->Evaluate(&b, schema)) == CmpBool::CmpTrue) {
                return true;
              }
              break;
          }
        }
        return false;
      });
  iterator_ = tuples_.cbegin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == tuples_.cend()) {
    return false;
  }
  *tuple = *iterator_;
  *rid = tuple->GetRid();
  iterator_++;
  return true;
}

}  // namespace bustub
