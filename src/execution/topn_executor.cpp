#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  auto cmp = [order_bys = plan_->GetOrderBy(), schema = child_executor_->GetOutputSchema()](const Tuple &a,
                                                                                            const Tuple &b) {
    for (const auto &order_by : order_bys) {
      switch (order_by.first) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (order_by.second->Evaluate(&a, schema).CompareLessThan(order_by.second->Evaluate(&b, schema)) ==
              CmpBool::CmpTrue) {
            return true;
          } else if (order_by.second->Evaluate(&a, schema).CompareGreaterThan(order_by.second->Evaluate(&b, schema)) ==
                     CmpBool::CmpTrue) {
            return false;
          }
          break;
        case OrderByType::DESC:
          if (order_by.second->Evaluate(&a, schema).CompareLessThan(order_by.second->Evaluate(&b, schema)) ==
              CmpBool::CmpTrue) {
            return false;
          } else if (order_by.second->Evaluate(&a, schema).CompareGreaterThan(order_by.second->Evaluate(&b, schema)) ==
                     CmpBool::CmpTrue) {
            return true;
          }
          break;
      }
    }
    return false;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> q(cmp);

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    q.push(tuple);
    if (q.size() > plan_->GetN()) {
      q.pop();
    }
  }
  while (!q.empty()) {
    stack_.push(q.top());
    q.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (stack_.empty()) {
    return false;
  }
  *tuple = stack_.top();
  *rid = tuple->GetRid();
  stack_.pop();
  return true;
}

}  // namespace bustub
