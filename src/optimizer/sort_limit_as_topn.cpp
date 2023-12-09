#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    const auto &limit = limit_plan.GetLimit();

    BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "limit with multiple children?? Impossible!");
    if (limit_plan.GetChildAt(0)->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*limit_plan.GetChildAt(0));
      const auto &order_bys = sort_plan.GetOrderBy();

      BUSTUB_ENSURE(sort_plan.children_.size() == 1, "sort with mutiple children?? Impossible!");

      return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_plan.GetChildAt(0), order_bys, limit);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
