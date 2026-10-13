#ifndef OCEANBASE_SQL_OPTIMIZER_LOG_SEMANTIC_H_
#define OCEANBASE_SQL_OPTIMIZER_LOG_SEMANTIC_H_

#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{
class LogSemantic : public ObLogicalOperator
{
public:
  explicit LogSemantic(ObLogPlan &plan)
      : ObLogicalOperator(plan), query_task_count_(1), lifted_join_count_(0),
        lift_stop_reason_(nullptr), function_cache_enabled_(false) {}
  int add_expression(ObRawExpr *expr);
  int add_shared_expression(ObRawExpr *expr);
  int add_carried_expression(ObRawExpr *expr);
  const common::ObIArray<ObRawExpr *> &expressions() const { return expressions_; }
  const common::ObIArray<ObRawExpr *> &shared_exprs() const { return shared_exprs_; }
  const common::ObIArray<ObRawExpr *> &carried_exprs() const { return carried_exprs_; }
  const common::ObIArray<ObRawExpr *> &semantic_exprs() const { return semantic_exprs_; }
  const common::ObIArray<ObRawExpr *> &owned_exprs() const { return owned_exprs_; }
  const common::ObIArray<ObRawExpr *> &local_exprs() const { return local_exprs_; }
  void set_query_task_count(int64_t count) { query_task_count_ = count; }
  int64_t query_task_count() const { return query_task_count_; }
  void set_function_cache_enabled(bool enabled) { function_cache_enabled_ = enabled; }
  bool function_cache_enabled() const { return function_cache_enabled_; }
  void record_lifted_join() { ++lifted_join_count_; lift_stop_reason_ = nullptr; }
  void set_lift_stop_reason(const char *reason) { lift_stop_reason_ = reason; }
  int get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item) override;
  int get_op_exprs(common::ObIArray<ObRawExpr *> &exprs) override;
  int allocate_expr_pre(ObAllocExprContext &ctx) override;
  int allocate_expr_post(ObAllocExprContext &ctx) override;
  int is_my_fixed_expr(const ObRawExpr *expr, bool &fixed) override;
  int est_cost() override;
  int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
private:
  int collect_owned(ObRawExpr *expr, bool local_parent = false);
  bool is_shared(const ObRawExpr *expr) const;
  common::ObSEArray<ObRawExpr *, 8> expressions_;
  common::ObSEArray<ObRawExpr *, 8> shared_exprs_;
  common::ObSEArray<ObRawExpr *, 8> carried_exprs_;
  common::ObSEArray<ObRawExpr *, 8> semantic_exprs_;
  common::ObSEArray<ObRawExpr *, 16> owned_exprs_;
  common::ObSEArray<ObRawExpr *, 16> local_exprs_;
  int64_t query_task_count_;
  int64_t lifted_join_count_;
  const char *lift_stop_reason_;
  bool function_cache_enabled_;
};

class LogSemanticMap : public LogSemantic
{
public:
  explicit LogSemanticMap(ObLogPlan &plan) : LogSemantic(plan) {}
};

class LogSemanticFilter : public LogSemantic
{
public:
  explicit LogSemanticFilter(ObLogPlan &plan) : LogSemantic(plan) {}
};

class LogSemanticJoin : public LogSemantic
{
public:
  explicit LogSemanticJoin(ObLogPlan &plan) : LogSemantic(plan) {}
  int set_candidate_inputs(const ObRelIds &left, const ObRelIds &right);
  int add_join_expression(ObRawExpr *expr);
  int get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item) override;
  const ObRelIds &candidate_left_tables() const { return candidate_left_tables_; }
  const ObRelIds &candidate_right_tables() const { return candidate_right_tables_; }
  const common::ObIArray<ObRawExpr *> &join_exprs() const { return join_exprs_; }
private:
  ObRelIds candidate_left_tables_;
  ObRelIds candidate_right_tables_;
  common::ObSEArray<ObRawExpr *, 4> join_exprs_;
};
}
}
#endif
