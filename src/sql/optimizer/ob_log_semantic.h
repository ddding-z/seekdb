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
  explicit LogSemantic(ObLogPlan &plan) : ObLogicalOperator(plan), query_task_count_(1) {}
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
}
}
#endif
