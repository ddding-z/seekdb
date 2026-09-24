#ifndef OCEANBASE_SQL_OB_LOG_AI_FUNC_H_
#define OCEANBASE_SQL_OB_LOG_AI_FUNC_H_

#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{

class LogAIFunc : public ObLogicalOperator
{
public:
  explicit LogAIFunc(ObLogPlan &plan) : ObLogicalOperator(plan), ai_expr_(nullptr) {}
  void set_ai_expr(ObRawExpr *ai_expr) { ai_expr_ = ai_expr; }
  ObRawExpr *get_ai_expr() const { return ai_expr_; }
  int get_op_exprs(common::ObIArray<ObRawExpr *> &all_exprs) override;
  int allocate_expr_pre(ObAllocExprContext &ctx) override;
  int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
  int est_cost() override;
  int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
private:
  ObRawExpr *ai_expr_;
  DISALLOW_COPY_AND_ASSIGN(LogAIFunc);
};

}
}

#endif