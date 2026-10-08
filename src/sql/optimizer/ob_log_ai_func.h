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
  explicit LogAIFunc(ObLogPlan &plan) : ObLogicalOperator(plan), ai_expr_(nullptr), solo_(false) {}
  void set_ai_expr(ObRawExpr *ai_expr) { ai_expr_ = ai_expr; }
  ObRawExpr *get_ai_expr() const { return ai_expr_; }
  void set_solo(bool solo) { solo_ = solo; }
  bool is_solo() const { return solo_; }
  const char *get_name() const override
  { return solo_ ? "AI SOLO GLOBAL" : ObLogicalOperator::get_name(); }
  int get_op_exprs(common::ObIArray<ObRawExpr *> &all_exprs) override;
  int allocate_expr_pre(ObAllocExprContext &ctx) override;
  int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
  int est_cost() override;
  int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
private:
  ObRawExpr *ai_expr_;
  bool solo_;
  DISALLOW_COPY_AND_ASSIGN(LogAIFunc);
};

}
}

#endif