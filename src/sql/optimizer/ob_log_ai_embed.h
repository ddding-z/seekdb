#ifndef OCEANBASE_SQL_OB_LOG_AI_EMBED_H_
#define OCEANBASE_SQL_OB_LOG_AI_EMBED_H_

#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{

class LogAIEmbed : public ObLogicalOperator
{
public:
  explicit LogAIEmbed(ObLogPlan &plan) : ObLogicalOperator(plan), embedding_(nullptr) {}
  void set_embedding(ObRawExpr *embedding) { embedding_ = embedding; }
  ObRawExpr *get_embedding() const { return embedding_; }
  int get_op_exprs(common::ObIArray<ObRawExpr *> &all_exprs) override;
  int allocate_expr_pre(ObAllocExprContext &ctx) override;
  int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
  int est_cost() override;
  int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
private:
  ObRawExpr *embedding_;
  DISALLOW_COPY_AND_ASSIGN(LogAIEmbed);
};

}
}

#endif