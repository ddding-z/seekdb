#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_log_ai_embed.h"
#include "sql/optimizer/ob_join_order.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

int LogAIEmbed::get_op_exprs(ObIArray<ObRawExpr *> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(embedding_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
  } else if (OB_FAIL(add_var_to_array_no_dup(all_exprs, embedding_))) {
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < embedding_->get_param_count(); ++index) {
      ret = add_var_to_array_no_dup(all_exprs, embedding_->get_param_expr(index));
    }
  }
  return ret;
}

int LogAIEmbed::allocate_expr_pre(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  ExprProducer *producer = nullptr;
  if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
  } else if (OB_FAIL(ctx.find(embedding_, producer))) {
  } else if (OB_ISNULL(producer)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    producer->producer_id_ = id_;
  }
  return ret;
}

int LogAIEmbed::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  is_fixed = expr == embedding_;
  return OB_SUCCESS;
}

int LogAIEmbed::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = get_child(first_child);
  if (OB_ISNULL(child) || OB_ISNULL(get_plan()) || get_parallel() < 1) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const double op_cost = ObOptEstCost::cost_material(child->get_card() / get_parallel(), child->get_width(),
                                                       get_plan()->get_optimizer_context());
    set_op_cost(op_cost);
    set_cost(child->get_cost() + op_cost);
    set_card(child->get_card());
  }
  return ret;
}

int LogAIEmbed::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = get_child(first_child);
  double child_cost = 0;
  if (OB_ISNULL(child) || OB_ISNULL(get_plan()) || param.need_parallel_ < 1) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(SMART_CALL(child->re_est_cost(param, card, child_cost)))) {
  } else {
    op_cost = ObOptEstCost::cost_material(card / param.need_parallel_, child->get_width(),
                                         get_plan()->get_optimizer_context());
    cost = child_cost + op_cost;
  }
  return ret;
}

}
}