#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_log_semantic.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_semantic.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

int LogSemantic::collect_owned(ObRawExpr *expr, bool local_parent)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expression in semantic stage", K(ret));
  } else if (expr->is_column_ref_expr() || expr->is_aggr_expr() ||
             expr->is_exec_param_expr() ||
             expr->is_const_expr()) {
  } else if (expr->is_query_ref_expr() || expr->is_win_func_expr() ||
             expr->get_expr_type() == T_FUN_SYS_AI_COMPLETE ||
             expr->get_expr_type() == T_FUN_SYS_AI_EMBED ||
             expr->get_expr_type() == T_FUN_SYS_AI_RERANK ||
             expr->get_expr_type() == T_FUN_SYS_RAND ||
             expr->get_expr_type() == T_FUN_SYS_UUID ||
             expr->get_expr_type() == T_FUN_SYS_SLEEP) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "semantic stages require ordinary deterministic SQL expressions");
  } else {
    bool local = false;
    if (expr->get_expr_type() != T_OP_ROW && !is_shared(expr)) {
      ret = add_var_to_array_no_dup(owned_exprs_, expr);
      if (OB_SUCC(ret) && !SemanticExprUtils::is_semantic(expr->get_expr_type())) {
        if (OB_FAIL(SemanticExprUtils::contains_semantic(expr, local))) {
        } else if (local || local_parent) {
          local = true;
          ret = add_var_to_array_no_dup(local_exprs_, expr);
        }
      }
    }
    if (OB_SUCC(ret) && SemanticExprUtils::is_semantic(expr->get_expr_type())) {
      ret = add_var_to_array_no_dup(semantic_exprs_, expr);
    }
    for (int64_t index = 0; OB_SUCC(ret) && index < expr->get_param_count(); ++index) {
      ret = SMART_CALL(collect_owned(expr->get_param_expr(index), local));
    }
  }
  return ret;
}

int LogSemantic::add_expression(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_var_to_array_no_dup(expressions_, expr))) {
  } else if (OB_FAIL(collect_owned(expr))) {
  }
  return ret;
}

bool LogSemantic::is_shared(const ObRawExpr *expr) const
{
  bool shared = false;
  for (int64_t index = 0; !shared && index < shared_exprs_.count(); ++index) {
    shared = expr == shared_exprs_.at(index);
  }
  return shared;
}

int LogSemantic::add_shared_expression(ObRawExpr *expr)
{
  return add_var_to_array_no_dup(shared_exprs_, expr);
}

int LogSemantic::add_carried_expression(ObRawExpr *expr)
{
  return add_var_to_array_no_dup(carried_exprs_, expr);
}

int LogSemantic::get_op_exprs(ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_op_exprs(exprs))) {
  } else if (OB_FAIL(append_array_no_dup(exprs, expressions_))) {
  } else if (OB_FAIL(append_array_no_dup(exprs, shared_exprs_))) {
  } else if (OB_FAIL(append_array_no_dup(exprs, carried_exprs_))) {
  } else if (OB_FAIL(append_array_no_dup(exprs, owned_exprs_))) {
  }
  return ret;
}

int LogSemantic::allocate_expr_pre(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < owned_exprs_.count(); ++index) {
      ExprProducer *producer = nullptr;
      if (OB_FAIL(ctx.find(owned_exprs_.at(index), producer))) {
      } else if (nullptr == producer) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("semantic expression was not allocated", K(ret));
      } else {
        producer->producer_id_ = id_;
      }
    }
  }
  return ret;
}

int LogSemantic::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
  } else {
    for (int64_t index = output_exprs_.count() - 1; OB_SUCC(ret) && index >= 0; --index) {
      ObRawExpr *expr = output_exprs_.at(index);
      if (ObOptimizerUtil::find_item(local_exprs_, expr) &&
          !ObOptimizerUtil::find_item(expressions_, expr) &&
          !is_child_output_exprs(expr)) {
        // A guarded argument recipe is not a completed relational value.
        ret = output_exprs_.remove(index);
      }
    }
  }
  return ret;
}

int LogSemantic::is_my_fixed_expr(const ObRawExpr *expr, bool &fixed)
{
  fixed = false;
  for (int64_t index = 0; !fixed && index < owned_exprs_.count(); ++index) {
    fixed = expr == owned_exprs_.at(index);
  }
  return OB_SUCCESS;
}

int LogSemantic::est_cost()
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator *child = get_child(first_child);
  if (OB_ISNULL(child) || OB_ISNULL(get_plan()) || get_parallel() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid semantic cost input", K(ret));
  } else {
    const double cost = ObOptEstCost::cost_material(child->get_card() / get_parallel(),
        child->get_width(), get_plan()->get_optimizer_context());
    set_card(child->get_card());
    set_op_cost(cost);
    set_cost(child->get_cost() + cost);
  }
  return ret;
}

int LogSemantic::do_re_est_cost(EstimateCostInfo &param, double &card,
                               double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  double child_cost = 0;
  ObLogicalOperator *child = get_child(first_child);
  if (OB_ISNULL(child) || OB_ISNULL(get_plan()) || param.need_parallel_ < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid semantic recost input", K(ret));
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
