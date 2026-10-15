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

int LogSemantic::set_proxy_config(const ObRawExpr &expr, const SemanticProxyConfig &config)
{
  int ret = OB_SUCCESS;
  int64_t position = -1;
  for (int64_t index = 0; position < 0 && index < semantic_exprs_.count(); ++index) {
    if (semantic_exprs_.at(index) == &expr) {
      position = index;
    }
  }
  if (position < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("embedding proxy call is not owned by its semantic stage", K(ret));
  } else if (proxy_configs_.empty()) {
    for (int64_t index = 0; OB_SUCC(ret) && index < semantic_exprs_.count(); ++index) {
      ret = proxy_configs_.push_back(SemanticProxyConfig());
    }
  }
  if (OB_SUCC(ret) && proxy_configs_.count() != semantic_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("embedding proxy bindings do not match semantic calls", K(ret));
  } else if (OB_SUCC(ret)) {
    proxy_configs_.at(position) = config;
  }
  return ret;
}

int64_t LogSemantic::proxy_count() const
{
  int64_t count = 0;
  for (const SemanticProxyConfig &config : proxy_configs_) {
    count += config.enabled_;
  }
  return count;
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

int LogSemantic::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  if (output_order_ == SemanticOutputOrder::READY_ORDER) {
    reset_op_ordering();
    set_is_local_order(false);
    set_is_range_order(false);
  } else {
    ret = ObLogicalOperator::compute_op_ordering();
  }
  return ret;
}

int LogSemantic::get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
  } else if (lifted_join_count_ > 0 || nullptr != lift_stop_reason_ ||
             function_cache_enabled_ || has_embedding_proxy() || row_async_enabled_) {
    BEGIN_BUF_PRINT;
    const bool placement = lifted_join_count_ > 0 || nullptr != lift_stop_reason_;
    if (placement && OB_FAIL(BUF_PRINTF("semantic_placement=%s, crossed_inner_joins=%ld",
                          lifted_join_count_ > 0 ? "LIFTED" : "KEPT", lifted_join_count_))) {
    } else if (nullptr != lift_stop_reason_) {
      ret = BUF_PRINTF(", lift_stop_reason=%s", lift_stop_reason_);
    }
    if (OB_SUCC(ret) && function_cache_enabled_) {
      int64_t memoized = 0;
      for (const ObRawExpr *expr : semantic_exprs_) {
        memoized += nullptr != expr && SemanticExprUtils::supports_function_cache(*expr);
      }
      ret = BUF_PRINTF("%sfunction_cache=LOCAL_QUERY_TEXT, memoized_calls=%ld",
                       placement ? ", " : "", memoized);
    }
    if (OB_SUCC(ret) && has_embedding_proxy()) {
      const int64_t count = proxy_count();
      if (OB_FAIL(BUF_PRINTF(", embedding_proxy=REJECT_ONLY, approximate=true, proxy_calls=%ld",
                            count))) {
      } else if (count == 1) {
        for (const SemanticProxyConfig &config : proxy_configs_) {
          if (OB_SUCC(ret) && config.enabled_) {
            ret = BUF_PRINTF(", threshold=%.17g", config.threshold_);
          }
        }
      } else if (OB_FAIL(BUF_PRINTF(", thresholds=["))) {
      } else {
        int64_t printed = 0;
        for (const SemanticProxyConfig &config : proxy_configs_) {
          if (OB_SUCC(ret) && config.enabled_) {
            ret = BUF_PRINTF("%s%.17g", printed++ > 0 ? "," : "", config.threshold_);
          }
        }
        if (OB_SUCC(ret)) {
          ret = BUF_PRINTF("]");
        }
      }
    }
    if (OB_SUCC(ret) && row_async_enabled_) {
      ret = BUF_PRINTF("%srow_async=%s",
                       placement || function_cache_enabled_ || has_embedding_proxy() ? ", " : "",
                       output_order_ == SemanticOutputOrder::READY_ORDER
                           ? "UNORDERED_READY" : "ORDERED_PREFIX");
    }
    END_BUF_PRINT(plan_item.special_predicates_, plan_item.special_predicates_len_);
  }
  return ret;
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

int LogSemanticJoin::set_candidate_inputs(const ObRelIds &left, const ObRelIds &right)
{
  int ret = OB_SUCCESS;
  if (left.is_empty() || right.is_empty() || left.overlap(right)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid semantic join candidate inputs", K(ret), K(left), K(right));
  } else {
    candidate_left_tables_.reuse();
    candidate_right_tables_.reuse();
    if (OB_FAIL(candidate_left_tables_.add_members(left))) {
    } else if (OB_FAIL(candidate_right_tables_.add_members(right))) {
    }
  }
  return ret;
}

int LogSemanticJoin::add_join_expression(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || expr->get_expr_type() != T_FUN_SYS_AI_JOIN ||
      (expr->get_param_count() != 4 && expr->get_param_count() != 5)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid semantic join expression binding", K(ret), KP(expr));
  } else {
    ret = add_var_to_array_no_dup(join_exprs_, expr);
  }
  return ret;
}

int LogSemanticJoin::get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
  } else {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(BUF_PRINTF("candidate_policy=ALL_SQL_CANDIDATES, "))) {
    } else {
      const ObIArray<ObRawExpr *> &pair_predicates = join_exprs_;
      EXPLAIN_PRINT_EXPRS(pair_predicates, type);
    }
    if (OB_SUCC(ret) && function_cache_enabled()) {
      int64_t memoized = 0;
      for (int64_t index = 0; index < semantic_exprs().count(); ++index) {
        const ObRawExpr *expr = semantic_exprs().at(index);
        memoized += nullptr != expr && SemanticExprUtils::supports_function_cache(*expr);
      }
      ret = BUF_PRINTF(", function_cache=LOCAL_QUERY_TEXT, memoized_calls=%ld", memoized);
    }
    if (OB_SUCC(ret) && has_embedding_proxy()) {
      ret = BUF_PRINTF(", embedding_proxy=REJECT_ONLY, proxy_input=LEFT_RIGHT_TEXT, "
                       "approximate=true, threshold=%.17g", proxy_configs().at(0).threshold_);
    }
    END_BUF_PRINT(plan_item.special_predicates_, plan_item.special_predicates_len_);
  }
  return ret;
}
}
}
