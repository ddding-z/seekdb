/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_ai_agg.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ExprAIAgg::ExprAIAgg(ObIAllocator &allocator)
    : ObFuncExprOperator(allocator, T_FUN_AI_AGG, N_AI_AGG, MORE_THAN_TWO,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ExprAIAgg::check_param_count(int64_t param_num)
{
  int ret = OB_SUCCESS;
  if (param_num != MIN_PARAM_COUNT && param_num != MAX_PARAM_COUNT) {
    ret = OB_ERR_PARAM_SIZE;
    const ObString name(N_AI_AGG);
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
    LOG_WARN("AI_AGG requires three or four arguments", K(ret), K(param_num));
  }
  return ret;
}

int ExprAIAgg::check_constant_argument(const ObObj &value, int64_t argument_index)
{
  int ret = OB_SUCCESS;
  if (argument_index != MODEL_IDX && argument_index != INSTRUCTION_IDX &&
      argument_index != OPTIONS_IDX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid AI_AGG configuration argument index", K(ret), K(argument_index));
  } else if (value.is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI_AGG model, instruction and options must not be NULL");
    LOG_WARN("AI_AGG configuration argument is NULL", K(ret), K(argument_index));
  } else if (argument_index != OPTIONS_IDX &&
             (!ob_is_string_type(value.get_type()) || value.get_string().empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI_AGG model and instruction must be nonempty strings");
    LOG_WARN("invalid AI_AGG model or instruction", K(ret), K(argument_index), K(value.get_type()));
  }
  return ret;
}

int ExprAIAgg::check_arguments(const ObAggFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.get_expr_type() != T_FUN_AI_AGG) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expected an AI_AGG aggregate expression", K(ret), K(expr.get_expr_type()));
  } else if (OB_FAIL(check_param_count(expr.get_real_param_count()))) {
  } else if (expr.is_param_distinct()) {
    ret = OB_DISTINCT_NOT_ALLOWED;
    LOG_USER_ERROR(OB_DISTINCT_NOT_ALLOWED);
    LOG_WARN("AI_AGG does not support DISTINCT", K(ret));
  } else if (expr.get_order_items().count() != 0 || nullptr != expr.get_separator_param_expr()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI_AGG aggregate ordering or separators");
    LOG_WARN("invalid AI_AGG aggregate modifiers", K(ret));
  }
  for (int64_t index = 0; OB_SUCC(ret) && index < expr.get_real_param_count(); ++index) {
    const ObRawExpr *argument = expr.get_param_expr(index);
    if (OB_ISNULL(argument)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("AI_AGG argument is null", K(ret), K(index));
    } else if (argument->has_flag(IS_AGG) || argument->has_flag(CNT_AGG)) {
      ret = OB_ERR_INVALID_GROUP_FUNC_USE;
      LOG_USER_ERROR(OB_ERR_INVALID_GROUP_FUNC_USE);
      LOG_WARN("AI_AGG cannot contain another aggregate", K(ret), K(index));
    } else if (argument->has_flag(IS_WINDOW_FUNC) || argument->has_flag(CNT_WINDOW_FUNC)) {
      ret = OB_ERR_INVALID_WINDOW_FUNCTION_PLACE;
      LOG_USER_ERROR(OB_ERR_INVALID_WINDOW_FUNCTION_PLACE);
      LOG_WARN("AI_AGG cannot contain a window function", K(ret), K(index));
    } else if (argument->get_expr_type() == T_OP_ROW) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI_AGG does not support ROW arguments");
      LOG_WARN("AI_AGG argument is a row", K(ret), K(index));
    } else if (index != INPUT_IDX && !argument->is_static_scalar_const_expr()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "nonconstant AI_AGG model, instruction or options");
      LOG_WARN("AI_AGG configuration must be static", K(ret), K(index));
    } else if (index != INPUT_IDX && argument->is_const_raw_expr()) {
      const ObObj &value = static_cast<const ObConstRawExpr *>(argument)->get_param();
      if (!value.is_unknown() && OB_FAIL(check_constant_argument(value, index))) {
      }
    }
  }
  return ret;
}

int ExprAIAgg::calc_result_typeN(ObExprResType &type, ObExprResType *types,
                               int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_FAIL(check_param_count(param_num))) {
  } else if (OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("AI_AGG argument types are null", K(ret));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < MIN_PARAM_COUNT; ++index) {
      const ObObjType input_type = types[index].get_type();
      if (index != INPUT_IDX && !ob_is_string_type(input_type)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI_AGG model and instruction must be non-NULL strings");
        LOG_WARN("invalid AI_AGG configuration type", K(ret), K(index), K(input_type));
      } else if (index == INPUT_IDX && input_type != ObNullType && !ob_is_string_type(input_type)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                       "AI_AGG input must be NULL or string; FILE, JSON and ROW are not supported");
        LOG_WARN("invalid AI_AGG row input type", K(ret), K(input_type));
      } else {
        if (input_type == ObNullType) {
          types[index].set_calc_type(ObVarcharType);
        }
        types[index].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        if (index != INPUT_IDX && types[index].is_literal() &&
            OB_FAIL(check_constant_argument(types[index].get_param(), index))) {
        }
      }
    }
    if (OB_SUCC(ret) && param_num == MAX_PARAM_COUNT) {
      if (types[OPTIONS_IDX].get_type() == ObNullType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI_AGG options must not be NULL");
        LOG_WARN("AI_AGG options type is NULL", K(ret));
      } else if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types, OPTIONS_IDX, N_AI_AGG))) {
      }
    }
    if (OB_SUCC(ret)) {
      type.set_type(ObLongTextType);
      type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      type.set_collation_level(CS_LEVEL_IMPLICIT);
      type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);
      type.unset_result_flag(NOT_NULL_FLAG);
      type.unset_result_flag(ZEROFILL_FLAG);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
