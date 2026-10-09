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
#include "ob_expr_ai_semantic.h"
#include "ob_expr_ai_complete.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
namespace
{
thread_local ISemanticExprRuntime *semantic_expr_runtime = nullptr;

int calc_semantic_result_type(ObExprResType &type, ObExprResType *types,
                              int64_t param_num, const char *function_name, bool is_filter)
{
  int ret = OB_SUCCESS;
  if (param_num < 2 || param_num > 3) {
    const ObString name(function_name);
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
  } else if (OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("semantic argument types are null", K(ret));
  } else {
    types[0].set_calc_type(ObVarcharType);
    types[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (types[1].get_type() == ObNullType) {
      types[1].set_calc_type(ObVarcharType);
      types[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else if (ob_is_string_type(types[1].get_type())) {
      types[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else if (!ob_is_json(types[1].get_type())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid semantic prompt type", K(ret), K(types[1]));
    }
    if (OB_SUCC(ret) && param_num == 3) {
      const ObObjType input_type = types[2].get_type();
      if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types, 2, function_name))) {
      } else if (ob_is_string_type(input_type) && types[2].get_collation_type() != CS_TYPE_BINARY &&
                 types[2].get_charset_type() != CHARSET_UTF8MB4) {
        types[2].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
    if (OB_SUCC(ret)) {
      if (is_filter) {
        type.set_tinyint();
        type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
        type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
      } else {
        type.set_type(ObLongTextType);
        type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        type.set_collation_level(CS_LEVEL_IMPLICIT);
        type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);
      }
    }
  }
  return ret;
}

int prepare_semantic_filter_config(MultimodeAlloctor &allocator, ObJsonObject *&config)
{
  static const char *filter_format = R"({
    "type": "json_schema",
    "json_schema": {
      "name": "ai_filter",
      "strict": true,
      "schema": {
        "type": "object",
        "properties": {"value": {"type": "boolean"}},
        "required": ["value"],
        "additionalProperties": false
      }
    }
  })";
  int ret = OB_SUCCESS;
  ObIJsonBase *required_format = nullptr;
  if (nullptr != config && nullptr != config->get_value("Schema")) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI_FILTER options must not set the reserved Schema key");
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, ObString(filter_format),
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, required_format))) {
  } else if (nullptr == config && OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {
  } else {
    ObJsonNode *format = config->get_value("response_format");
    if (nullptr == format) {
      ret = config->add("response_format", static_cast<ObJsonNode *>(required_format));
    } else if (format->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ObJsonObject *format_object = static_cast<ObJsonObject *>(format);
      ObJsonNode *format_type = format_object->get_value("type");
      ObJsonNode *definition = format_object->get_value("json_schema");
      if (nullptr == format_type || format_type->json_type() != ObJsonNodeType::J_STRING ||
          ObString(format_type->get_data_length(), format_type->get_data()) != "json_schema" ||
          nullptr == definition || definition->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        ObJsonObject *definition_object = static_cast<ObJsonObject *>(definition);
        ObJsonNode *name = definition_object->get_value("name");
        ObJsonNode *strict = definition_object->get_value("strict");
        ObJsonNode *schema = definition_object->get_value("schema");
        int comparison = 0;
        ObJsonNode *required_schema = static_cast<ObJsonObject *>(
            static_cast<ObJsonObject *>(required_format)->get_value("json_schema"))->get_value("schema");
        if (nullptr == name || name->json_type() != ObJsonNodeType::J_STRING || name->get_data_length() == 0 ||
            nullptr == strict || strict->json_type() != ObJsonNodeType::J_BOOLEAN || !strict->get_boolean() ||
            nullptr == schema || schema->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_INVALID_ARGUMENT;
        } else if (OB_FAIL(required_schema->compare(*schema, comparison))) {
        } else if (comparison != 0) {
          ret = OB_INVALID_ARGUMENT;
        }
      }
    }
    if (ret == OB_INVALID_ARGUMENT) {
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI_FILTER response_format conflicts with its mandatory boolean value JSON schema");
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to prepare AI_FILTER output schema", K(ret));
  }
  return ret;
}

int get_semantic_expr_runtime(const ObExpr &expr, ISemanticExprRuntime *&runtime)
{
  int ret = OB_SUCCESS;
  runtime = ISemanticExprRuntime::current();
  if (nullptr == runtime) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI_MAP and AI_FILTER require a semantic execution operator");
    LOG_WARN("semantic expression runtime is not installed", K(ret), K(expr.type_));
  }
  return ret;
}

int eval_semantic_scalar(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  int ret = OB_SUCCESS;
  ISemanticExprRuntime *runtime = nullptr;
  if (OB_FAIL(get_semantic_expr_runtime(expr, runtime))) {
  } else if (OB_FAIL(runtime->evaluate(expr, ctx, result)) && ret != OB_EAGAIN) {
    LOG_WARN("semantic expression evaluation failed", K(ret), K(expr.type_));
  }
  return ret;
}

int eval_semantic_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, int64_t size)
{
  int ret = OB_SUCCESS;
  ISemanticExprRuntime *runtime = nullptr;
  if (OB_FAIL(get_semantic_expr_runtime(expr, runtime))) {
  } else if (size < 0 || size > ctx.max_batch_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid semantic expression batch size", K(ret), K(size), K(ctx.max_batch_size_));
  } else {
    ObBitVector &evaluated = expr.get_evaluated_flags(ctx);
    ObDatumVector output = expr.locate_expr_datumvector(ctx);
    ObEvalCtx::BatchInfoScopeGuard batch_guard(ctx);
    batch_guard.set_batch_size(size);
    bool pending = false;
    for (int64_t row = 0; OB_SUCC(ret) && row < size; ++row) {
      if (skip.at(row) || evaluated.at(row)) {
        continue;
      }
      batch_guard.set_batch_idx(row);
      if (OB_FAIL(ctx.exec_ctx_.check_status())) {
        LOG_WARN("semantic expression execution status failed", K(ret));
      } else {
        const int eval_ret = runtime->evaluate(expr, ctx, *output.at(row));
        if (eval_ret == OB_EAGAIN) {
          pending = true;
        } else if (eval_ret != OB_SUCCESS) {
          ret = eval_ret;
          LOG_WARN("semantic batch evaluation failed", K(ret), K(expr.type_), K(row));
        } else {
          evaluated.set(row);
          if (output.at(row)->is_null()) {
            expr.get_eval_info(ctx).notnull_ = false;
          }
        }
      }
    }
    if (OB_SUCC(ret) && pending) {
      ret = OB_EAGAIN;
    }
    if (OB_FAIL(ret)) {
      // The core batch evaluator clears every datum on error, including EAGAIN.
      evaluated.reset(size);
    }
  }
  return ret;
}
} // namespace

ISemanticExprRuntime *ISemanticExprRuntime::current()
{
  return semantic_expr_runtime;
}

SemanticExprRuntimeGuard::SemanticExprRuntimeGuard(ISemanticExprRuntime *runtime)
    : previous_(semantic_expr_runtime)
{
  semantic_expr_runtime = runtime;
}

SemanticExprRuntimeGuard::SemanticExprRuntimeGuard(ISemanticExprRuntime &runtime)
    : SemanticExprRuntimeGuard(&runtime)
{
}

SemanticExprRuntimeGuard::~SemanticExprRuntimeGuard()
{
  semantic_expr_runtime = previous_;
}

bool SemanticExprUtils::is_semantic(ObItemType type)
{
  return type == T_FUN_SYS_AI_MAP || type == T_FUN_SYS_AI_FILTER;
}

int SemanticExprUtils::contains_semantic(const ObRawExpr *expr, bool &contains)
{
  int ret = OB_SUCCESS;
  contains = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expression while checking semantic dependencies", K(ret));
  } else if (is_semantic(expr->get_expr_type())) {
    contains = true;
  } else if (!expr->is_column_ref_expr() && !expr->is_aggr_expr()) {
    for (int64_t index = 0; OB_SUCC(ret) && !contains && index < expr->get_param_count(); ++index) {
      ret = SMART_CALL(contains_semantic(expr->get_param_expr(index), contains));
    }
  }
  return ret;
}

int SemanticExprUtils::prepare_input(const ObExpr &expr, ObEvalCtx &ctx,
                                    MultimodeAlloctor &allocator, ObString &model_id,
                                    ObString &prompt, ObJsonObject *&config, bool prepare_config)
{
  int ret = OB_SUCCESS;
  config = nullptr;
  if (!is_semantic(expr.type_) || expr.arg_cnt_ < 2 || expr.arg_cnt_ > 3 || nullptr == expr.args_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid semantic expression", K(ret), K(expr.type_), K(expr.arg_cnt_));
  } else {
    for (uint32_t index = 0; OB_SUCC(ret) && index < expr.arg_cnt_; ++index) {
      if (nullptr == expr.args_[index]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("semantic argument expression is null", K(ret), K(index));
      }
    }
    // Share completion prompt validation, but use the JSON-aware options reader.
    if (OB_SUCC(ret) &&
        OB_FAIL(ObExprAIComplete::prepare_input(expr, ctx, allocator, model_id, prompt, config, nullptr, false))) {
      LOG_WARN("failed to prepare semantic model and prompt", K(ret), K(expr.type_));
    }
    if (OB_SUCC(ret) && prepare_config && expr.arg_cnt_ == 3) {
      ObIJsonBase *options = nullptr;
      bool is_null = false;
      if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, allocator, 2, options, is_null))) {
        LOG_WARN("failed to read semantic options", K(ret), K(expr.type_));
      } else if (is_null || nullptr == options || options->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI_MAP and AI_FILTER options must be a JSON object");
        LOG_WARN("semantic options are not a JSON object", K(ret), K(expr.type_));
      } else {
        config = static_cast<ObJsonObject *>(options);
      }
    }
    if (OB_SUCC(ret) && prepare_config && expr.type_ == T_FUN_SYS_AI_FILTER &&
        OB_FAIL(prepare_semantic_filter_config(allocator, config))) {
    }
  }
  return ret;
}

int SemanticExprUtils::set_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum,
                                 const ObString &result)
{
  int ret = OB_SUCCESS;
  if (expr.type_ == T_FUN_SYS_AI_MAP) {
    ObString text = result;
    if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, datum, text))) {
      LOG_WARN("failed to set AI_MAP result", K(ret));
    }
  } else if (expr.type_ == T_FUN_SYS_AI_FILTER) {
    ObEvalCtx::TempAllocGuard guard(ctx);
    MultimodeAlloctor allocator(guard.get_allocator());
    ObIJsonBase *document = nullptr;
    if (result.empty()) {
      ret = OB_INVALID_DATA;
    } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, result,
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, document,
        ObJsonParser::JSN_STRICT_FLAG | ObJsonParser::JSN_UNIQUE_FLAG))) {
      LOG_WARN("AI_FILTER result is not valid JSON", K(ret));
      if (ret == OB_ERR_INVALID_JSON_TEXT || ret == OB_ERR_DUPLICATE_KEY) {
        ret = OB_INVALID_DATA;
      }
    } else if (nullptr == document || document->json_type() != ObJsonNodeType::J_OBJECT ||
               document->element_count() != 1) {
      ret = OB_INVALID_DATA;
    } else {
      ObJsonNode *value = static_cast<ObJsonObject *>(document)->get_value("value");
      if (nullptr == value || value->json_type() != ObJsonNodeType::J_BOOLEAN) {
        ret = OB_INVALID_DATA;
      } else {
        datum.set_bool(value->get_boolean());
      }
    }
    if (ret == OB_INVALID_DATA) {
      LOG_WARN("AI_FILTER result violates its boolean value JSON schema", K(ret));
      FORWARD_USER_ERROR(ret, "AI_FILTER response must be a JSON object containing only a boolean value field");
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cannot set a non-semantic expression result", K(ret), K(expr.type_));
  }
  if (OB_FAIL(ret)) {
    datum.set_null();
  }
  return ret;
}

ExprAIMap::ExprAIMap(ObIAllocator &allocator)
    : ObFuncExprOperator(allocator, T_FUN_SYS_AI_MAP, N_AI_MAP, MORE_THAN_ZERO,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ExprAIMap::calc_result_typeN(ObExprResType &type, ObExprResType *types,
                                int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  return calc_semantic_result_type(type, types, param_num, get_name(), false);
}

int ExprAIMap::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_ai_map;
  rt_expr.eval_batch_func_ = eval_ai_map_batch;
  return OB_SUCCESS;
}

int ExprAIMap::eval_ai_map(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  return eval_semantic_scalar(expr, ctx, result);
}

int ExprAIMap::eval_ai_map_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                const ObBitVector &skip, int64_t size)
{
  return eval_semantic_batch(expr, ctx, skip, size);
}

ExprAIFilter::ExprAIFilter(ObIAllocator &allocator)
    : ObFuncExprOperator(allocator, T_FUN_SYS_AI_FILTER, N_AI_FILTER, MORE_THAN_ZERO,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ExprAIFilter::calc_result_typeN(ObExprResType &type, ObExprResType *types,
                                   int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  return calc_semantic_result_type(type, types, param_num, get_name(), true);
}

int ExprAIFilter::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_ai_filter;
  rt_expr.eval_batch_func_ = eval_ai_filter_batch;
  return OB_SUCCESS;
}

int ExprAIFilter::eval_ai_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  return eval_semantic_scalar(expr, ctx, result);
}

int ExprAIFilter::eval_ai_filter_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                     const ObBitVector &skip, int64_t size)
{
  return eval_semantic_batch(expr, ctx, skip, size);
}

} // namespace sql
} // namespace oceanbase
