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
#include "ob_expr_ai_input.h"
#include "common/object/file_ref.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
namespace
{
thread_local ISemanticExprRuntime *semantic_expr_runtime = nullptr;

int calc_semantic_input_type(ObExprResType &type, bool allow_json, bool allow_file = false)
{
  int ret = OB_SUCCESS;
  if (type.get_type() == ObNullType) {
    type.set_calc_type(ObVarcharType);
    type.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
  } else if (ob_is_string_type(type.get_type())) {
    type.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
  } else if (allow_file && type.get_type() == ObFileType) {
  } else if (!allow_json || !ob_is_json(type.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid semantic input type", K(ret), K(type), K(allow_json));
  }
  return ret;
}

int calc_semantic_result_type(ObExprResType &type, ObExprResType *types,
                              int64_t param_num, const char *function_name, ObItemType function_type)
{
  int ret = OB_SUCCESS;
  const bool is_join = function_type == T_FUN_SYS_AI_JOIN;
  const int64_t required_args = is_join ? 4 : 2;
  if (param_num < required_args || param_num > required_args + 1) {
    const ObString name(function_name);
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
  } else if (OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("semantic argument types are null", K(ret));
  } else {
    types[0].set_calc_type(ObVarcharType);
    types[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (OB_FAIL(calc_semantic_input_type(types[1], !is_join, !is_join))) {
    }
    for (int64_t index = 2; OB_SUCC(ret) && index < required_args; ++index) {
      ret = calc_semantic_input_type(types[index], true, true);
    }
    if (OB_SUCC(ret) && param_num == required_args + 1) {
      const ObObjType input_type = types[required_args].get_type();
      if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types, required_args, function_name))) {
      } else if (ob_is_string_type(input_type) &&
                 types[required_args].get_collation_type() != CS_TYPE_BINARY &&
                 types[required_args].get_charset_type() != CHARSET_UTF8MB4) {
        types[required_args].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
    if (OB_SUCC(ret)) {
      if (function_type == T_FUN_SYS_AI_FILTER || is_join) {
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

int prepare_semantic_boolean_config(MultimodeAlloctor &allocator, ObJsonObject *&config,
                                    ObItemType function_type)
{
  static const char *boolean_format = R"({
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
  const bool is_join = function_type == T_FUN_SYS_AI_JOIN;
  const char *reserved_error = is_join
      ? "AI_JOIN options must not set the reserved Schema key"
      : "AI_FILTER options must not set the reserved Schema key";
  const char *format_error = is_join
      ? "AI_JOIN response_format conflicts with its mandatory boolean value JSON schema"
      : "AI_FILTER response_format conflicts with its mandatory boolean value JSON schema";
  ObIJsonBase *required_format = nullptr;
  if (nullptr != config && nullptr != config->get_value("Schema")) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, reserved_error);
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, ObString(boolean_format),
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, required_format))) {
  } else if (nullptr == config && OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {
  } else {
    ObJsonNode *format = config->get_value("response_format");
    if (nullptr == format) {
      ObJsonObject *definition = static_cast<ObJsonObject *>(
          static_cast<ObJsonObject *>(required_format)->get_value("json_schema"));
      ObString default_name(is_join ? N_AI_JOIN : N_AI_FILTER);
      ObJsonString *name = nullptr;
      if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, default_name, name))) {
      } else if (OB_FAIL(definition->add("name", name))) {
      } else if (OB_FAIL(config->add("response_format", static_cast<ObJsonNode *>(required_format)))) {
      }
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
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, format_error);
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to prepare semantic boolean output schema", K(ret), K(function_type));
  }
  return ret;
}

int append_compact_semantic_json(ObJsonNode *node, ObJsonBuffer &buffer)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("semantic JSON input node is null", K(ret));
  } else if (node->json_type() == ObJsonNodeType::J_OBJECT) {
    ObJsonObject *object = static_cast<ObJsonObject *>(node);
    if (OB_FAIL(buffer.append("{"))) {
    }
    for (uint64_t index = 0; OB_SUCC(ret) && index < object->element_count(); ++index) {
      ObString key;
      if (index > 0 && OB_FAIL(buffer.append(","))) {
      } else if (OB_FAIL(object->get_key(index, key))) {
      } else {
        ObJsonString key_node(key);
        if (OB_FAIL(key_node.print(buffer, true))) {
        } else if (OB_FAIL(buffer.append(":"))) {
        } else if (OB_FAIL(SMART_CALL(append_compact_semantic_json(object->get_value(index), buffer)))) {
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = buffer.append("}");
    }
  } else if (node->json_type() == ObJsonNodeType::J_ARRAY) {
    ObJsonArray *array = static_cast<ObJsonArray *>(node);
    if (OB_FAIL(buffer.append("["))) {
    }
    for (uint64_t index = 0; OB_SUCC(ret) && index < array->element_count(); ++index) {
      if (index > 0 && OB_FAIL(buffer.append(","))) {
      } else if (OB_FAIL(SMART_CALL(append_compact_semantic_json(array->get_value(index), buffer)))) {
      }
    }
    if (OB_SUCC(ret)) {
      ret = buffer.append("]");
    }
  } else {
    ret = node->print(buffer, true);
  }
  return ret;
}

int get_semantic_join_input(const ObExpr &expr, ObEvalCtx &ctx, MultimodeAlloctor &allocator,
                            uint16_t index, ObJsonNode *&node)
{
  int ret = OB_SUCCESS;
  ObExpr &argument = *expr.args_[index];
  ObDatum *datum = nullptr;
  if (OB_FAIL(allocator.eval_arg(&argument, ctx, datum))) {
  } else if (datum->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI_JOIN rule and inputs must not be SQL NULL");
    LOG_WARN("AI_JOIN input is SQL NULL", K(ret), K(index));
  } else if (index > 1 && argument.datum_meta_.type_ == ObJsonType) {
    ObIJsonBase *document = nullptr;
    bool is_null = false;
    if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, allocator, index, document, is_null, true))) {
    } else if (is_null) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI_JOIN inputs must not be SQL NULL");
      LOG_WARN("AI_JOIN JSON input is SQL NULL", K(ret), K(index));
    } else if (OB_ISNULL(document)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("AI_JOIN JSON document is null", K(ret), K(index));
    } else {
      node = static_cast<ObJsonNode *>(document);
    }
  } else if (!ob_is_string_type(argument.datum_meta_.type_)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid AI_JOIN input type", K(ret), K(index), K(argument.datum_meta_));
  } else {
    ObString text;
    ObJsonString *string = nullptr;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, allocator, *datum,
        argument.datum_meta_, argument.obj_meta_.has_lob_header(), text))) {
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, text, string))) {
    } else {
      node = string;
    }
  }
  return ret;
}

int prepare_semantic_join_input(const ObExpr &expr, ObEvalCtx &ctx, MultimodeAlloctor &allocator,
                                ObString &model_id, ObString &prompt)
{
  int ret = OB_SUCCESS;
  ObDatum *model_datum = nullptr;
  ObString model;
  ObJsonNode *inputs[3] = {nullptr, nullptr, nullptr};
  if (OB_FAIL(allocator.eval_arg(expr.args_[0], ctx, model_datum))) {
  } else if (model_datum->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI_JOIN model must not be SQL NULL");
    LOG_WARN("AI_JOIN model is SQL NULL", K(ret));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, allocator, *model_datum,
      expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), model))) {
  } else if (model.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI_JOIN model must not be empty");
    LOG_WARN("AI_JOIN model is empty", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, model, model_id, true))) {
    LOG_WARN("failed to copy AI_JOIN model", K(ret));
  }
  for (uint16_t index = 1; OB_SUCC(ret) && index <= 3; ++index) {
    if (OB_FAIL(get_semantic_join_input(expr, ctx, allocator, index, inputs[index - 1]))) {
      LOG_WARN("failed to prepare AI_JOIN input", K(ret), K(index));
    }
  }
  if (OB_SUCC(ret)) {
    // ObJsonObject sorts keys; write the envelope in user argument order.
    ObJsonBuffer buffer(&allocator);
    if (OB_FAIL(buffer.append("{\"rule\":"))) {
    } else if (OB_FAIL(append_compact_semantic_json(inputs[0], buffer))) {
    } else if (OB_FAIL(buffer.append(",\"left\":"))) {
    } else if (OB_FAIL(append_compact_semantic_json(inputs[1], buffer))) {
    } else if (OB_FAIL(buffer.append(",\"right\":"))) {
    } else if (OB_FAIL(append_compact_semantic_json(inputs[2], buffer))) {
    } else if (OB_FAIL(buffer.append("}"))) {
    } else {
      prompt = buffer.string();
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to serialize AI_JOIN prompt", K(ret));
    }
  }
  return ret;
}

int append_semantic_argument(const ObExpr &expr, uint16_t index, ObEvalCtx &ctx,
                             MultimodeAlloctor &allocator, SemanticInput &input,
                             bool &ordered)
{
  int ret = OB_SUCCESS;
  const ObExpr &argument = *expr.args_[index];
  ObDatum *datum = nullptr;
  ordered = false;
  if (OB_FAIL(allocator.eval_arg(&argument, ctx, datum))) {
  } else if (datum->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "demanded semantic inputs must not be SQL NULL");
  } else if (argument.datum_meta_.type_ == ObFileType) {
    FileRef reference;
    const ObString encoded = datum->get_string();
    if (OB_FAIL(FileRefCodec::decode(encoded, reference))) {
    } else if (OB_FAIL(input.append(allocator, SemanticInputPart::FILE, encoded))) {
    } else {
      ordered = true;
    }
  } else if (argument.datum_meta_.type_ == ObJsonType) {
    ObIJsonBase *document = nullptr;
    bool is_null = false;
    if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, allocator, index, document, is_null))) {
    } else if (is_null || nullptr == document) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "demanded semantic inputs must not be SQL NULL");
    } else if (OB_FAIL(SemanticInput::is_input_object(*document, ordered))) {
    } else if (ordered) {
      ordered = true;
      ret = input.append_json(allocator, *document);
    } else {
      ObJsonBuffer buffer(&allocator);
      if (OB_FAIL(append_compact_semantic_json(static_cast<ObJsonNode *>(document), buffer))) {
      } else if (OB_FAIL(input.append(allocator, SemanticInputPart::TEXT, buffer.string()))) {
      }
    }
  } else {
    ObString text;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, allocator, *datum,
        argument.datum_meta_, argument.obj_meta_.has_lob_header(), text))) {
    } else if (OB_FAIL(input.append(allocator, SemanticInputPart::TEXT, text))) {
    }
  }
  return ret;
}

int get_semantic_expr_runtime(const ObExpr &expr, ISemanticExprRuntime *&runtime)
{
  int ret = OB_SUCCESS;
  runtime = ISemanticExprRuntime::current();
  if (nullptr == runtime) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, expr.type_ == T_FUN_SYS_AI_JOIN
        ? "AI_JOIN requires a semantic join execution operator"
        : "AI_MAP and AI_FILTER require a semantic execution operator");
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
  return type == T_FUN_SYS_AI_MAP || type == T_FUN_SYS_AI_FILTER || type == T_FUN_SYS_AI_JOIN;
}

int SemanticExprUtils::contains_semantic(const ObRawExpr *expr, bool &contains)
{
  return contains_semantic(expr, T_INVALID, contains);
}

int SemanticExprUtils::contains_semantic_join(const ObRawExpr *expr, bool &contains)
{
  return contains_semantic(expr, T_FUN_SYS_AI_JOIN, contains);
}

int SemanticExprUtils::contains_semantic(const ObRawExpr *expr, ObItemType type, bool &contains)
{
  int ret = OB_SUCCESS;
  contains = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expression while checking semantic dependencies", K(ret));
  } else if (type == T_INVALID ? is_semantic(expr->get_expr_type()) : expr->get_expr_type() == type) {
    contains = true;
  } else if (!expr->is_column_ref_expr() && !expr->is_aggr_expr()) {
    for (int64_t index = 0; OB_SUCC(ret) && !contains && index < expr->get_param_count(); ++index) {
      ret = SMART_CALL(contains_semantic(expr->get_param_expr(index), type, contains));
    }
  }
  return ret;
}

int SemanticExprUtils::prepare_input(const ObExpr &expr, ObEvalCtx &ctx,
                                    MultimodeAlloctor &allocator, ObString &model_id,
                                    ObString &prompt, ObJsonObject *&config, bool prepare_config)
{
  int ret = OB_SUCCESS;
  const bool is_join = expr.type_ == T_FUN_SYS_AI_JOIN;
  const uint32_t required_args = is_join ? 4 : 2;
  config = nullptr;
  if (!is_semantic(expr.type_) || expr.arg_cnt_ < required_args ||
      expr.arg_cnt_ > required_args + 1 || nullptr == expr.args_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid semantic expression", K(ret), K(expr.type_), K(expr.arg_cnt_));
  } else {
    for (uint32_t index = 0; OB_SUCC(ret) && index < expr.arg_cnt_; ++index) {
      if (nullptr == expr.args_[index]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("semantic argument expression is null", K(ret), K(index));
      }
    }
    if (OB_SUCC(ret) && is_join) {
      if (OB_FAIL(prepare_semantic_join_input(expr, ctx, allocator, model_id, prompt))) {
        LOG_WARN("failed to prepare semantic join model and prompt", K(ret));
      }
    } else if (OB_SUCC(ret) &&
        OB_FAIL(ObExprAIComplete::prepare_input(expr, ctx, allocator, model_id, prompt, config, nullptr, false))) {
      LOG_WARN("failed to prepare semantic model and prompt", K(ret), K(expr.type_));
    }
    if (OB_SUCC(ret) && prepare_config && expr.arg_cnt_ == required_args + 1) {
      ObIJsonBase *options = nullptr;
      bool is_null = false;
      if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, allocator, required_args, options, is_null))) {
        LOG_WARN("failed to read semantic options", K(ret), K(expr.type_));
      } else if (is_null || nullptr == options || options->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, is_join
            ? "AI_JOIN options must be a JSON object"
            : "AI_MAP and AI_FILTER options must be a JSON object");
        LOG_WARN("semantic options are not a JSON object", K(ret), K(expr.type_));
      } else {
        config = static_cast<ObJsonObject *>(options);
      }
    }
    if (OB_SUCC(ret) && prepare_config && (expr.type_ == T_FUN_SYS_AI_FILTER || is_join) &&
        OB_FAIL(prepare_semantic_boolean_config(allocator, config, expr.type_))) {
    }
  }
  return ret;
}

int SemanticExprUtils::prepare_request_input(const ObExpr &expr, ObEvalCtx &ctx,
                                             MultimodeAlloctor &allocator, ObString &model_id,
                                             SemanticInput &input, ObJsonObject *&config,
                                             bool prepare_config)
{
  int ret = OB_SUCCESS;
  const bool is_join = expr.type_ == T_FUN_SYS_AI_JOIN;
  const uint32_t required_args = is_join ? 4 : 2;
  bool ordered = false;
  config = nullptr;
  if (!is_semantic(expr.type_) || expr.arg_cnt_ < required_args ||
      expr.arg_cnt_ > required_args + 1 || nullptr == expr.args_) {
    ret = OB_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; OB_SUCC(ret) && index < expr.arg_cnt_; ++index) {
    if (nullptr == expr.args_[index]) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  if (OB_SUCC(ret) && is_join) {
    SemanticInput left;
    SemanticInput right;
    bool left_ordered = false;
    bool right_ordered = false;
    if (OB_FAIL(append_semantic_argument(expr, 2, ctx, allocator, left, left_ordered))) {
    } else if (OB_FAIL(append_semantic_argument(expr, 3, ctx, allocator, right, right_ordered))) {
    } else if (left_ordered || right_ordered) {
      ordered = true;
      SemanticInput rule;
      bool rule_ordered = false;
      if (OB_FAIL(append_semantic_argument(expr, 1, ctx, allocator, rule, rule_ordered))) {
      } else if (rule_ordered || rule.parts_.count() != 1) {
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(input.append(allocator, SemanticInputPart::TEXT,
                                     ObString("AI_JOIN rule:\n")))) {
      } else if (OB_FAIL(input.append(allocator, SemanticInputPart::TEXT, rule.parts_.at(0).value_))) {
      } else if (OB_FAIL(input.append(allocator, SemanticInputPart::TEXT,
                                     ObString("\nLEFT INPUT:\n")))) {
      }
      for (int64_t index = 0; OB_SUCC(ret) && index < left.parts_.count(); ++index) {
        const auto &part = left.parts_.at(index);
        ret = input.append(allocator, part.kind_, part.value_);
      }
      if (OB_SUCC(ret) && OB_FAIL(input.append(allocator, SemanticInputPart::TEXT,
                                              ObString("\nRIGHT INPUT:\n")))) {
      }
      for (int64_t index = 0; OB_SUCC(ret) && index < right.parts_.count(); ++index) {
        const auto &part = right.parts_.at(index);
        ret = input.append(allocator, part.kind_, part.value_);
      }
    }
  } else if (OB_SUCC(ret) && expr.args_[1]->datum_meta_.type_ == ObFileType) {
    ret = append_semantic_argument(expr, 1, ctx, allocator, input, ordered);
  } else if (OB_SUCC(ret) && expr.args_[1]->datum_meta_.type_ == ObJsonType) {
    ObIJsonBase *document = nullptr;
    bool is_null = false;
    if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, allocator, 1, document, is_null))) {
    } else if (!is_null && nullptr != document) {
      if (OB_FAIL(SemanticInput::is_input_object(*document, ordered))) {
      } else if (ordered) {
        ret = input.append_json(allocator, *document);
      }
    }
  }
  if (OB_SUCC(ret) && !ordered) {
    ObString prompt;
    if (OB_FAIL(prepare_input(expr, ctx, allocator, model_id, prompt, config, prepare_config))) {
    } else if (OB_FAIL(input.append(allocator, SemanticInputPart::TEXT, prompt))) {
    }
  } else if (OB_SUCC(ret)) {
    ObDatum *model = nullptr;
    ObString text;
    if (OB_FAIL(allocator.eval_arg(expr.args_[0], ctx, model))) {
    } else if (model->is_null()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "semantic model must not be SQL NULL");
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, allocator, *model,
        expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), text))) {
    } else if (text.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "semantic model must not be empty");
    } else if (OB_FAIL(ob_write_string(allocator, text, model_id))) {
    }
    if (OB_SUCC(ret) && prepare_config && expr.arg_cnt_ == required_args + 1) {
      ObIJsonBase *options = nullptr;
      bool is_null = false;
      if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, allocator, required_args, options, is_null))) {
      } else if (is_null || nullptr == options || options->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "semantic options must be a JSON object");
      } else {
        config = static_cast<ObJsonObject *>(options);
      }
    }
    if (OB_SUCC(ret) && prepare_config && (is_join || expr.type_ == T_FUN_SYS_AI_FILTER)) {
      ret = prepare_semantic_boolean_config(allocator, config, expr.type_);
    }
  }
  if (OB_FAIL(ret) && ret != OB_EAGAIN) {
    LOG_WARN("failed to prepare semantic request input", K(ret), K(expr.type_));
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
  } else if (expr.type_ == T_FUN_SYS_AI_FILTER || expr.type_ == T_FUN_SYS_AI_JOIN) {
    ObEvalCtx::TempAllocGuard guard(ctx);
    MultimodeAlloctor allocator(guard.get_allocator());
    ObIJsonBase *document = nullptr;
    if (result.empty()) {
      ret = OB_INVALID_DATA;
    } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, result,
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, document,
        ObJsonParser::JSN_STRICT_FLAG | ObJsonParser::JSN_UNIQUE_FLAG))) {
      LOG_WARN("semantic boolean result is not valid JSON", K(ret), K(expr.type_));
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
      LOG_WARN("semantic result violates its boolean value JSON schema", K(ret), K(expr.type_));
      FORWARD_USER_ERROR(ret, expr.type_ == T_FUN_SYS_AI_JOIN
          ? "AI_JOIN response must be a JSON object containing only a boolean value field"
          : "AI_FILTER response must be a JSON object containing only a boolean value field");
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
  return calc_semantic_result_type(type, types, param_num, get_name(), T_FUN_SYS_AI_MAP);
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
  return calc_semantic_result_type(type, types, param_num, get_name(), T_FUN_SYS_AI_FILTER);
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

ExprAIJoin::ExprAIJoin(ObIAllocator &allocator)
    : ObFuncExprOperator(allocator, T_FUN_SYS_AI_JOIN, N_AI_JOIN, MORE_THAN_ZERO,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ExprAIJoin::calc_result_typeN(ObExprResType &type, ObExprResType *types,
                                 int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  return calc_semantic_result_type(type, types, param_num, get_name(), T_FUN_SYS_AI_JOIN);
}

int ExprAIJoin::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_ai_join;
  rt_expr.eval_batch_func_ = eval_ai_join_batch;
  return OB_SUCCESS;
}

int ExprAIJoin::eval_ai_join(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  return eval_semantic_scalar(expr, ctx, result);
}

int ExprAIJoin::eval_ai_join_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                  const ObBitVector &skip, int64_t size)
{
  return eval_semantic_batch(expr, ctx, skip, size);
}

} // namespace sql
} // namespace oceanbase
