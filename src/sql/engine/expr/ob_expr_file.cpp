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
#include "sql/engine/expr/ob_expr_file.h"
#include "common/object/file_ref.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
namespace
{

constexpr char TO_FILE_NAME[] = "to_file";
constexpr char FILE_URI_NAME[] = "file_uri";
constexpr char FILE_META_NAME[] = "file_meta";

int copy_file_expr_result(const ObExpr &expr, ObEvalCtx &ctx,
                          const ObString &value, ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObExprStrResAlloc result_allocator(expr, ctx);
  ObString owned;
  if (OB_FAIL(ob_write_string(result_allocator, value, owned))) {
    LOG_WARN("failed to own FILE expression result", K(ret), K(value.length()));
  } else {
    result.set_string(owned);
  }
  return ret;
}

int eval_file_expr_batch(const ObExpr &expr, ObEvalCtx &ctx,
                         const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("failed to evaluate FILE function input batch", K(ret));
  } else {
    ObDatum *results = expr.locate_batch_datums(ctx);
    ObBitVector &evaluated = expr.get_evaluated_flags(ctx);
    ObEvalCtx::BatchInfoScopeGuard guard(ctx);
    guard.set_batch_size(batch_size);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (!skip.at(i) && !evaluated.at(i)) {
        guard.set_batch_idx(i);
        if (OB_FAIL(expr.eval_func_(expr, ctx, results[i]))) {
          LOG_WARN("failed to evaluate FILE function batch row", K(ret), K(i));
        } else {
          evaluated.set(i);
        }
      }
    }
  }
  return ret;
}

int check_file_expr_input(ObExprResType &input, const char *function_name)
{
  int ret = OB_SUCCESS;
  if (ObFileType != input.get_type() && ObNullType != input.get_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "FILE", ob_obj_type_str(input.get_type()));
    LOG_WARN("FILE accessor requires a FILE argument", K(ret), K(function_name), K(input));
  } else {
    input.set_calc_meta(input.get_obj_meta());
    input.set_calc_accuracy(input.get_accuracy());
  }
  return ret;
}

int check_file_constructor_input_size(const ObExpr &arg, const ObDatum &input)
{
  int ret = OB_SUCCESS;
  int64_t byte_length = input.get_string().length();
  if (is_lob_storage(arg.datum_meta_.type_)) {
    ObLobLocatorV2 locator(input.get_string(), arg.obj_meta_.has_lob_header());
    if (OB_FAIL(locator.get_lob_data_byte_len(byte_length))) {
      LOG_WARN("failed to read TO_FILE descriptor length", K(ret));
    }
  }
  // JSON escaping can expand a bounded reference, but a LOB locator must not allow unbounded materialization.
  if (OB_SUCC(ret) && byte_length > 8 * FileRefCodec::MAX_REF_BYTES) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("TO_FILE descriptor input exceeds the materialization limit", K(ret), K(byte_length));
  }
  return ret;
}

int cg_file_expr(ObExpr &expr, ObExpr::EvalFunc eval_func)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expr.arg_cnt_ != 1) || OB_ISNULL(expr.args_)
      || OB_ISNULL(expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid FILE function expression", K(ret), K(expr.arg_cnt_));
  } else {
    expr.eval_func_ = eval_func;
    expr.eval_batch_func_ = eval_file_expr_batch;
  }
  return ret;
}

} // namespace

ExprToFile::ExprToFile(ObIAllocator &allocator)
    : ObFuncExprOperator(allocator, T_FUN_SYS_TO_FILE, TO_FILE_NAME, 1,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ExprToFile::calc_result_type1(ObExprResType &type, ObExprResType &input,
                                ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (ObNullType != input.get_type() && ObJsonType != input.get_type()
      && !ob_is_string_type(input.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "CHAR or JSON", ob_obj_type_str(input.get_type()));
    LOG_WARN("TO_FILE requires a path or JSON descriptor", K(ret), K(input));
  } else {
    type.set_type(ObFileType);
    type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObFileType]);
    type.set_collation_type(CS_TYPE_BINARY);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    input.set_calc_meta(input.get_obj_meta());
    input.set_calc_accuracy(input.get_accuracy());
  }
  return ret;
}

int ExprToFile::eval_to_file(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObDatum *input = nullptr;
  if (OB_FAIL(expr.args_[0]->eval(ctx, input))) {
    LOG_WARN("failed to evaluate TO_FILE input", K(ret));
  } else if (input->is_null()) {
    result.set_null();
  } else if (OB_FAIL(check_file_constructor_input_size(*expr.args_[0], *input))) {
    LOG_WARN("invalid TO_FILE descriptor size", K(ret));
  } else {
    ObEvalCtx::TempAllocGuard guard(ctx);
    MultimodeAlloctor allocator(guard.get_allocator());
    ObString encoded;
    ObIJsonBase *json = nullptr;
    if (ObJsonType == expr.args_[0]->datum_meta_.type_) {
      bool is_null = false;
      if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, allocator, 0, json,
                                                is_null, false, false, true))) {
        LOG_WARN("failed to read TO_FILE JSON descriptor", K(ret));
      } else if (OB_ISNULL(json) || is_null) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid TO_FILE JSON descriptor", K(ret));
      } else if (OB_FAIL(FileRefCodec::from_json(allocator, *json, encoded))) {
        LOG_WARN("invalid TO_FILE JSON descriptor", K(ret));
      }
    } else {
      ObString value;
      ObString utf8_value;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
            ctx.exec_ctx_, allocator, *input, expr.args_[0]->datum_meta_,
            expr.args_[0]->obj_meta_.has_lob_header(), value))) {
        LOG_WARN("failed to read TO_FILE path or descriptor", K(ret));
      } else if (OB_FAIL(ObExprUtil::convert_string_collation(
            value, expr.args_[0]->datum_meta_.cs_type_, utf8_value, CS_TYPE_UTF8MB4_BIN, allocator))) {
        LOG_WARN("failed to convert TO_FILE descriptor to UTF8", K(ret));
      } else {
        ObString trimmed = utf8_value;
        trimmed.trim();
        if (!trimmed.empty() && ('{' == trimmed.ptr()[0] || '[' == trimmed.ptr()[0])) {
          if (OB_FAIL(ObJsonBaseFactory::get_json_base(
                &allocator, trimmed, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE,
                json, ObJsonParser::JSN_PRESERVE_DUP_FLAG,
                ObJsonExprHelper::get_json_max_depth_config()))) {
            LOG_WARN("invalid TO_FILE JSON text descriptor", K(ret));
          } else if (OB_ISNULL(json)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("TO_FILE JSON parser returned no descriptor", K(ret));
          } else if (OB_FAIL(FileRefCodec::from_json(allocator, *json, encoded))) {
            LOG_WARN("invalid TO_FILE JSON text descriptor", K(ret));
          }
        } else if (OB_FAIL(FileRefCodec::from_path(allocator, utf8_value, encoded))) {
          LOG_WARN("invalid TO_FILE path", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(copy_file_expr_result(expr, ctx, encoded, result))) {
      LOG_WARN("failed to copy TO_FILE result", K(ret));
    }
  }
  return ret;
}

int ExprToFile::cg_expr(ObExprCGCtx &cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  UNUSED(cg_ctx);
  UNUSED(raw_expr);
  return cg_file_expr(rt_expr, eval_to_file);
}

ExprFileUri::ExprFileUri(ObIAllocator &allocator)
    : ObFuncExprOperator(allocator, T_FUN_SYS_FILE_URI, FILE_URI_NAME, 1,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ExprFileUri::calc_result_type1(ObExprResType &type, ObExprResType &input,
                                 ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_file_expr_input(input, FILE_URI_NAME))) {
  } else {
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_length_semantics(LS_BYTE);
    type.set_length(FileRefCodec::MAX_URI_BYTES);
  }
  return ret;
}

int ExprFileUri::eval_file_uri(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObDatum *input = nullptr;
  ObString uri;
  if (OB_FAIL(expr.args_[0]->eval(ctx, input))) {
    LOG_WARN("failed to evaluate FILE_URI input", K(ret));
  } else if (input->is_null()) {
    result.set_null();
  } else if (OB_FAIL(FileRefCodec::get_uri(input->get_string(), uri))) {
    LOG_WARN("invalid FILE_URI reference", K(ret));
  } else if (OB_FAIL(copy_file_expr_result(expr, ctx, uri, result))) {
    LOG_WARN("failed to copy FILE_URI result", K(ret));
  }
  return ret;
}

int ExprFileUri::cg_expr(ObExprCGCtx &cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  UNUSED(cg_ctx);
  UNUSED(raw_expr);
  return cg_file_expr(rt_expr, eval_file_uri);
}

ExprFileMeta::ExprFileMeta(ObIAllocator &allocator)
    : ObFuncExprOperator(allocator, T_FUN_SYS_FILE_META, FILE_META_NAME, 1,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ExprFileMeta::calc_result_type1(ObExprResType &type, ObExprResType &input,
                                  ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_file_expr_input(input, FILE_META_NAME))) {
  } else {
    type.set_json();
    type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]);
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
  }
  return ret;
}

int ExprFileMeta::eval_file_meta(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObDatum *input = nullptr;
  if (OB_FAIL(expr.args_[0]->eval(ctx, input))) {
    LOG_WARN("failed to evaluate FILE_META input", K(ret));
  } else if (input->is_null()) {
    result.set_null();
  } else {
    ObEvalCtx::TempAllocGuard guard(ctx);
    MultimodeAlloctor allocator(guard.get_allocator());
    ObString json_text;
    ObIJsonBase *json = nullptr;
    if (OB_FAIL(FileRefCodec::to_json(allocator, input->get_string(), json_text))) {
      LOG_WARN("invalid FILE_META reference", K(ret));
    } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(
          &allocator, json_text, ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, json))) {
      LOG_WARN("failed to encode FILE_META JSON result", K(ret));
    } else if (OB_ISNULL(json)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("FILE_META JSON encoder returned no descriptor", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_res(expr, ctx, allocator, json, result))) {
      LOG_WARN("failed to own FILE_META JSON result", K(ret));
    }
  }
  return ret;
}

int ExprFileMeta::cg_expr(ObExprCGCtx &cg_ctx, const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  UNUSED(cg_ctx);
  UNUSED(raw_expr);
  return cg_file_expr(rt_expr, eval_file_meta);
}

} // namespace sql
} // namespace oceanbase
