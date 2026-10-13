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
#include "ob_expr_ai_input.h"
#include "ob_ai_func_client.h"
#include "ob_ai_func_utils.h"
#include "common/object/file_ref.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

int SemanticInput::append(ObIAllocator &allocator, SemanticInputPart::Kind kind,
                          const ObString &value)
{
  int ret = OB_SUCCESS;
  ObString owned;
  if (value.length() > ObAIFuncClient::MAX_REQUEST_BYTES - owned_bytes_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_USER_ERROR(OB_SIZE_OVERFLOW);
  } else if (OB_FAIL(ob_write_string(allocator, value, owned))) {
  } else if (OB_FAIL(parts_.push_back(SemanticInputPart(kind, owned)))) {
  } else {
    has_files_ |= kind == SemanticInputPart::FILE;
    owned_bytes_ += value.length();
  }
  return ret;
}

int SemanticInput::render_text(ObIAllocator &allocator, ObString &prompt) const
{
  int ret = OB_SUCCESS;
  prompt.reset();
  if (has_files_ || parts_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cannot render non-text semantic input", K(ret), K_(has_files));
  } else if (parts_.count() == 1) {
    prompt = parts_.at(0).value_;
  } else {
    ObJsonBuffer buffer(&allocator);
    for (int64_t index = 0; OB_SUCC(ret) && index < parts_.count(); ++index) {
      if (parts_.at(index).kind_ != SemanticInputPart::TEXT) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        ret = buffer.append(parts_.at(index).value_.ptr(), parts_.at(index).value_.length());
      }
    }
    if (OB_SUCC(ret)) {
      ret = ob_write_string(allocator, buffer.string(), prompt);
    }
  }
  return ret;
}

int SemanticInput::is_input_object(const ObIJsonBase &document, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  if (document.json_type() == ObJsonNodeType::J_OBJECT) {
    JsonObjectIterator iterator = document.object_iterator();
    while (OB_SUCC(ret) && !found && !iterator.end()) {
      ObJsonObjPair pair;
      if (OB_FAIL(iterator.get_elem(pair))) {
        LOG_WARN("failed to inspect semantic input marker", K(ret));
      } else {
        found = pair.first == "$seekdb_ai_input";
        iterator.next();
      }
    }
  }
  return ret;
}

int SemanticInput::append_json(ObIAllocator &allocator, ObIJsonBase &document)
{
  int ret = OB_SUCCESS;
  ObJsonObject *object = nullptr;
  ObJsonNode *version = nullptr;
  ObJsonNode *parts = nullptr;
  bool null_input = false;
  if (document.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    object = static_cast<ObJsonObject *>(&document);
    version = object->get_value("$seekdb_ai_input");
    parts = object->get_value("parts");
    if (object->element_count() != 2 || nullptr == version ||
        !((version->json_type() == ObJsonNodeType::J_INT && version->get_int() == 1) ||
          (version->json_type() == ObJsonNodeType::J_UINT && version->get_uint() == 1)) ||
        nullptr == parts || parts->json_type() != ObJsonNodeType::J_ARRAY ||
        parts->element_count() == 0 || parts->element_count() > 64) {
      ret = OB_INVALID_ARGUMENT;
    }
  }
  for (uint64_t index = 0; OB_SUCC(ret) && index < parts->element_count(); ++index) {
    ObJsonNode *part = parts->get_value(index);
    ObJsonNode *kind = nullptr;
    ObJsonNode *value = nullptr;
    if (nullptr == part || part->json_type() != ObJsonNodeType::J_OBJECT ||
        part->element_count() != 2) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      auto *part_object = static_cast<ObJsonObject *>(part);
      kind = part_object->get_value("type");
      value = part_object->get_value("value");
      if (nullptr == kind || kind->json_type() != ObJsonNodeType::J_STRING || nullptr == value) {
        ret = OB_INVALID_ARGUMENT;
      } else if (value->json_type() == ObJsonNodeType::J_NULL) {
        ret = OB_INVALID_ARGUMENT;
        null_input = true;
      } else {
        const ObString kind_name(kind->get_data_length(), kind->get_data());
        if (kind_name == "text" && value->json_type() == ObJsonNodeType::J_STRING) {
          ret = append(allocator, SemanticInputPart::TEXT,
                       ObString(value->get_data_length(), value->get_data()));
        } else if (kind_name == "file" && value->json_type() == ObJsonNodeType::J_OBJECT) {
          ObString encoded;
          if (OB_FAIL(FileRefCodec::from_json(allocator, *value, encoded))) {
          } else if (OB_FAIL(append(allocator, SemanticInputPart::FILE, encoded))) {
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
        }
      }
    }
  }
  if (ret == OB_INVALID_ARGUMENT) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, null_input
        ? "demanded semantic tuple inputs must not be SQL NULL" : "invalid ordered semantic input");
    LOG_WARN("invalid ordered semantic input", K(ret));
  }
  return ret;
}

ExprAIInput::ExprAIInput(ObIAllocator &allocator)
    : ObFuncExprOperator(allocator, T_FUN_SYS_AI_INPUT, N_AI_INPUT, MORE_THAN_ZERO,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ExprAIInput::calc_result_typeN(ObExprResType &type, ObExprResType *types,
                                  int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (nullptr == types || param_num < 2 || param_num > 64 ||
      (!ob_is_string_type(types[0].get_type()) && types[0].get_type() != ObNullType)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "semantic tuple requires a text instruction and 1 to 63 inputs");
  }
  for (int64_t index = 0; OB_SUCC(ret) && index < param_num; ++index) {
    if (ob_is_string_type(types[index].get_type())) {
      types[index].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else if (types[index].get_type() == ObNullType) {
      types[index].set_calc_type(ObVarcharType);
      types[index].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else if (types[index].get_type() != ObFileType) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid semantic tuple input type", K(ret), K(index), K(types[index]));
    }
  }
  if (OB_SUCC(ret)) {
    type.set_json();
    type.set_length(ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType].get_length());
  }
  return ret;
}

int ExprAIInput::eval_ai_input(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard temporary(ctx);
  MultimodeAlloctor allocator(temporary.get_allocator());
  ObJsonArray *parts = nullptr;
  ObJsonObject *object = nullptr;
  ObJsonInt *version = nullptr;
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, parts))) {
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, object))) {
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, 1, version))) {
  } else if (OB_FAIL(object->add("$seekdb_ai_input", version))) {
  } else if (OB_FAIL(object->add("parts", parts))) {
  }
  int64_t bytes = 0;
  for (uint32_t index = 0; OB_SUCC(ret) && index < expr.arg_cnt_; ++index) {
    ObDatum *datum = nullptr;
    ObString value;
    ObJsonObject *part = nullptr;
    ObJsonString *kind = nullptr;
    ObJsonNode *value_node = nullptr;
    if (OB_FAIL(allocator.eval_arg(expr.args_[index], ctx, datum))) {
    } else if (datum->is_null()) {
    } else if (expr.args_[index]->datum_meta_.type_ == ObFileType) {
      value = datum->get_string();
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx.exec_ctx_, allocator, *datum,
        expr.args_[index]->datum_meta_, expr.args_[index]->obj_meta_.has_lob_header(), value))) {
    }
    if (OB_FAIL(ret)) {
    } else if (value.length() > ObAIFuncClient::MAX_REQUEST_BYTES - bytes) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      const bool file = expr.args_[index]->datum_meta_.type_ == ObFileType;
      ObString kind_name(file ? "file" : "text");
      bytes += value.length();
      if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, part))) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, kind_name, kind))) {
      } else if (OB_FAIL(part->add("type", kind))) {
      } else if (datum->is_null()) {
        if (OB_ISNULL(value_node = OB_NEWx(ObJsonNull, &allocator))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      } else if (file) {
        ObString json;
        ObIJsonBase *document = nullptr;
        if (OB_FAIL(FileRefCodec::to_json(allocator, value, json))) {
        } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, json,
            ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, document))) {
        } else {
          value_node = static_cast<ObJsonNode *>(document);
        }
      } else {
        ObJsonString *text = nullptr;
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, value, text))) {
        } else {
          value_node = text;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(part->add("value", value_node))) {
      } else if (OB_SUCC(ret) && OB_FAIL(parts->append(part))) {
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObString binary;
    if (OB_FAIL(object->get_raw_binary(binary, &allocator))) {
    } else if (binary.length() > ObAIFuncClient::MAX_REQUEST_BYTES) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, result, binary))) {
    }
  }
  if (OB_FAIL(ret) && ret != OB_EAGAIN) {
    LOG_WARN("failed to compose ordered semantic input", K(ret));
  }
  return ret;
}

int ExprAIInput::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_ai_input;
  return OB_SUCCESS;
}
}
}
