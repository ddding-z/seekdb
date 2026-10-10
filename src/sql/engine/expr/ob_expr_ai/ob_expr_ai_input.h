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

#ifndef OCEANBASE_SQL_OB_EXPR_AI_INPUT_H_
#define OCEANBASE_SQL_OB_EXPR_AI_INPUT_H_

#include "query/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_multi_mode_func_helper.h"
#include <memory>

namespace oceanbase
{
namespace common
{
class ObIJsonBase;
}
namespace sql
{
class SemanticMediaTask;

struct SemanticInputPart
{
  enum Kind { TEXT, FILE };
  SemanticInputPart() : kind_(TEXT), value_() {}
  SemanticInputPart(Kind kind, const common::ObString &value) : kind_(kind), value_(value) {}
  TO_STRING_KV(K_(kind), "bytes", value_.length());
  Kind kind_;
  common::ObString value_;
  std::shared_ptr<SemanticMediaTask> media_;
};

class SemanticInput
{
public:
  SemanticInput() : has_files_(false), owned_bytes_(0) {}
  TO_STRING_KV("part_count", parts_.count(), K_(has_files), K_(owned_bytes));
  int append(common::ObIAllocator &allocator, SemanticInputPart::Kind kind,
             const common::ObString &value);
  int append_json(common::ObIAllocator &allocator, common::ObIJsonBase &document);
  static int is_input_object(const common::ObIJsonBase &document, bool &found);
  common::ObArray<SemanticInputPart> parts_;
  bool has_files_;
  int64_t owned_bytes_;
  DISALLOW_COPY_AND_ASSIGN(SemanticInput);
};

class ExprAIInput : public ObFuncExprOperator
{
public:
  explicit ExprAIInput(common::ObIAllocator &allocator);
  int calc_result_typeN(ObExprResType &type, ObExprResType *types,
                       int64_t param_num, common::ObExprTypeCtx &type_ctx) const override;
  int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
  static int eval_ai_input(const ObExpr &expr, ObEvalCtx &ctx, common::ObDatum &result);
private:
  DISALLOW_COPY_AND_ASSIGN(ExprAIInput);
};
}
}
#endif
