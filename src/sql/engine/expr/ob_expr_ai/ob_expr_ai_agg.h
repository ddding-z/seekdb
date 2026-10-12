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

#ifndef OCEANBASE_SQL_OB_EXPR_AI_AGG_H_
#define OCEANBASE_SQL_OB_EXPR_AI_AGG_H_

#include "query/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObAggFunRawExpr;

// SQL uses ObAggFunRawExpr; this operator supplies registration and type deduction only.
class ExprAIAgg : public ObFuncExprOperator
{
public:
  explicit ExprAIAgg(common::ObIAllocator &allocator);
  virtual ~ExprAIAgg() {}
  virtual int calc_result_typeN(ObExprResType &type, ObExprResType *types,
                               int64_t param_num, common::ObExprTypeCtx &type_ctx) const override;
  static int check_param_count(int64_t param_num);
  static int check_arguments(const ObAggFunRawExpr &expr);
  // Recheck evaluated configuration expressions and bind values before aggregate execution.
  static int check_constant_argument(const common::ObObj &value, int64_t argument_index);

  static constexpr int64_t MODEL_IDX = 0;
  static constexpr int64_t INPUT_IDX = 1;
  static constexpr int64_t INSTRUCTION_IDX = 2;
  static constexpr int64_t OPTIONS_IDX = 3;
  static constexpr int64_t MIN_PARAM_COUNT = 3;
  static constexpr int64_t MAX_PARAM_COUNT = 4;
private:
  DISALLOW_COPY_AND_ASSIGN(ExprAIAgg);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_EXPR_AI_AGG_H_
