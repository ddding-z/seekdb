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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FILE_H_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FILE_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ExprToFile : public ObFuncExprOperator
{
public:
  explicit ExprToFile(common::ObIAllocator &allocator);
  int calc_result_type1(ObExprResType &type, ObExprResType &input,
                       common::ObExprTypeCtx &type_ctx) const override;
  int cg_expr(ObExprCGCtx &cg_ctx, const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override;
  static int eval_to_file(const ObExpr &expr, ObEvalCtx &ctx, common::ObDatum &result);
};

class ExprFileUri : public ObFuncExprOperator
{
public:
  explicit ExprFileUri(common::ObIAllocator &allocator);
  int calc_result_type1(ObExprResType &type, ObExprResType &input,
                       common::ObExprTypeCtx &type_ctx) const override;
  int cg_expr(ObExprCGCtx &cg_ctx, const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override;
  static int eval_file_uri(const ObExpr &expr, ObEvalCtx &ctx, common::ObDatum &result);
};

class ExprFileMeta : public ObFuncExprOperator
{
public:
  explicit ExprFileMeta(common::ObIAllocator &allocator);
  int calc_result_type1(ObExprResType &type, ObExprResType &input,
                       common::ObExprTypeCtx &type_ctx) const override;
  int cg_expr(ObExprCGCtx &cg_ctx, const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override;
  static int eval_file_meta(const ObExpr &expr, ObEvalCtx &ctx, common::ObDatum &result);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FILE_H_
