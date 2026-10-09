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

#ifndef OCEANBASE_SQL_OB_EXPR_AI_SEMANTIC_H_
#define OCEANBASE_SQL_OB_EXPR_AI_SEMANTIC_H_

#include "query/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_multi_mode_func_helper.h"

namespace oceanbase
{
namespace common
{
class ObJsonObject;
}
namespace sql
{

class ISemanticExprRuntime
{
public:
  virtual ~ISemanticExprRuntime() {}
  virtual int evaluate(const ObExpr &expr, ObEvalCtx &ctx, common::ObDatum &result) = 0;
  static ISemanticExprRuntime *current();
};

class SemanticExprRuntimeGuard
{
public:
  explicit SemanticExprRuntimeGuard(ISemanticExprRuntime *runtime);
  explicit SemanticExprRuntimeGuard(ISemanticExprRuntime &runtime);
  ~SemanticExprRuntimeGuard();
private:
  ISemanticExprRuntime *previous_;
  DISALLOW_COPY_AND_ASSIGN(SemanticExprRuntimeGuard);
};

class SemanticExprUtils
{
public:
  static bool is_semantic(ObItemType type);
  static int contains_semantic(const ObRawExpr *expr, bool &contains);
  static int prepare_input(const ObExpr &expr, ObEvalCtx &ctx,
                           MultimodeAlloctor &allocator,
                           common::ObString &model_id, common::ObString &prompt,
                           common::ObJsonObject *&config, bool prepare_config = true);
  static int set_result(const ObExpr &expr, ObEvalCtx &ctx, common::ObDatum &datum,
                        const common::ObString &result);
private:
  SemanticExprUtils() = delete;
};

class ExprAIMap : public ObFuncExprOperator
{
public:
  explicit ExprAIMap(common::ObIAllocator &allocator);
  virtual ~ExprAIMap() {}
  virtual int calc_result_typeN(ObExprResType &type, ObExprResType *types,
                                int64_t param_num, common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
  static int eval_ai_map(const ObExpr &expr, ObEvalCtx &ctx, common::ObDatum &result);
  static int eval_ai_map_batch(const ObExpr &expr, ObEvalCtx &ctx,
                               const ObBitVector &skip, int64_t size);
private:
  DISALLOW_COPY_AND_ASSIGN(ExprAIMap);
};

class ExprAIFilter : public ObFuncExprOperator
{
public:
  explicit ExprAIFilter(common::ObIAllocator &allocator);
  virtual ~ExprAIFilter() {}
  virtual int calc_result_typeN(ObExprResType &type, ObExprResType *types,
                                int64_t param_num, common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
  static int eval_ai_filter(const ObExpr &expr, ObEvalCtx &ctx, common::ObDatum &result);
  static int eval_ai_filter_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                  const ObBitVector &skip, int64_t size);
private:
  DISALLOW_COPY_AND_ASSIGN(ExprAIFilter);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_EXPR_AI_SEMANTIC_H_
